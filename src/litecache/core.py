"""The LiteCache class: the entire public API surface of litecache."""

from __future__ import annotations

import contextlib
import functools
import hashlib
import logging
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import (
    Any,
    Callable,
    TypeVar,
    Union,
    cast,
)

from ._paths import default_path as _resolve_default_path
from ._schema import DDL, PRAGMAS, SCHEMA_VERSION
from ._serialize import deserialize, serialize
from ._sweeper import Sweeper
from .exceptions import CacheFullError, LiteCacheError, LockTimeout, SchemaVersionError

logger = logging.getLogger("litecache")

F = TypeVar("F", bound=Callable[..., Any])

_VALID_EVICTION = ("lru", "ttl", "random", "noeviction")
_LRU_FLUSH_THRESHOLD = 200
_EXPIRE_BATCH = 500
_OPPORTUNISTIC_EVERY = 100


def _now_ms() -> int:
    return int(time.time() * 1000)


class LiteCache:
    """An embedded, Redis-like cache backed by a local SQLite file.

    The class is the entire public API -- there are no module-level
    functions. Zero configuration is required::

        from litecache import LiteCache

        cache = LiteCache()
        cache.set("user:42", {"name": "Ada"}, ttl=300)
        cache.get("user:42")
    """

    def __init__(
        self,
        path: Union[str, os.PathLike[str]] | None = None,
        *,
        namespace: str = "default",
        max_keys: int | None = None,
        max_bytes: int | None = None,
        eviction: str = "lru",
        sweep_interval: float | None = 60.0,
        serializer: str = "json",
        strict: bool = False,
    ) -> None:
        if eviction not in _VALID_EVICTION:
            raise ValueError(
                f"invalid eviction policy {eviction!r}; expected one of {_VALID_EVICTION}"
            )
        if serializer != "json":
            raise ValueError("only the 'json' serializer is supported in this version")
        if sweep_interval is not None and sweep_interval <= 0:
            raise ValueError("sweep_interval must be a positive number of seconds, or None")

        self._path = Path(path).expanduser() if path is not None else _resolve_default_path()
        self._namespace = namespace
        self._max_keys = max_keys
        self._max_bytes = max_bytes
        self._eviction = eviction
        self._sweep_interval = sweep_interval
        self._serializer = serializer
        self._strict = strict

        self._local = threading.local()
        self._lock = threading.RLock()
        self._conn_registry_lock = threading.Lock()
        self._connections: list[sqlite3.Connection] = []

        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expired_removed = 0
        self._lru_buffer: dict[str, list[int]] = {}
        self._op_count = 0
        self._closed = False

        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Eagerly open + initialize the schema so setup failures surface immediately,
        # right here in __init__, rather than on the first cache operation.
        self._get_conn()

        self._sweeper: Sweeper | None = None
        if self._sweep_interval is not None:
            self._sweeper = Sweeper(self._sweep_interval, self._sweep_once)
            self._sweeper.start()

    # -- construction / connection plumbing ---------------------------------

    @classmethod
    def default_path(cls) -> Path:
        """Return the resolved zero-config default database path."""
        return _resolve_default_path()

    @property
    def path(self) -> Path:
        """The actual database file backing this instance."""
        return self._path

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                str(self._path), timeout=5.0, isolation_level=None, check_same_thread=False
            )
            for pragma in PRAGMAS:
                conn.execute(pragma)
            self._ensure_schema(conn)
            self._local.conn = conn
            with self._conn_registry_lock:
                self._connections.append(conn)
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(DDL)
        row = conn.execute("SELECT v FROM meta WHERE k = 'schema_version'").fetchone()
        if row is None:
            conn.execute(
                "INSERT OR IGNORE INTO meta (k, v) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        else:
            existing = int(row[0])
            if existing > SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"database schema version {existing} is newer than the version "
                    f"{SCHEMA_VERSION} supported by this litecache install; "
                    "upgrade litecache to open this file"
                )

    def _require_open(self) -> None:
        if self._closed:
            raise LiteCacheError("this LiteCache instance is closed")

    def _has_capacity_limits(self) -> bool:
        return self._max_keys is not None or self._max_bytes is not None

    # -- key/value ------------------------------------------------------------

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        self._require_open()
        blob, vtype = serialize(value)
        now = _now_ms()
        expires_at = None if ttl is None else now + int(ttl * 1000)
        conn = self._get_conn()
        if self._eviction == "noeviction" and self._has_capacity_limits():
            self._check_capacity(conn, key, now)
        conn.execute(
            """
            INSERT INTO cache
                (namespace, key, value, value_type, created_at, expires_at,
                 last_accessed, access_count, size_bytes)
            VALUES (:ns, :key, :value, :vtype, :now, :expires_at, :now, 0, :size)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = excluded.value,
                value_type = excluded.value_type,
                created_at = :now,
                expires_at = excluded.expires_at,
                last_accessed = excluded.last_accessed,
                access_count = 0,
                size_bytes = excluded.size_bytes
            """,
            {
                "ns": self._namespace,
                "key": key,
                "value": blob,
                "vtype": vtype,
                "now": now,
                "expires_at": expires_at,
                "size": len(blob),
            },
        )
        self._maybe_evict(conn)
        self._maybe_opportunistic_maintenance()

    def get(self, key: str, default: Any = None) -> Any:
        self._require_open()
        now = _now_ms()
        try:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?",
                (self._namespace, key),
            ).fetchone()
        except sqlite3.Error as exc:
            if self._strict:
                raise LiteCacheError(str(exc)) from exc
            logger.warning("litecache: get(%r) failed, treating as a miss: %s", key, exc)
            with self._lock:
                self._misses += 1
            return default

        if row is None:
            with self._lock:
                self._misses += 1
            self._maybe_opportunistic_maintenance()
            return default

        value_blob, value_type, expires_at = row
        if expires_at is not None and expires_at <= now:
            with self._lock:
                self._misses += 1
            self._maybe_opportunistic_maintenance()
            return default

        with self._lock:
            self._hits += 1
        self._buffer_lru(key, now)
        self._maybe_opportunistic_maintenance()

        try:
            return deserialize(value_blob, value_type)
        except LiteCacheError as exc:
            if self._strict:
                raise
            logger.warning("litecache: failed to deserialize key %r: %s", key, exc)
            return default

    def delete(self, *keys: str) -> int:
        self._require_open()
        if not keys:
            return 0
        conn = self._get_conn()
        placeholders = ",".join("?" * len(keys))
        cur = conn.execute(
            f"DELETE FROM cache WHERE namespace = ? AND key IN ({placeholders})",
            [self._namespace, *keys],
        )
        with self._lock:
            for k in keys:
                self._lru_buffer.pop(k, None)
        return cur.rowcount

    def exists(self, key: str) -> bool:
        self._require_open()
        now = _now_ms()
        conn = self._get_conn()
        row = conn.execute(
            "SELECT 1 FROM cache WHERE namespace = ? AND key = ? "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (self._namespace, key, now),
        ).fetchone()
        return row is not None

    def add(self, key: str, value: Any, ttl: float | None = None) -> bool:
        self._require_open()
        blob, vtype = serialize(value)
        now = _now_ms()
        expires_at = None if ttl is None else now + int(ttl * 1000)
        conn = self._get_conn()
        if self._eviction == "noeviction" and self._has_capacity_limits():
            self._check_capacity(conn, key, now)
        cur = conn.execute(
            """
            INSERT INTO cache
                (namespace, key, value, value_type, created_at, expires_at,
                 last_accessed, access_count, size_bytes)
            VALUES (:ns, :key, :value, :vtype, :now, :expires_at, :now, 0, :size)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = :value,
                value_type = :vtype,
                created_at = :now,
                expires_at = :expires_at,
                last_accessed = :now,
                access_count = 0,
                size_bytes = :size
            WHERE cache.expires_at IS NOT NULL AND cache.expires_at <= :now
            """,
            {
                "ns": self._namespace,
                "key": key,
                "value": blob,
                "vtype": vtype,
                "now": now,
                "expires_at": expires_at,
                "size": len(blob),
            },
        )
        won = cur.rowcount == 1
        if won:
            self._maybe_evict(conn)
        return won

    def replace(self, key: str, value: Any, ttl: float | None = None) -> bool:
        self._require_open()
        blob, vtype = serialize(value)
        now = _now_ms()
        expires_at = None if ttl is None else now + int(ttl * 1000)
        conn = self._get_conn()
        cur = conn.execute(
            """
            UPDATE cache
               SET value = ?, value_type = ?, created_at = ?, expires_at = ?,
                   last_accessed = ?, access_count = 0, size_bytes = ?
             WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)
            """,
            (blob, vtype, now, expires_at, now, len(blob), self._namespace, key, now),
        )
        return cur.rowcount == 1

    def get_set(self, key: str, value: Any) -> Any:
        self._require_open()
        blob, vtype = serialize(value)
        now = _now_ms()
        conn = self._get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?",
                (self._namespace, key),
            ).fetchone()
            if row is not None and row[2] is not None and row[2] <= now:
                row = None
            conn.execute(
                """
                INSERT INTO cache
                    (namespace, key, value, value_type, created_at, expires_at,
                     last_accessed, access_count, size_bytes)
                VALUES (:ns, :key, :value, :vtype, :now, NULL, :now, 0, :size)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value = :value,
                    value_type = :vtype,
                    created_at = :now,
                    expires_at = NULL,
                    last_accessed = :now,
                    access_count = 0,
                    size_bytes = :size
                """,
                {
                    "ns": self._namespace,
                    "key": key,
                    "value": blob,
                    "vtype": vtype,
                    "now": now,
                    "size": len(blob),
                },
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
        if row is None:
            return None
        return deserialize(row[0], row[1])

    def set_many(self, mapping: Mapping[str, Any], ttl: float | None = None) -> None:
        self._require_open()
        if not mapping:
            return
        now = _now_ms()
        expires_at = None if ttl is None else now + int(ttl * 1000)
        rows = []
        for k, v in mapping.items():
            blob, vtype = serialize(v)
            rows.append(
                {
                    "ns": self._namespace,
                    "key": k,
                    "value": blob,
                    "vtype": vtype,
                    "now": now,
                    "expires_at": expires_at,
                    "size": len(blob),
                }
            )
        conn = self._get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                """
                INSERT INTO cache
                    (namespace, key, value, value_type, created_at, expires_at,
                     last_accessed, access_count, size_bytes)
                VALUES (:ns, :key, :value, :vtype, :now, :expires_at, :now, 0, :size)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value = excluded.value,
                    value_type = excluded.value_type,
                    created_at = :now,
                    expires_at = excluded.expires_at,
                    last_accessed = excluded.last_accessed,
                    access_count = 0,
                    size_bytes = excluded.size_bytes
                """,
                rows,
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
        self._maybe_evict(conn)

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:
        self._require_open()
        key_list = list(keys)
        if not key_list:
            return {}
        now = _now_ms()
        conn = self._get_conn()
        placeholders = ",".join("?" * len(key_list))
        rows = conn.execute(
            f"SELECT key, value, value_type, expires_at FROM cache "
            f"WHERE namespace = ? AND key IN ({placeholders})",
            [self._namespace, *key_list],
        ).fetchall()
        result: dict[str, Any] = {}
        for k, value_blob, value_type, expires_at in rows:
            if expires_at is not None and expires_at <= now:
                continue
            result[k] = deserialize(value_blob, value_type)
            self._buffer_lru(k, now)
        hits = len(result)
        misses = len(key_list) - hits
        with self._lock:
            self._hits += hits
            self._misses += misses
        return result

    # -- expiration -----------------------------------------------------------

    def expire(self, key: str, ttl: float) -> bool:
        self._require_open()
        now = _now_ms()
        expires_at = now + int(ttl * 1000)
        conn = self._get_conn()
        cur = conn.execute(
            "UPDATE cache SET expires_at = ? WHERE namespace = ? AND key = ? "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (expires_at, self._namespace, key, now),
        )
        return cur.rowcount == 1

    def persist(self, key: str) -> bool:
        self._require_open()
        now = _now_ms()
        conn = self._get_conn()
        cur = conn.execute(
            "UPDATE cache SET expires_at = NULL WHERE namespace = ? AND key = ? "
            "AND expires_at IS NOT NULL AND expires_at > ?",
            (self._namespace, key, now),
        )
        return cur.rowcount == 1

    def ttl(self, key: str) -> float | None:
        self._require_open()
        now = _now_ms()
        conn = self._get_conn()
        row = conn.execute(
            "SELECT expires_at FROM cache WHERE namespace = ? AND key = ?",
            (self._namespace, key),
        ).fetchone()
        if row is None:
            return None
        expires_at = row[0]
        if expires_at is None:
            return -1.0
        remaining = (expires_at - now) / 1000.0
        if remaining <= 0:
            return None
        return cast(float, remaining)

    def touch(self, key: str, ttl: float) -> bool:
        """Refresh a key's TTL (sliding expiration)."""
        return self.expire(key, ttl)

    # -- atomic counters --------------------------------------------------------

    def incr(self, key: str, amount: int = 1) -> int:
        self._require_open()
        now = _now_ms()
        return cast(int, self._incr(key, int(amount), allowed_types=(2,), result_type=2, now=now))

    def decr(self, key: str, amount: int = 1) -> int:
        return self.incr(key, -amount)

    def incr_float(self, key: str, amount: float) -> float:
        self._require_open()
        now = _now_ms()
        return cast(
            float,
            self._incr(key, float(amount), allowed_types=(2, 3), result_type=3, now=now),
        )

    def _incr(
        self,
        key: str,
        amount: Union[int, float],
        *,
        allowed_types: tuple[int, ...],
        result_type: int,
        now: int,
    ) -> Union[int, float]:
        conn = self._get_conn()
        if result_type == 2:
            blob = str(int(amount)).encode("utf-8")
        else:
            blob = repr(float(amount)).encode("utf-8")
        in_clause = "(" + ",".join(str(t) for t in allowed_types) + ")"
        sql = f"""
            INSERT INTO cache
                (namespace, key, value, value_type, created_at, expires_at,
                 last_accessed, access_count, size_bytes)
            VALUES (:ns, :key, :blob, :rtype, :now, NULL, :now, 0, :size)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = CAST(CAST(
                    (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now THEN 0
                          ELSE CAST(cache.value AS TEXT) END) + :amount
                    AS TEXT) AS BLOB),
                value_type = :rtype,
                expires_at = CASE
                    WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now THEN NULL
                    ELSE cache.expires_at
                END,
                last_accessed = :now,
                size_bytes = LENGTH(CAST(
                    (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now THEN 0
                          ELSE CAST(cache.value AS TEXT) END) + :amount
                    AS TEXT))
            WHERE (cache.expires_at IS NOT NULL AND cache.expires_at <= :now)
               OR cache.value_type IN {in_clause}
        """
        params = {
            "ns": self._namespace,
            "key": key,
            "blob": blob,
            "rtype": result_type,
            "now": now,
            "size": len(blob),
            "amount": amount,
        }
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(sql, params)
            if cur.rowcount == 0:
                kind = "a number" if result_type == 3 else "an integer"
                raise TypeError(f"value for key {key!r} is not {kind}")
            row = conn.execute(
                "SELECT value FROM cache WHERE namespace = ? AND key = ?",
                (self._namespace, key),
            ).fetchone()
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
        text = row[0].decode("utf-8")
        return int(text) if result_type == 2 else float(text)

    # -- introspection & management -----------------------------------------

    def keys(self, pattern: str = "*") -> Iterator[str]:
        self._require_open()
        conn = self._get_conn()
        now = _now_ms()
        batch_size = 500
        last_key = ""
        while True:
            rows = conn.execute(
                "SELECT key FROM cache WHERE namespace = ? AND key GLOB ? AND key > ? "
                "AND (expires_at IS NULL OR expires_at > ?) ORDER BY key LIMIT ?",
                (self._namespace, pattern, last_key, now, batch_size),
            ).fetchall()
            if not rows:
                return
            for (k,) in rows:
                yield k
            last_key = rows[-1][0]
            if len(rows) < batch_size:
                return

    def flush(self) -> None:
        self._require_open()
        conn = self._get_conn()
        conn.execute("DELETE FROM cache WHERE namespace = ?", (self._namespace,))
        with self._lock:
            self._lru_buffer.clear()

    def stats(self) -> dict[str, Any]:
        self._require_open()
        conn = self._get_conn()
        count, size_bytes = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM cache WHERE namespace = ?",
            (self._namespace,),
        ).fetchone()
        with self._lock:
            hits, misses = self._hits, self._misses
            evictions, expired_removed = self._evictions, self._expired_removed
        total = hits + misses
        hit_rate = hits / total if total else 0.0
        return {
            "hits": hits,
            "misses": misses,
            "hit_rate": hit_rate,
            "key_count": count,
            "size_bytes": size_bytes,
            "evictions": evictions,
            "expired_removed": expired_removed,
            "path": str(self._path),
            "namespace": self._namespace,
        }

    def vacuum(self) -> None:
        self._require_open()
        self._flush_lru_buffer()
        conn = self._get_conn()
        conn.execute("VACUUM")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._sweeper is not None:
            self._sweeper.stop()
        self._flush_lru_buffer()
        with self._conn_registry_lock:
            for conn in self._connections:
                with contextlib.suppress(sqlite3.Error):
                    conn.close()
            self._connections.clear()

    def __enter__(self) -> LiteCache:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- extras: memoize + lock -----------------------------------------------

    def memoize(self, ttl: float | None = None) -> Callable[[F], F]:
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                key = self._memo_key(func, args, kwargs)
                sentinel = object()
                cached = self.get(key, sentinel)
                if cached is not sentinel:
                    return cached
                result = func(*args, **kwargs)
                self.set(key, result, ttl=ttl)
                return result

            return cast(F, wrapper)

        return decorator

    @staticmethod
    def _memo_key(func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        raw = repr((args, sorted(kwargs.items())))
        digest = hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:16]
        return f"memoize:{func.__qualname__}:{digest}"

    @contextlib.contextmanager
    def lock(
        self,
        name: str,
        timeout: float = 30,
        blocking: bool = True,
        poll: float = 0.05,
    ) -> Iterator[None]:
        key = f"lock:{name}"
        token = uuid.uuid4().hex
        deadline = time.monotonic() + timeout if timeout is not None else None
        acquired = False
        while True:
            if self.add(key, token, ttl=timeout):
                acquired = True
                break
            if not blocking:
                break
            if deadline is not None and time.monotonic() >= deadline:
                break
            time.sleep(poll)
        if not acquired:
            raise LockTimeout(f"could not acquire lock {name!r} within {timeout}s")
        try:
            yield
        finally:
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT value FROM cache WHERE namespace = ? AND key = ?",
                    (self._namespace, key),
                ).fetchone()
                if row is not None and row[0] == token.encode("utf-8"):
                    conn.execute(
                        "DELETE FROM cache WHERE namespace = ? AND key = ?",
                        (self._namespace, key),
                    )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    # -- internal: LRU buffering, sweeping, eviction --------------------------

    def _buffer_lru(self, key: str, now: int) -> None:
        should_flush = False
        with self._lock:
            entry = self._lru_buffer.get(key)
            if entry is None:
                self._lru_buffer[key] = [now, 1]
            else:
                entry[0] = now
                entry[1] += 1
            should_flush = len(self._lru_buffer) >= _LRU_FLUSH_THRESHOLD
        if should_flush:
            self._flush_lru_buffer()

    def _flush_lru_buffer(self) -> None:
        with self._lock:
            if not self._lru_buffer:
                return
            pending = self._lru_buffer
            self._lru_buffer = {}
        conn = self._get_conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                "UPDATE cache SET last_accessed = ?, access_count = access_count + ? "
                "WHERE namespace = ? AND key = ?",
                [(ts, cnt, self._namespace, k) for k, (ts, cnt) in pending.items()],
            )
        except BaseException:
            conn.execute("ROLLBACK")
            if self._strict:
                raise
            logger.warning("litecache: failed to flush LRU bookkeeping buffer")
        else:
            conn.execute("COMMIT")

    def _maybe_opportunistic_maintenance(self) -> None:
        if self._sweep_interval is not None:
            return
        due = False
        with self._lock:
            self._op_count += 1
            if self._op_count >= _OPPORTUNISTIC_EVERY:
                self._op_count = 0
                due = True
        if due:
            self._sweep_once()

    def _sweep_once(self) -> None:
        try:
            self._flush_lru_buffer()
            conn = self._get_conn()
            now = _now_ms()
            while True:
                cur = conn.execute(
                    "DELETE FROM cache WHERE namespace = ? AND key IN ("
                    "  SELECT key FROM cache WHERE namespace = ? "
                    "  AND expires_at IS NOT NULL AND expires_at <= ? LIMIT ?"
                    ")",
                    (self._namespace, self._namespace, now, _EXPIRE_BATCH),
                )
                deleted = cur.rowcount
                if deleted <= 0:
                    break
                with self._lock:
                    self._expired_removed += deleted
                if deleted < _EXPIRE_BATCH:
                    break
            self._maybe_evict(conn)
        except sqlite3.Error as exc:
            logger.warning("litecache: background sweep failed: %s", exc)

    def _check_capacity(self, conn: sqlite3.Connection, key: str, now: int) -> None:
        exists = conn.execute(
            "SELECT 1 FROM cache WHERE namespace = ? AND key = ? "
            "AND (expires_at IS NULL OR expires_at > ?)",
            (self._namespace, key, now),
        ).fetchone()
        if exists is not None:
            return  # updating an existing key never grows the dataset
        count, total = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM cache WHERE namespace = ?",
            (self._namespace,),
        ).fetchone()
        if self._max_keys is not None and count >= self._max_keys:
            raise CacheFullError(f"cache is full: max_keys={self._max_keys} reached")
        if self._max_bytes is not None and total >= self._max_bytes:
            raise CacheFullError(f"cache is full: max_bytes={self._max_bytes} reached")

    def _eviction_order_sql(self) -> str:
        if self._eviction == "lru":
            return "last_accessed ASC"
        if self._eviction == "ttl":
            return "(expires_at IS NULL) ASC, expires_at ASC"
        if self._eviction == "random":
            return "RANDOM()"
        raise AssertionError(f"unexpected eviction policy {self._eviction!r}")

    def _maybe_evict(self, conn: sqlite3.Connection) -> None:
        if self._eviction == "noeviction":
            return
        if self._max_keys is None and self._max_bytes is None:
            return
        order = self._eviction_order_sql()
        while True:
            count, total = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM cache WHERE namespace = ?",
                (self._namespace,),
            ).fetchone()
            over_keys = (count - self._max_keys) if self._max_keys is not None else 0
            over_bytes = self._max_bytes is not None and total > self._max_bytes
            if over_keys <= 0 and not over_bytes:
                return
            batch = max(over_keys, 1)
            rows = conn.execute(
                f"SELECT key FROM cache WHERE namespace = ? ORDER BY {order} LIMIT ?",
                (self._namespace, batch),
            ).fetchall()
            if not rows:
                return
            to_delete = [r[0] for r in rows]
            conn.executemany(
                "DELETE FROM cache WHERE namespace = ? AND key = ?",
                [(self._namespace, k) for k in to_delete],
            )
            with self._lock:
                self._evictions += len(to_delete)
                for k in to_delete:
                    self._lru_buffer.pop(k, None)
