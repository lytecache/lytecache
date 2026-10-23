"""The LyteCache class: the entire public API surface of lytecache."""

from __future__ import annotations

import contextlib
import functools
import hashlib
import json
import logging
import math
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

from ._events import (
    CACHE_CHANNEL,
    CHANGE_OPS,
    ChangeRecord,
    Dispatcher,
    Subscription,
    channel_filter_sql,
    gap_check,
    last_assigned_seq,
    validate_channel,
)
from ._paths import default_path as _resolve_default_path
from ._schema import DDL, EVENTS_DDL, PRAGMAS, SCHEMA_VERSION
from ._serialize import _build_from_cls, deserialize, encode_json_text, serialize
from ._sweeper import Sweeper
from .exceptions import (
    CacheFullError,
    LockTimeout,
    LyteCacheError,
    SchemaVersionError,
    SerializationError,
)

logger = logging.getLogger("lytecache")

F = TypeVar("F", bound=Callable[..., Any])

_VALID_EVICTION = ("lru", "ttl", "random", "noeviction")
_VALID_SERIALIZERS = ("auto", "json", "pickle")
_LRU_FLUSH_THRESHOLD = 200
_EXPIRE_BATCH = 500
_OPPORTUNISTIC_EVERY = 100
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
# Testing knob: forces the read-modify-write hash implementation even where
# JSON1 is available, so the test suite can prove both paths equivalent.
_FORCE_JSON_FALLBACK_ENV = "LYTECACHE_FORCE_JSON_FALLBACK"
_HSET_MANY_STATEMENT = 500  # fields per statement (bound-variable limit)
_JSON_SET_PAIRS = 30  # path/value pairs per json_set call (function-argument limit)
_TYPE_NAMES = {0: "bytes", 1: "str", 2: "int", 3: "float", 4: "json", 5: "pickle", 6: "java-native"}

# -- SQL fragments for the JSON1 hash path ------------------------------------
# Every fragment binds the field name (never interpolates it): the path is
# built in SQL as $."<field>" with backslashes and quotes escaped, so a field
# named `a.b`, `a[0]` or `q"x` addresses exactly that top-level key. The value
# column is a BLOB; it is always CAST to TEXT before reaching a JSON function
# so SQLite 3.45+ never tries to interpret it as JSONB, and results are CAST
# back to BLOB so the stored value_type-4 row keeps its byte encoding.
_DOC = "CAST(cache.value AS TEXT)"
_LIVE = "(cache.expires_at IS NULL OR cache.expires_at > :now)"
_EXPIRED = "(cache.expires_at IS NOT NULL AND cache.expires_at <= :now)"
_ROOT_TYPE = f"(CASE WHEN json_valid({_DOC}) THEN json_type({_DOC}) END)"
_IS_OBJ = (
    f"(cache.value_type = 4 AND CASE WHEN json_valid({_DOC}) THEN json_type({_DOC}) END = 'object')"
)


def _path(param: str) -> str:
    return rf"""('$."' || replace(replace(:{param}, '\', '\\'), '"', '\"') || '"')"""


def _float_text(expr: str) -> str:
    """JSON text for a REAL, with the fewest digits that round-trip exactly.

    SQLite before ~3.5x renders a REAL into JSON (and CAST(x AS TEXT)) with
    15 significant digits, which would store 0.1 + 0.2 as 0.3. This picks
    the shortest of 15/16/17 digits that reads back to the same double, on
    every SQLite version, matching Python's repr().
    """
    p = [f"printf('%!.{n}g', {expr})" for n in (15, 16, 17)]
    return (
        f"(CASE WHEN CAST({p[0]} AS REAL) = {expr} THEN {p[0]} "
        f"WHEN CAST({p[1]} AS REAL) = {expr} THEN {p[1]} ELSE {p[2]} END)"
    )


def _field_type(param: str) -> str:
    return f"(CASE WHEN {_IS_OBJ} THEN json_type({_DOC}, {_path(param)}) END)"


def _now_ms() -> int:
    return int(time.time() * 1000)


class LyteCache:
    """An embedded, Redis-like cache backed by a local SQLite file.

    The class is the entire public API -- there are no module-level
    functions. Zero configuration is required::

        from lytecache import LyteCache

        cache = LyteCache()
        cache.set("user:42", {"name": "Ada"}, ttl=300)
        cache.get("user:42")

    SECURITY: unpickling can execute arbitrary code. The default
    ``serializer="auto"`` (and ``serializer="json"``) never write or read
    pickled data, so this does not apply to them. Only ``serializer="pickle"``
    (or ``serializer="auto"`` combined with ``allow_pickle=True``) can read
    pickled values -- treat a cache file that might contain pickled data like
    application code, and never open one from an untrusted source.
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
        serializer: str = "auto",
        strict: bool = False,
        allow_pickle: bool = False,
        publish_changes: Iterable[str] | None = None,
        event_retention: float = 3600.0,
        event_max_count: int | None = None,
        change_key_cap: int = 100,
    ) -> None:
        if eviction not in _VALID_EVICTION:
            raise ValueError(
                f"invalid eviction policy {eviction!r}; expected one of {_VALID_EVICTION}"
            )
        if serializer not in _VALID_SERIALIZERS:
            raise ValueError(
                f"invalid serializer {serializer!r}; expected one of {_VALID_SERIALIZERS}"
            )
        if serializer == "json" and allow_pickle:
            raise ValueError(
                "allow_pickle is not compatible with serializer='json' "
                "(json mode never reads pickled values, by design)"
            )
        if sweep_interval is not None and sweep_interval <= 0:
            raise ValueError("sweep_interval must be a positive number of seconds, or None")
        publish_ops = frozenset(publish_changes or ())
        if publish_ops - CHANGE_OPS:
            raise ValueError(
                f"publish_changes must be a subset of {sorted(CHANGE_OPS)}, "
                f"got {sorted(publish_ops)}"
            )
        if event_retention <= 0:
            raise ValueError("event_retention must be a positive number of seconds")
        if event_max_count is not None and event_max_count < 1:
            raise ValueError("event_max_count must be a positive integer, or None")
        if change_key_cap < 0:
            raise ValueError("change_key_cap must be >= 0")

        self._path = Path(path).expanduser() if path is not None else _resolve_default_path()
        self._namespace = namespace
        self._max_keys = max_keys
        self._max_bytes = max_bytes
        self._eviction = eviction
        self._sweep_interval = sweep_interval
        self._serializer = serializer
        self._strict = strict
        self._allow_pickle = allow_pickle

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
        self._json1: bool | None = None  # JSON1 support, detected on first connection
        # Events and change listeners are opt-in: none of this costs anything
        # (no table, no thread) until notify/watch/on_change is first used.
        self._publish_ops = publish_ops
        self._event_retention_ms = int(event_retention * 1000)
        self._event_max_count = event_max_count
        self._change_key_cap = change_key_cap
        self._events_ready = False
        self._subscriptions: set[Subscription] = set()
        self._subs_lock = threading.Lock()
        self._dispatcher: Dispatcher | None = None

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
            if self._json1 is None:
                self._detect_json1(conn)
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
                    f"{SCHEMA_VERSION} supported by this lytecache install; "
                    "upgrade lytecache to open this file"
                )

    def _require_open(self) -> None:
        if self._closed:
            raise LyteCacheError("this LyteCache instance is closed")

    def _has_capacity_limits(self) -> bool:
        return self._max_keys is not None or self._max_bytes is not None

    # -- key/value ------------------------------------------------------------

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        self._require_open()
        blob, vtype = serialize(value, mode=self._serializer)
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
        self._changed("set", [key], 1)
        self._maybe_evict(conn)
        self._maybe_opportunistic_maintenance()

    def get(self, key: str, default: Any = None, cls: type[Any] | None = None) -> Any:
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
                raise LyteCacheError(str(exc)) from exc
            logger.warning("lytecache: get(%r) failed, treating as a miss: %s", key, exc)
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
            return deserialize(
                value_blob,
                value_type,
                mode=self._serializer,
                allow_pickle=self._allow_pickle,
                cls=cls,
            )
        except LyteCacheError as exc:
            if self._strict:
                raise
            logger.warning("lytecache: failed to deserialize key %r: %s", key, exc)
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
        if cur.rowcount > 0:
            self._changed("delete", list(dict.fromkeys(keys)), cur.rowcount)
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
        return self._add(key, value, ttl, emit=True)

    def _add(self, key: str, value: Any, ttl: float | None, *, emit: bool) -> bool:
        self._require_open()
        blob, vtype = serialize(value, mode=self._serializer)
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
            if emit:
                self._changed("set", [key], 1)
            self._maybe_evict(conn)
        return won

    def replace(self, key: str, value: Any, ttl: float | None = None) -> bool:
        self._require_open()
        blob, vtype = serialize(value, mode=self._serializer)
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
        replaced = cur.rowcount == 1
        if replaced:
            self._changed("set", [key], 1)
        return replaced

    def get_set(self, key: str, value: Any) -> Any:
        self._require_open()
        blob, vtype = serialize(value, mode=self._serializer)
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
        self._changed("set", [key], 1)
        if row is None:
            return None
        return deserialize(
            row[0], row[1], mode=self._serializer, allow_pickle=self._allow_pickle
        )

    def set_many(self, mapping: Mapping[str, Any], ttl: float | None = None) -> None:
        self._require_open()
        if not mapping:
            return
        now = _now_ms()
        expires_at = None if ttl is None else now + int(ttl * 1000)
        rows = []
        for k, v in mapping.items():
            blob, vtype = serialize(v, mode=self._serializer)
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
        # ONE change record for the whole batch, never one per key.
        self._changed("set", list(mapping), len(mapping))
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
            result[k] = deserialize(
                value_blob, value_type, mode=self._serializer, allow_pickle=self._allow_pickle
            )
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
        self._changed("set", [key], 1)
        text = row[0].decode("utf-8")
        return int(text) if result_type == 2 else float(text)

    # -- hashes: field-level access to JSON object values ---------------------
    #
    # A hash is not a separate type: it is a view over an ordinary value_type-4
    # row whose JSON root is an object. Anything written with set(key, {...})
    # is usable with hget, and get() on a hash returns the whole document.
    #
    # The reason these exist is atomicity, not I/O. Every write on the JSON1
    # path is a single SQL statement that computes the new document from the
    # row's *current* value (json_set/json_remove), so two processes updating
    # different fields of one key cannot lose each other's write. Where a
    # method needs an exact return value (hset's "created" flag, hdel's count),
    # it runs guarded writes -- e.g. "set this field WHERE it is absent" --
    # inside one BEGIN IMMEDIATE transaction; no write ever depends on data
    # read into Python. The read-modify-write fallback (used only when the
    # linked SQLite lacks JSON1) holds the write lock for the whole sequence.

    @property
    def _hash_json1(self) -> bool:
        return bool(self._json1)

    def _detect_json1(self, conn: sqlite3.Connection) -> None:
        if os.environ.get(_FORCE_JSON_FALLBACK_ENV):
            self._json1 = False
            logger.warning(
                "lytecache: %s is set; hash operations use the read-modify-write fallback",
                _FORCE_JSON_FALLBACK_ENV,
            )
            return
        try:
            probe = r"""SELECT json_valid('{}'), '{"a\"b":[1]}' -> '$."a\"b"'"""
            row = conn.execute(probe).fetchone()
            ok = tuple(row) == (1, "[1]")
        except sqlite3.Error:
            ok = False
        self._json1 = ok
        if not ok:
            logger.warning(
                "lytecache: SQLite %s lacks the JSON1 support hash operations need "
                "(JSON functions, the -> operator and quoted path labels; SQLite >= 3.38). "
                "Falling back to a slower read-modify-write implementation under an "
                "IMMEDIATE transaction -- still correct, but it holds the write lock longer.",
                sqlite3.sqlite_version,
            )

    @contextlib.contextmanager
    def _write_txn(self, conn: sqlite3.Connection) -> Iterator[None]:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    @staticmethod
    def _wrong_type(key: str, value_type: int, root_type: str | None) -> SerializationError:
        if value_type != 4:
            name = _TYPE_NAMES.get(value_type, "unknown")
            return SerializationError(
                f"key {key!r} holds a value of type {name} (value_type={value_type}), not a JSON "
                "object; hash operations apply only to JSON objects (value_type=4)"
            )
        if root_type is None:
            return SerializationError(f"key {key!r} holds malformed JSON (value_type=4)")
        return SerializationError(
            f"key {key!r} holds a JSON {root_type}, not a JSON object; hash operations "
            "apply only to JSON objects"
        )

    def _check_hash_type(self, conn: sqlite3.Connection, key: str, now: int) -> None:
        """Raise SerializationError if `key` is live but not a JSON object."""
        row = conn.execute(
            f"SELECT cache.value_type, {_ROOT_TYPE} FROM cache "
            f"WHERE namespace = :ns AND key = :key AND {_LIVE}",
            {"ns": self._namespace, "key": key, "now": now},
        ).fetchone()
        if row is not None and not (row[0] == 4 and row[1] == "object"):
            raise self._wrong_type(key, row[0], row[1])

    def _load_hash(self, conn: sqlite3.Connection, key: str, now: int) -> dict[str, Any] | None:
        """Fallback path: the whole live document as a dict, or None if missing."""
        row = conn.execute(
            f"SELECT value, value_type FROM cache WHERE namespace = :ns AND key = :key AND {_LIVE}",
            {"ns": self._namespace, "key": key, "now": now},
        ).fetchone()
        if row is None:
            return None
        blob, value_type = row
        if value_type != 4:
            raise self._wrong_type(key, value_type, None)
        try:
            doc = json.loads(bytes(blob).decode("utf-8"))
        except ValueError:
            raise self._wrong_type(key, 4, None) from None
        if not isinstance(doc, dict):
            root = {list: "array", str: "text", bool: "boolean", type(None): "null"}
            raise self._wrong_type(key, 4, root.get(type(doc), "number"))
        return doc

    def _rmw_hash(
        self,
        key: str,
        mutate: Callable[[dict[str, Any]], tuple[bool, Any]],
        ttl: float | None,
        *,
        create: bool = True,
    ) -> Any:
        """Fallback path: read, mutate in Python, write back -- all under the write lock.

        `mutate(doc)` changes `doc` in place and returns (changed, result).
        A missing key starts as {} when `create`, with `ttl` as its expiry.
        """
        now = _now_ms()
        conn = self._get_conn()
        with self._write_txn(conn):
            doc = self._load_hash(conn, key, now)
            exists = doc is not None
            if doc is None:
                if not create:
                    return mutate({})[1]
                if self._eviction == "noeviction" and self._has_capacity_limits():
                    self._check_capacity(conn, key, now)
                doc = {}
            changed, result = mutate(doc)
            if not changed:
                return result
            blob = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if exists:
                conn.execute(
                    "UPDATE cache SET value = ?, size_bytes = ?, last_accessed = ?, "
                    "access_count = access_count + 1 WHERE namespace = ? AND key = ?",
                    (blob, len(blob), now, self._namespace, key),
                )
            else:
                expires_at = None if ttl is None else now + int(ttl * 1000)
                conn.execute(
                    "INSERT OR REPLACE INTO cache (namespace, key, value, value_type, "
                    "created_at, expires_at, last_accessed, access_count, size_bytes) "
                    "VALUES (?, ?, ?, 4, ?, ?, ?, 0, ?)",
                    (self._namespace, key, blob, now, expires_at, now, len(blob)),
                )
        self._changed("set", [key], 1)
        self._maybe_evict(conn)
        return result

    @staticmethod
    def _require_field(field: Any) -> None:
        if not isinstance(field, str):
            raise TypeError(f"hash field names must be str, not {type(field).__name__}")

    def _upsert_hash_sql(self, live_doc: str, fresh_doc: str, where: str) -> str:
        """An UPSERT that creates `fresh_doc` for a missing/expired key, or
        rewrites a live JSON object to `live_doc` when `where` holds.

        expires_at is preserved on a live row (a field write never resets the
        TTL) and size_bytes is recomputed from the new value in the same
        statement.
        """
        new_doc = f"CAST(CASE WHEN {_EXPIRED} THEN {fresh_doc} ELSE {live_doc} END AS BLOB)"
        return f"""
            INSERT INTO cache
                (namespace, key, value, value_type, created_at, expires_at,
                 last_accessed, access_count, size_bytes)
            VALUES (:ns, :key, CAST({fresh_doc} AS BLOB), 4, :now, :exp, :now, 0,
                    length(CAST({fresh_doc} AS BLOB)))
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = {new_doc},
                value_type = 4,
                created_at = CASE WHEN {_EXPIRED} THEN :now ELSE cache.created_at END,
                expires_at = CASE WHEN {_EXPIRED} THEN :exp ELSE cache.expires_at END,
                last_accessed = :now,
                access_count = CASE WHEN {_EXPIRED} THEN 0 ELSE cache.access_count + 1 END,
                size_bytes = length({new_doc})
            WHERE {_EXPIRED} OR {where}
        """

    def _hash_params(self, key: str, now: int, ttl: float | None) -> dict[str, Any]:
        return {
            "ns": self._namespace,
            "key": key,
            "now": now,
            "exp": None if ttl is None else now + int(ttl * 1000),
        }

    def hset(self, key: str, field: str, value: Any, ttl: float | None = None) -> bool:
        """Set one field of the JSON object at `key`, atomically.

        Returns True if the field was newly created, False if it replaced an
        existing value. A missing (or expired) key is created as a new object
        with **no expiry** unless `ttl` is given; `ttl` is ignored for a key
        that already exists, whose expiry is never changed by a field write.
        Raises SerializationError if `key` holds anything but a JSON object.
        """
        self._require_open()
        self._require_field(field)
        encoded = encode_json_text(value)
        if not self._hash_json1:

            def mutate(doc: dict[str, Any]) -> tuple[bool, Any]:
                created = field not in doc
                doc[field] = json.loads(encoded)
                return True, created

            return cast(bool, self._rmw_hash(key, mutate, ttl))

        now = _now_ms()
        params = {**self._hash_params(key, now, ttl), "f0": field, "v0": encoded}
        set_expr = f"json_set({_DOC}, {_path('f0')}, json(:v0))"
        conn = self._get_conn()
        with self._write_txn(conn):
            if self._eviction == "noeviction" and self._has_capacity_limits():
                self._check_capacity(conn, key, now)
            # 1. create the key, or add the field where it is absent
            cur = conn.execute(
                self._upsert_hash_sql(
                    set_expr,
                    "json_object(:f0, json(:v0))",
                    f"({_field_type('f0')} IS NULL AND {_IS_OBJ})",
                ),
                params,
            )
            created = cur.rowcount == 1
            if not created:
                # 2. the field exists on a live object: replace it
                cur = conn.execute(
                    f"""
                    UPDATE cache SET
                        value = CAST({set_expr} AS BLOB),
                        size_bytes = length(CAST({set_expr} AS BLOB)),
                        last_accessed = :now,
                        access_count = access_count + 1
                    WHERE namespace = :ns AND key = :key AND {_LIVE} AND {_IS_OBJ}
                    """,
                    params,
                )
                if cur.rowcount == 0:
                    self._check_hash_type(conn, key, now)
        self._changed("set", [key], 1)
        self._maybe_evict(conn)
        self._maybe_opportunistic_maintenance()
        return created

    def hsetnx(self, key: str, field: str, value: Any, ttl: float | None = None) -> bool:
        """Set a field only if it is absent; atomic. Returns True if it was set.

        Creates a missing key exactly as :meth:`hset` does.
        """
        self._require_open()
        self._require_field(field)
        encoded = encode_json_text(value)
        if not self._hash_json1:

            def mutate(doc: dict[str, Any]) -> tuple[bool, Any]:
                if field in doc:
                    return False, False
                doc[field] = json.loads(encoded)
                return True, True

            return cast(bool, self._rmw_hash(key, mutate, ttl))

        now = _now_ms()
        params = {**self._hash_params(key, now, ttl), "f0": field, "v0": encoded}
        conn = self._get_conn()
        with self._write_txn(conn):
            if self._eviction == "noeviction" and self._has_capacity_limits():
                self._check_capacity(conn, key, now)
            cur = conn.execute(
                self._upsert_hash_sql(
                    f"json_set({_DOC}, {_path('f0')}, json(:v0))",
                    "json_object(:f0, json(:v0))",
                    f"({_field_type('f0')} IS NULL AND {_IS_OBJ})",
                ),
                params,
            )
            won = cur.rowcount == 1
            if not won:
                self._check_hash_type(conn, key, now)
        if won:
            self._changed("set", [key], 1)
            self._maybe_evict(conn)
        return won

    def hset_many(self, key: str, mapping: Mapping[str, Any], ttl: float | None = None) -> None:
        """Set several fields at once, atomically (Redis' multi-field HSET).

        All fields land in one transaction, or none do. Missing-key creation
        and TTL handling are the same as :meth:`hset`. Use :meth:`hset` per
        field if you need to know which fields were new.
        """
        self._require_open()
        if not mapping:
            return
        encoded: list[tuple[str, str]] = []
        for f, v in mapping.items():
            self._require_field(f)
            encoded.append((f, encode_json_text(v)))
        if not self._hash_json1:

            def mutate(doc: dict[str, Any]) -> tuple[bool, Any]:
                for f, text in encoded:
                    doc[f] = json.loads(text)
                return True, None

            self._rmw_hash(key, mutate, ttl)
            return

        now = _now_ms()
        conn = self._get_conn()
        with self._write_txn(conn):
            if self._eviction == "noeviction" and self._has_capacity_limits():
                self._check_capacity(conn, key, now)
            # SQLite caps function arguments (127 by default) and bound
            # variables, so large mappings are applied as several statements
            # -- each one json_set nested over groups of pairs -- inside this
            # single transaction.
            for start in range(0, len(encoded), _HSET_MANY_STATEMENT):
                chunk = encoded[start : start + _HSET_MANY_STATEMENT]
                params = self._hash_params(key, now, ttl)
                live_doc, fresh_doc = _DOC, "'{}'"
                for g in range(0, len(chunk), _JSON_SET_PAIRS):
                    pairs = []
                    for i in range(g, min(g + _JSON_SET_PAIRS, len(chunk))):
                        params[f"f{i}"], params[f"v{i}"] = chunk[i]
                        pairs.append(f"{_path(f'f{i}')}, json(:v{i})")
                    args = ", ".join(pairs)
                    live_doc = f"json_set({live_doc}, {args})"
                    fresh_doc = f"json_set({fresh_doc}, {args})"
                cur = conn.execute(self._upsert_hash_sql(live_doc, fresh_doc, _IS_OBJ), params)
                if cur.rowcount == 0:
                    self._check_hash_type(conn, key, now)
        self._changed("set", [key], 1)
        self._maybe_evict(conn)

    def hget(
        self, key: str, field: str, default: Any = None, cls: type[Any] | None = None
    ) -> Any:
        """Return one field of the JSON object at `key`, or `default`.

        Never raises on a miss -- a missing key, missing field or expired key
        all return `default`. `cls` reconstructs a type from an object field,
        exactly as ``get(key, cls=...)`` does. Raises SerializationError if
        `key` holds anything but a JSON object.
        """
        self._require_open()
        self._require_field(field)
        now = _now_ms()
        conn = self._get_conn()
        if self._hash_json1:
            row = conn.execute(
                f"SELECT cache.value_type, {_ROOT_TYPE}, "
                f"CASE WHEN {_IS_OBJ} THEN {_DOC} -> {_path('f0')} END "
                f"FROM cache WHERE namespace = :ns AND key = :key AND {_LIVE}",
                {"ns": self._namespace, "key": key, "now": now, "f0": field},
            ).fetchone()
            if row is not None and not (row[0] == 4 and row[1] == "object"):
                raise self._wrong_type(key, row[0], row[1])
            found = row is not None
            text = row[2] if row is not None else None
            value = default if text is None else json.loads(text)
            present = text is not None
        else:
            doc = self._load_hash(conn, key, now)
            found = doc is not None
            present = found and field in cast(dict[str, Any], doc)
            value = cast(dict[str, Any], doc)[field] if present else default
        self._record_hash_read(key, now, found)
        if present and cls is not None:
            return _build_from_cls(value, cls)
        return value

    def hmget(self, key: str, fields: Iterable[str]) -> list[Any]:
        """Return several fields in order, with None for each one that is missing."""
        self._require_open()
        field_list = list(fields)
        for f in field_list:
            self._require_field(f)
        if not field_list:
            return []
        now = _now_ms()
        conn = self._get_conn()
        if self._hash_json1:
            params: dict[str, Any] = {"ns": self._namespace, "key": key, "now": now}
            cols = []
            for i, f in enumerate(field_list):
                params[f"f{i}"] = f
                cols.append(f"CASE WHEN {_IS_OBJ} THEN {_DOC} -> {_path(f'f{i}')} END")
            row = conn.execute(
                f"SELECT cache.value_type, {_ROOT_TYPE}, {', '.join(cols)} "
                f"FROM cache WHERE namespace = :ns AND key = :key AND {_LIVE}",
                params,
            ).fetchone()
            if row is None:
                result: list[Any] = [None] * len(field_list)
            else:
                if not (row[0] == 4 and row[1] == "object"):
                    raise self._wrong_type(key, row[0], row[1])
                result = [None if t is None else json.loads(t) for t in row[2:]]
            found = row is not None
        else:
            doc = self._load_hash(conn, key, now)
            found = doc is not None
            result = [(doc or {}).get(f) for f in field_list]
        self._record_hash_read(key, now, found)
        return result

    def hgetall(self, key: str) -> dict[str, Any]:
        """Return the whole object at `key` as a dict ({} if missing)."""
        self._require_open()
        now = _now_ms()
        conn = self._get_conn()
        doc = self._load_hash(conn, key, now)
        self._record_hash_read(key, now, doc is not None)
        return doc or {}

    def hdel(self, key: str, *fields: str) -> int:
        """Remove fields; returns how many were actually present and removed.

        Unlike Redis, removing the last field leaves an empty object ``{}``
        rather than deleting the key: a hash is a view over a JSON value, and
        ``get(key)`` keeps returning that value. A missing key returns 0.
        """
        self._require_open()
        unique = list(dict.fromkeys(fields))
        for f in unique:
            self._require_field(f)
        if not unique:
            return 0
        if not self._hash_json1:

            def mutate(doc: dict[str, Any]) -> tuple[bool, Any]:
                removed = [f for f in unique if f in doc]
                for f in removed:
                    del doc[f]
                return bool(removed), len(removed)

            return cast(int, self._rmw_hash(key, mutate, None, create=False))

        now = _now_ms()
        conn = self._get_conn()
        removed = 0
        with self._write_txn(conn):
            # One guarded statement per field ("remove it WHERE it exists")
            # gives an exact count without reading the document into Python.
            for f in unique:
                remove_expr = f"json_remove({_DOC}, {_path('f0')})"
                # One hdel call is one access, however many statements it takes.
                cur = conn.execute(
                    f"""
                    UPDATE cache SET
                        value = CAST({remove_expr} AS BLOB),
                        size_bytes = length(CAST({remove_expr} AS BLOB)),
                        last_accessed = :now,
                        access_count = access_count + :bump
                    WHERE namespace = :ns AND key = :key AND {_LIVE}
                      AND {_field_type('f0')} IS NOT NULL
                    """,
                    {"ns": self._namespace, "key": key, "now": now, "f0": f,
                     "bump": 1 if removed == 0 else 0},
                )
                removed += cur.rowcount
            if removed == 0:
                self._check_hash_type(conn, key, now)
        if removed:
            self._changed("set", [key], 1)
        return removed

    def hexists(self, key: str, field: str) -> bool:
        """True if `key` is a live JSON object containing `field`."""
        self._require_open()
        self._require_field(field)
        now = _now_ms()
        conn = self._get_conn()
        if self._hash_json1:
            row = conn.execute(
                f"SELECT cache.value_type, {_ROOT_TYPE}, {_field_type('f0')} "
                f"FROM cache WHERE namespace = :ns AND key = :key AND {_LIVE}",
                {"ns": self._namespace, "key": key, "now": now, "f0": field},
            ).fetchone()
            if row is None:
                return False
            if not (row[0] == 4 and row[1] == "object"):
                raise self._wrong_type(key, row[0], row[1])
            exists = row[2] is not None
        else:
            doc = self._load_hash(conn, key, now)
            if doc is None:
                return False
            exists = field in doc
        self._buffer_lru(key, now)
        return exists

    def _hash_entries(self, key: str, with_values: bool) -> list[tuple[str, Any]]:
        now = _now_ms()
        conn = self._get_conn()
        if not self._hash_json1:
            doc = self._load_hash(conn, key, now)
            if doc is not None:
                self._buffer_lru(key, now)
            return list((doc or {}).items())
        # Guarded: on a non-object row the LEFT JOIN yields NULL, and `->` must not run.
        value_col = (
            f", CASE WHEN j.key IS NOT NULL THEN {_DOC} -> j.fullkey END" if with_values else ""
        )
        rows = conn.execute(
            f"SELECT cache.value_type, {_ROOT_TYPE}, j.key{value_col} FROM cache "
            f"LEFT JOIN json_each(CASE WHEN {_IS_OBJ} THEN {_DOC} ELSE '{{}}' END) AS j "
            f"WHERE cache.namespace = :ns AND cache.key = :key AND {_LIVE}",
            {"ns": self._namespace, "key": key, "now": now},
        ).fetchall()
        if not rows:
            return []
        if not (rows[0][0] == 4 and rows[0][1] == "object"):
            raise self._wrong_type(key, rows[0][0], rows[0][1])
        self._buffer_lru(key, now)
        return [
            (r[2], json.loads(r[3]) if with_values else None) for r in rows if r[2] is not None
        ]

    def hkeys(self, key: str) -> list[str]:
        """Field names of the object at `key`, in document order ([] if missing)."""
        self._require_open()
        return [k for k, _ in self._hash_entries(key, with_values=False)]

    def hvals(self, key: str) -> list[Any]:
        """Field values of the object at `key`, in document order ([] if missing)."""
        self._require_open()
        return [v for _, v in self._hash_entries(key, with_values=True)]

    def hlen(self, key: str) -> int:
        """Number of fields in the object at `key` (0 if missing)."""
        self._require_open()
        now = _now_ms()
        conn = self._get_conn()
        if not self._hash_json1:
            doc = self._load_hash(conn, key, now)
            if doc is not None:
                self._buffer_lru(key, now)
            return len(doc or {})
        row = conn.execute(
            f"SELECT cache.value_type, {_ROOT_TYPE}, "
            f"CASE WHEN {_IS_OBJ} THEN (SELECT count(*) FROM json_each({_DOC})) END "
            f"FROM cache WHERE namespace = :ns AND key = :key AND {_LIVE}",
            {"ns": self._namespace, "key": key, "now": now},
        ).fetchone()
        if row is None:
            return 0
        if not (row[0] == 4 and row[1] == "object"):
            raise self._wrong_type(key, row[0], row[1])
        self._buffer_lru(key, now)
        return cast(int, row[2])

    def hincrby(self, key: str, field: str, amount: int = 1, ttl: float | None = None) -> int:
        """Atomically add `amount` to an integer field and return the new value.

        A missing field starts at 0; a missing key is created (no expiry
        unless `ttl` is given). Raises TypeError if the field holds anything
        but a JSON integer, and OverflowError rather than leaving the signed
        64-bit range -- in which case nothing is written.
        """
        self._require_open()
        self._require_field(field)
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError(f"hincrby amount must be an int, not {type(amount).__name__}")
        if not _INT64_MIN <= amount <= _INT64_MAX:
            raise OverflowError(f"hincrby amount {amount} is outside the signed 64-bit range")
        return cast(int, self._hincr(key, field, amount, ttl, integer=True))

    def hincrbyfloat(
        self, key: str, field: str, amount: float, ttl: float | None = None
    ) -> float:
        """Atomically add `amount` to a numeric field and return the new float value.

        The field may hold an integer or a float; the result is stored as a
        float. Otherwise behaves like :meth:`hincrby`.
        """
        self._require_open()
        self._require_field(field)
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise TypeError(f"hincrbyfloat amount must be a number, not {type(amount).__name__}")
        amount = float(amount)
        if not math.isfinite(amount):
            raise ValueError("hincrbyfloat amount must be finite")
        return cast(float, self._hincr(key, field, amount, ttl, integer=False))

    def _hincr(
        self, key: str, field: str, amount: Union[int, float], ttl: float | None, *, integer: bool
    ) -> Union[int, float]:
        kind = "an integer" if integer else "a number"
        if not self._hash_json1:

            def mutate(doc: dict[str, Any]) -> tuple[bool, Any]:
                current: Any = doc.get(field, 0)
                ok_types: tuple[type, ...] = (int,) if integer else (int, float)
                if isinstance(current, bool) or not isinstance(current, ok_types):
                    raise TypeError(f"hash field {field!r} of key {key!r} is not {kind}")
                new: Union[int, float] = cast(Union[int, float], current) + amount
                if integer and not _INT64_MIN <= new <= _INT64_MAX:
                    raise OverflowError(
                        f"hincrby on field {field!r} of key {key!r} would overflow int64"
                    )
                if not integer:
                    new = float(new)
                    if not math.isfinite(new):
                        raise OverflowError(
                            f"hincrbyfloat on field {field!r} of key {key!r} would overflow"
                        )
                doc[field] = new
                return True, new

            return cast(Union[int, float], self._rmw_hash(key, mutate, ttl))

        now = _now_ms()
        params = {**self._hash_params(key, now, ttl), "f0": field, "amount": amount}
        stored = f"json_extract({_DOC}, {_path('f0')})"
        if integer:
            new_val = f"(COALESCE({stored}, 0) + :amount)"
            guard = (
                f"COALESCE({_field_type('f0')}, 'integer') = 'integer' "
                f"AND typeof({new_val}) = 'integer'"
            )
        else:
            new_val = f"(CAST(COALESCE({stored}, 0) AS REAL) + :amount)"
            guard = (
                f"COALESCE({_field_type('f0')}, 'real') IN ('integer', 'real') "
                f"AND {new_val} BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308"
            )
        stored_val = new_val if integer else f"json({_float_text(new_val)})"
        sql = self._upsert_hash_sql(
            f"json_set({_DOC}, {_path('f0')}, {stored_val})",
            "json_object(:f0, :amount)",
            f"(CASE WHEN {_IS_OBJ} THEN ({guard}) ELSE 0 END)",
        )
        sql += f" RETURNING json_extract(CAST(value AS TEXT), {_path('f0')})"
        conn = self._get_conn()
        with self._write_txn(conn):
            if self._eviction == "noeviction" and self._has_capacity_limits():
                self._check_capacity(conn, key, now)
            rows = conn.execute(sql, params).fetchall()
            if not rows:
                self._check_hash_type(conn, key, now)
                ftype = conn.execute(
                    f"SELECT {_field_type('f0')} FROM cache WHERE namespace = :ns AND key = :key",
                    params,
                ).fetchone()[0]
                if (integer and ftype == "integer") or (
                    not integer and ftype in ("integer", "real")
                ):
                    raise OverflowError(
                        f"incrementing field {field!r} of key {key!r} would overflow"
                    )
                raise TypeError(
                    f"hash field {field!r} of key {key!r} holds a JSON {ftype}, not {kind}"
                )
        self._changed("set", [key], 1)
        self._maybe_evict(conn)
        result = rows[0][0]
        return int(result) if integer else float(result)

    def _record_hash_read(self, key: str, now: int, found: bool) -> None:
        with self._lock:
            if found:
                self._hits += 1
            else:
                self._misses += 1
        if found:
            self._buffer_lru(key, now)
        self._maybe_opportunistic_maintenance()

    # -- events: local notification (notify / watch) ---------------------------
    #
    # SINGLE HOST ONLY. POLLED, NOT PUSHED. DURABLE. This is not Redis Pub/Sub:
    # events are rows in the `events` table with a monotonic `seq`; watchers on
    # the same machine poll it (default every 50 ms, backing off to ~1 s when
    # idle). Subscribers on another machine never see anything, and SQLite
    # locking is unreliable on network filesystems, so this cannot be stretched
    # across hosts. See docs/notifications.md.

    def _ensure_events(self, conn: sqlite3.Connection) -> None:
        if not self._events_ready:
            conn.executescript(EVENTS_DDL)
            self._events_ready = True

    def _events_exist(self, conn: sqlite3.Connection) -> bool:
        if not self._events_ready:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'"
            ).fetchone()
            self._events_ready = row is not None
        return self._events_ready

    def notify(self, channel: str, payload: Any = None) -> int:
        """Publish an event for watchers **on this host**; returns its ``seq``.

        Single host only, delivered by polling (not pushed), and durable: the
        event is a committed row that watchers read until retention deletes it
        (``event_retention``, default 1 hour). ``payload`` is encoded exactly
        like a cached value. Each call is one write under the same lock as
        :meth:`set`, so a high event rate slows cache writes; batch with
        :meth:`notify_many`. Call it *after* your own database transaction
        commits, never from inside one you might roll back.
        """
        self._require_open()
        validate_channel(channel)
        return self._insert_events([(channel, payload)])[0]

    def notify_many(self, events: Iterable[tuple[str, Any]]) -> list[int]:
        """Publish several ``(channel, payload)`` events in one transaction.

        Same single-host, polled, durable semantics as :meth:`notify`, but one
        write-lock acquisition for the whole batch. Returns the seqs in order.
        """
        self._require_open()
        items = list(events)
        for channel, _ in items:
            validate_channel(channel)
        return self._insert_events(items) if items else []

    def _insert_events(self, items: list[tuple[str, Any]]) -> list[int]:
        rows = []
        now = _now_ms()
        for channel, payload in items:
            blob, vtype = serialize(payload, mode=self._serializer)
            rows.append((channel, self._namespace, blob, vtype, now))
        conn = self._get_conn()
        self._ensure_events(conn)
        sql = (
            "INSERT INTO events (channel, namespace, payload, value_type, created_at) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        seqs: list[int] = []
        if len(rows) == 1:
            seqs.append(cast(int, conn.execute(sql, rows[0]).lastrowid))
        else:
            with self._write_txn(conn):
                for row in rows:
                    seqs.append(cast(int, conn.execute(sql, row).lastrowid))
        # In-process optimization only: wake local watchers now instead of at
        # their next poll. The committed row stays the source of truth.
        with self._subs_lock:
            for sub in self._subscriptions:
                sub.wake.set()
        return seqs

    def watch(
        self,
        *channels: str,
        from_seq: int | None = None,
        poll_interval: float = 0.05,
    ) -> Subscription:
        """Subscribe to events published **on this host** to matching channels.

        Single host only, polled (latency is about `poll_interval`, backing off
        to ~1 s while idle), durable and resumable. A channel is an exact name,
        or ends in ``*`` to match a prefix (``"cache:invalidate:*"``); ``"*"``
        matches every channel. Only events from this cache's namespace are seen.

        - ``from_seq=None``: only events published from now on.
        - ``from_seq=N``: resume after seq N (e.g. a cursor you stored).
        - ``from_seq=0``: replay everything still retained.

        Delivery is at-least-once from your cursor, in total ``seq`` order, to
        every subscriber (fan-out; there are no consumer groups). Handlers must
        be idempotent. If events after your cursor were already deleted by
        retention, the first read raises :class:`GapError` rather than
        silently resuming elsewhere. Always ``close()`` the subscription, or
        use it as a context manager.
        """
        self._require_open()
        if not channels:
            raise ValueError("watch() needs at least one channel or pattern")
        channel_filter_sql(channels)  # validates the patterns
        if from_seq is not None and (isinstance(from_seq, bool) or from_seq < 0):
            raise ValueError("from_seq must be None or an int >= 0")
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        conn = self._get_conn()
        self._ensure_events(conn)
        gap = None
        conn.execute("BEGIN")
        try:
            if from_seq is None:
                start = last_assigned_seq(conn)
            elif from_seq == 0:
                oldest = conn.execute("SELECT min(seq) FROM events").fetchone()[0]
                start = (oldest - 1) if oldest is not None else last_assigned_seq(conn)
            else:
                start = from_seq
                gap = gap_check(conn, start)
        finally:
            conn.execute("COMMIT")
        sub = Subscription(
            path=str(self._path),
            namespace=self._namespace,
            patterns=channels,
            start_seq=start,
            gap=gap,
            poll_interval=poll_interval,
            serializer=self._serializer,
            allow_pickle=self._allow_pickle,
            on_close=self._forget_subscription,
        )
        with self._subs_lock:
            self._subscriptions.add(sub)
        return sub

    def _forget_subscription(self, sub: Subscription) -> None:
        with self._subs_lock:
            self._subscriptions.discard(sub)

    def save_cursor(self, name: str, seq: int) -> None:
        """Persist a named subscriber cursor (in the ``meta`` table).

        Store ``event.seq`` after you have *finished* handling an event, then
        resume with ``watch(..., from_seq=cache.load_cursor(name))``. Each
        save is a write; for busy streams save every N events, accepting
        that up to N may be redelivered after a crash.
        """
        self._require_open()
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (f"cursor:{self._namespace}:{name}", str(int(seq))),
        )

    def load_cursor(self, name: str) -> int | None:
        """The cursor last saved under `name`, or None if there is none."""
        self._require_open()
        row = (
            self._get_conn()
            .execute("SELECT v FROM meta WHERE k = ?", (f"cursor:{self._namespace}:{name}",))
            .fetchone()
        )
        return int(row[0]) if row else None

    def _sweep_events(self, conn: sqlite3.Connection, now: int) -> None:
        """Retention, in the sweeper's bounded-batch style.

        Always deletes a *prefix* of the seq range (everything up to the
        newest event past retention), never a hole in the middle, even if
        clocks disagree -- which is what keeps gap detection sound.
        """
        if not self._events_exist(conn):
            return
        bounds = [
            conn.execute(
                "SELECT max(seq) FROM events WHERE created_at <= ?",
                (now - self._event_retention_ms,),
            ).fetchone()[0]
        ]
        if self._event_max_count is not None:
            bounds.append(
                conn.execute(
                    "SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?",
                    (self._event_max_count,),
                ).fetchone()
            )
            bounds[-1] = bounds[-1][0] if bounds[-1] else None
        bound = max((b for b in bounds if b is not None), default=None)
        if bound is None:
            return
        while True:
            cur = conn.execute(
                "DELETE FROM events WHERE seq IN "
                "(SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)",
                (bound, _EXPIRE_BATCH),
            )
            if cur.rowcount < _EXPIRE_BATCH:
                return

    # -- change listeners (in-process) and auto-publish -------------------------

    def on_change(
        self,
        listener: Callable[[ChangeRecord], Any],
        events: Iterable[str] = ("set", "delete", "flush"),
    ) -> Callable[[], None]:
        """Call `listener(record)` after mutations in **this process** commit.

        In-process only, no SQLite involvement, effectively zero latency. A
        record arrives only after its transaction commits (a rolled-back
        ``set_many`` notifies nobody), on a dispatch thread that holds no cache
        lock, so a slow listener never stalls writers and a listener may write
        to the cache itself. Exceptions are logged and never affect the write.
        Writes made by listeners can trigger listeners again; past 8 levels
        those records are dropped with a warning. Bulk operations produce ONE
        record (see :class:`ChangeRecord`). Returns an unsubscribe function;
        once it returns the listener is never called again, even for records
        that were already queued.

        ``"set"`` covers every write that changes a value (set, set_many, add,
        replace, get_set, counters, hash writes); ``"delete"`` is
        :meth:`delete`; ``"flush"`` is :meth:`flush`. TTL expiry and eviction
        are not reported.
        """
        self._require_open()
        with self._subs_lock:
            if self._dispatcher is None:
                self._dispatcher = Dispatcher()  # first listener starts the thread
            dispatcher = self._dispatcher
        return dispatcher.add(listener, events)

    def _changed(self, op: str, keys: list[str] | None, count: int) -> None:
        dispatcher = self._dispatcher
        to_listeners = dispatcher is not None and dispatcher.wants(op)
        publish = op in self._publish_ops
        if not (to_listeners or publish):
            return  # the common case: nothing registered, nothing to do
        record = ChangeRecord.make(op, self._namespace, keys, count, self._change_key_cap)
        if to_listeners:
            cast(Dispatcher, dispatcher).submit(record)
        if publish:
            try:
                self._insert_events([(CACHE_CHANNEL, record.to_payload())])
            except sqlite3.Error as exc:
                if self._strict:
                    raise LyteCacheError(str(exc)) from exc
                logger.warning("lytecache: failed to publish %s change event: %s", op, exc)

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
        cur = conn.execute("DELETE FROM cache WHERE namespace = ?", (self._namespace,))
        with self._lock:
            self._lru_buffer.clear()
        if cur.rowcount > 0:
            self._changed("flush", None, cur.rowcount)

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
            "hash_mode": "json1" if self._json1 else "fallback",
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
        with self._subs_lock:
            subs = list(self._subscriptions)
        for sub in subs:
            sub.close()
        if self._dispatcher is not None:
            self._dispatcher.close()
            self._dispatcher = None
        if self._sweeper is not None:
            self._sweeper.stop()
        self._flush_lru_buffer()
        with self._conn_registry_lock:
            for conn in self._connections:
                with contextlib.suppress(sqlite3.Error):
                    conn.close()
            self._connections.clear()

    def __enter__(self) -> LyteCache:
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
            if self._add(key, token, timeout, emit=False):
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
            logger.warning("lytecache: failed to flush LRU bookkeeping buffer")
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
            self._sweep_events(conn, now)
        except sqlite3.Error as exc:
            logger.warning("lytecache: background sweep failed: %s", exc)

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
