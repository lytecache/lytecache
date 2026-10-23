"""Local event notification (notify/watch) and in-process change listeners.

Two separate mechanisms live here; see SPEC.md, "Events":

- ``Subscription``: the cross-process ``watch`` side. SINGLE HOST ONLY,
  POLLED (latency = poll interval), DURABLE (events are rows in the
  ``events`` table with a monotonic ``seq`` cursor, kept for the retention
  window). This is not Redis Pub/Sub. Each subscription owns one background
  thread and one read-only connection; it never takes the write lock.
- ``Dispatcher``: in-process ``on_change`` listeners. No SQLite involvement:
  change records are queued after a mutation commits and delivered on one
  dispatch thread, so a slow or throwing listener never holds up a writer.
"""

from __future__ import annotations

import dataclasses
import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Callable

from ._serialize import deserialize
from .exceptions import GapError

logger = logging.getLogger("lytecache")

EVENTS_BATCH = 500
MAX_IDLE_INTERVAL = 1.0
CACHE_CHANNEL = "__cache__"
CHANGE_OPS = frozenset({"set", "delete", "flush"})
MAX_DISPATCH_DEPTH = 8
_QUEUE_SIZE = 10_000


def validate_channel(channel: Any) -> str:
    """Channels a caller may publish to: non-empty, no '*', not reserved."""
    if not isinstance(channel, str) or not channel:
        raise ValueError("channel must be a non-empty str")
    if "*" in channel:
        raise ValueError(f"channel {channel!r} may not contain '*' (reserved for watch patterns)")
    if channel.startswith("__"):
        raise ValueError(f"channel names starting with '__' are reserved: {channel!r}")
    return channel


def channel_filter_sql(patterns: Sequence[str]) -> tuple[str, list[Any]]:
    """SQL for the channel-matching rule: exact name, or a trailing '*' prefix glob.

    ``"orders"`` matches only ``orders``; ``"cache:invalidate:*"`` matches
    every channel starting with ``cache:invalidate:``; a lone ``"*"`` matches
    everything. A '*' anywhere but the end is not special.
    """
    clauses: list[str] = []
    params: list[Any] = []
    for p in patterns:
        if not isinstance(p, str) or not p:
            raise ValueError("watch patterns must be non-empty strings")
        if p.endswith("*"):
            prefix = p[:-1]
            if not prefix:
                return "1", []
            clauses.append("substr(channel, 1, ?) = ?")
            params.extend([len(prefix), prefix])
        else:
            clauses.append("channel = ?")
            params.append(p)
    return "(" + " OR ".join(clauses) + ")", params


@dataclasses.dataclass(frozen=True)
class Event:
    """One published event. ``payload`` is decoded with the cache value rules."""

    seq: int
    channel: str
    namespace: str
    created_at: int  # unix ms
    _blob: bytes | None = dataclasses.field(repr=False, compare=False)
    _value_type: int = dataclasses.field(repr=False, compare=False)
    _mode: str = dataclasses.field(repr=False, compare=False, default="auto")
    _allow_pickle: bool = dataclasses.field(repr=False, compare=False, default=False)

    @property
    def payload(self) -> Any:
        if self._blob is None:
            return None
        return deserialize(
            bytes(self._blob), self._value_type, mode=self._mode, allow_pickle=self._allow_pickle
        )


@dataclasses.dataclass(frozen=True)
class ChangeRecord:
    """One committed mutation, as seen by ``on_change`` listeners.

    A bulk operation produces ONE record. ``keys`` is the list of keys the
    call targeted, or None when there were more than the configured cap
    (``truncated`` is then True); ``flush`` always has ``keys=None``.
    """

    op: str
    namespace: str
    keys: tuple[str, ...] | None
    count: int
    truncated: bool

    @classmethod
    def make(
        cls, op: str, namespace: str, keys: Sequence[str] | None, count: int, cap: int
    ) -> ChangeRecord:
        if keys is None or len(keys) > cap:
            return cls(op, namespace, None, count, True)
        return cls(op, namespace, tuple(keys), count, False)

    def to_payload(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "namespace": self.namespace,
            "keys": None if self.keys is None else list(self.keys),
            "count": self.count,
            "truncated": self.truncated,
        }


_CLOSED = object()


class Subscription:
    """A live, polled view of the ``events`` table for some channels.

    SINGLE HOST ONLY, POLLED, DURABLE -- see the module docstring. Iterate
    it (blocking), or use :meth:`poll` / :meth:`try_next`. Delivery is
    at-least-once from your stored cursor, in ``seq`` order; handlers must be
    idempotent. Always :meth:`close` it (or use it as a context manager).
    """

    def __init__(
        self,
        *,
        path: str,
        namespace: str,
        patterns: Sequence[str],
        start_seq: int,
        gap: GapError | None,
        poll_interval: float,
        serializer: str,
        allow_pickle: bool,
        on_close: Callable[[Subscription], None],
    ) -> None:
        self._path = path
        self._namespace = namespace
        self._patterns = tuple(patterns)
        self._filter_sql, self._filter_params = channel_filter_sql(self._patterns)
        self._cursor = start_seq
        self._last_seq = start_seq
        self._poll_interval = poll_interval
        self._serializer = serializer
        self._allow_pickle = allow_pickle
        self._on_close = on_close
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=_QUEUE_SIZE)
        self._stop = threading.Event()
        self.wake = threading.Event()
        self._closed = False
        self._close_lock = threading.Lock()
        if gap is not None:
            self._queue.put(gap)
            self._thread: threading.Thread | None = None
        else:
            self._thread = threading.Thread(
                target=self._run, name="lytecache-watch", daemon=True
            )
            self._thread.start()

    @property
    def channels(self) -> tuple[str, ...]:
        return self._patterns

    @property
    def last_seq(self) -> int:
        """seq of the last event handed to you (the value to persist as a cursor)."""
        return self._last_seq

    # -- consumer side --------------------------------------------------------

    def __iter__(self) -> Iterator[Event]:
        return self

    def __next__(self) -> Event:
        while True:
            if self._closed and self._queue.empty():
                raise StopIteration
            try:
                item = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            result = self._take(item)
            if result is _CLOSED:
                raise StopIteration
            return result  # type: ignore[no-any-return]

    def poll(self, timeout: float | None = None) -> Event | None:
        """Wait up to `timeout` seconds (forever if None) for the next event.

        Returns None on timeout or once the subscription is closed. Raises
        GapError if events this subscription should have seen were deleted
        by retention before it read them.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed and self._queue.empty():
                return None
            wait = 0.25 if deadline is None else min(0.25, deadline - time.monotonic())
            if wait <= 0:
                return None
            try:
                item = self._queue.get(timeout=wait)
            except queue.Empty:
                continue
            result = self._take(item)
            return None if result is _CLOSED else result

    def try_next(self) -> Event | None:
        """The next already-fetched event, without waiting; None if there is none."""
        try:
            item = self._queue.get_nowait()
        except queue.Empty:
            return None
        result = self._take(item)
        return None if result is _CLOSED else result

    def _take(self, item: Any) -> Any:
        if isinstance(item, GapError):
            self.close()
            raise item
        if item is _CLOSED:
            self._closed = True
            return _CLOSED
        self._last_seq = item.seq
        return item

    def close(self) -> None:
        """Stop polling and release the thread and connection. Idempotent."""
        with self._close_lock:
            if self._stop.is_set():
                self._closed = True
                return
            self._stop.set()
        self._closed = True
        self.wake.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        self._on_close(self)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- poller side ----------------------------------------------------------

    def _run(self) -> None:
        conn: sqlite3.Connection | None = None
        interval = self._poll_interval
        try:
            conn = sqlite3.connect(
                self._path, timeout=5.0, isolation_level=None, check_same_thread=False
            )
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA query_only=1")  # a subscription never writes
            while not self._stop.is_set():
                self.wake.clear()
                try:
                    rows, gap = self._fetch(conn)
                except sqlite3.Error as exc:
                    logger.warning("lytecache: watch poll failed, retrying: %s", exc)
                    rows, gap = [], None
                if gap is not None:
                    self._put(gap)
                    return
                for row in rows:
                    if not self._put(self._event(row)):
                        return
                if rows:
                    self._cursor = rows[-1][0]
                    interval = self._poll_interval
                    if len(rows) == EVENTS_BATCH:
                        continue  # more are waiting: read on without sleeping
                else:
                    interval = min(interval * 2, max(MAX_IDLE_INTERVAL, self._poll_interval))
                self.wake.wait(interval)
        except Exception:  # pragma: no cover - defensive: never kill silently
            logger.exception("lytecache: watch thread crashed")
        finally:
            if conn is not None:
                conn.close()
            self._put(_CLOSED, force=True)

    def _fetch(self, conn: sqlite3.Connection) -> tuple[list[Any], GapError | None]:
        # One read transaction, so the gap check and the fetch see the same
        # snapshot: the sweeper cannot delete events between the two.
        conn.execute("BEGIN")
        try:
            gap = gap_check(conn, self._cursor)
            if gap is not None:
                return [], gap
            rows = conn.execute(
                "SELECT seq, channel, namespace, payload, value_type, created_at FROM events "
                f"WHERE seq > ? AND namespace = ? AND {self._filter_sql} ORDER BY seq LIMIT ?",
                [self._cursor, self._namespace, *self._filter_params, EVENTS_BATCH],
            ).fetchall()
            return rows, None
        finally:
            conn.execute("COMMIT")

    def _event(self, row: Any) -> Event:
        seq, channel, namespace, payload, value_type, created_at = row
        return Event(
            seq, channel, namespace, created_at, payload, value_type, self._serializer,
            self._allow_pickle,
        )

    def _put(self, item: Any, *, force: bool = False) -> bool:
        while True:
            if self._stop.is_set() and not force:
                return False
            try:
                self._queue.put(item, timeout=0.1)
                return True
            except queue.Full:
                if force:
                    return False


def last_assigned_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'events'").fetchone()
    return int(row[0]) if row else 0


def gap_check(conn: sqlite3.Connection, cursor: int) -> GapError | None:
    """GapError if events after `cursor` may have been deleted, else None.

    Retention deletes the oldest events, so a cursor older than the oldest
    retained event minus one has lost events. A cursor *ahead* of the last
    seq ever assigned means the file was recreated, which is a gap too.
    """
    last = last_assigned_seq(conn)
    if cursor > last:
        return GapError(cursor, last + 1, ahead=True)
    oldest = conn.execute("SELECT min(seq) FROM events").fetchone()[0]
    if oldest is None:
        oldest = last + 1
    if cursor < oldest - 1:
        return GapError(cursor, oldest)
    return None


class Dispatcher:
    """In-process ``on_change`` delivery: one daemon thread, started lazily."""

    def __init__(self) -> None:
        self._listeners: dict[int, tuple[Callable[[ChangeRecord], Any], frozenset[str]]] = {}
        self._next_id = 0
        self._lock = threading.Lock()
        self._queue: queue.Queue[Any] = queue.Queue()
        self._local = threading.local()
        self._thread = threading.Thread(target=self._run, name="lytecache-dispatch", daemon=True)
        self._thread.start()

    def add(
        self, listener: Callable[[ChangeRecord], Any], ops: Iterable[str]
    ) -> Callable[[], None]:
        op_set = frozenset(ops)
        unknown = op_set - CHANGE_OPS
        if unknown or not op_set:
            raise ValueError(f"on_change events must be a non-empty subset of {sorted(CHANGE_OPS)}")
        with self._lock:
            token = self._next_id
            self._next_id += 1
            self._listeners[token] = (listener, op_set)

        def unsubscribe() -> None:
            with self._lock:
                self._listeners.pop(token, None)

        return unsubscribe

    def wants(self, op: str) -> bool:
        with self._lock:
            return any(op in ops for _, ops in self._listeners.values())

    def submit(self, record: ChangeRecord) -> None:
        depth = getattr(self._local, "depth", 0) + 1
        if depth > MAX_DISPATCH_DEPTH:
            logger.warning(
                "lytecache: dropping %s change record: on_change listeners re-entered the cache "
                "%d levels deep (a listener's write triggered listeners again, repeatedly)",
                record.op,
                MAX_DISPATCH_DEPTH,
            )
            return
        self._queue.put((record, depth))

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _CLOSED:
                return
            record, depth = item
            with self._lock:
                targets = [fn for fn, ops in self._listeners.values() if record.op in ops]
            self._local.depth = depth
            for fn in targets:
                try:
                    fn(record)
                except Exception:
                    logger.exception("lytecache: on_change listener %r raised", fn)
            self._local.depth = 0

    def close(self) -> None:
        with self._lock:
            self._listeners.clear()
        self._queue.put(_CLOSED)
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("lytecache: an on_change listener is still running at close()")
