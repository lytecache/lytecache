"""on_change (in-process listeners) and publish_changes (cross-process auto-publish)."""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time

import pytest

from lytecache import ChangeRecord, LyteCache


def dispatch_threads():
    return [t for t in threading.enumerate() if t.name == "lytecache-dispatch"]


def collector(cache, **kwargs):
    q: queue.Queue[ChangeRecord] = queue.Queue()
    unsubscribe = cache.on_change(q.put, **kwargs)
    return q, unsubscribe


def drain(q, settle=0.2):
    time.sleep(settle)
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def events_rows(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'"
        ).fetchone()
        return None if exists is None else conn.execute("SELECT * FROM events").fetchall()
    finally:
        conn.close()


# -- in-process listeners ---------------------------------------------------


def test_set_many_of_2000_keys_emits_exactly_one_truncated_record(cache):
    q, _ = collector(cache)
    cache.set_many({f"k{i}": i for i in range(2000)})
    records = drain(q)
    assert records == [ChangeRecord("set", "default", None, 2000, True)]


def test_small_bulk_records_carry_keys(cache):
    q, _ = collector(cache)
    cache.set_many({"a": 1, "b": 2})
    cache.delete("a", "b", "a")
    assert drain(q) == [
        ChangeRecord("set", "default", ("a", "b"), 2, False),
        ChangeRecord("delete", "default", ("a", "b"), 2, False),
    ]


def test_key_cap_is_configurable_and_applies_to_delete(db_path):
    c = LyteCache(db_path, sweep_interval=None, change_key_cap=3)
    try:
        q, _ = collector(c)
        c.set_many({f"k{i}": i for i in range(5)})
        c.delete(*[f"k{i}" for i in range(4)])
        c.delete("k4")
        recs = drain(q)
        assert [(r.op, r.keys, r.count, r.truncated) for r in recs] == [
            ("set", None, 5, True),
            ("delete", None, 4, True),
            ("delete", ("k4",), 1, False),
        ]
    finally:
        c.close()


def test_flush_always_truncated(cache):
    cache.set_many({"a": 1, "b": 2, "c": 3})
    q, _ = collector(cache)
    cache.flush()
    assert drain(q) == [ChangeRecord("flush", "default", None, 3, True)]


def test_every_value_write_reports_set(cache):
    q, _ = collector(cache, events={"set"})
    cache.set("a", 1)
    cache.add("b", 1)
    cache.add("b", 2)  # lost: no change, no record
    cache.replace("a", 2)
    cache.replace("missing", 2)  # no change
    cache.get_set("a", 3)
    cache.incr("n")
    cache.incr_float("f", 0.5)
    cache.hset("h", "f", 1)
    cache.hsetnx("h", "f", 2)  # no change
    cache.hset_many("h", {"g": 1})
    cache.hincrby("h", "n")
    cache.hdel("h", "nope")  # no change
    cache.hdel("h", "g")
    keys = [r.keys for r in drain(q)]
    assert keys == [("a",), ("b",), ("a",), ("a",), ("n",), ("f",)] + [("h",)] * 4


def test_no_change_no_record(cache):
    q, _ = collector(cache)
    cache.delete("missing")
    cache.flush()  # empty namespace
    assert drain(q) == []


def test_event_filter(cache):
    q, _ = collector(cache, events={"delete"})
    cache.set("a", 1)
    cache.delete("a")
    assert [r.op for r in drain(q)] == ["delete"]
    with pytest.raises(ValueError):
        cache.on_change(print, events={"expire"})
    with pytest.raises(ValueError):
        cache.on_change(print, events=set())


def test_lock_keys_do_not_emit(cache):
    q, _ = collector(cache)
    with cache.lock("job"):
        pass
    assert drain(q) == []


def test_not_called_when_transaction_rolls_back(cache, db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TRIGGER boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' "
        "BEGIN SELECT RAISE(ABORT, 'boom'); END"
    )
    conn.commit()
    conn.close()
    q, _ = collector(cache)
    with pytest.raises(sqlite3.IntegrityError):
        cache.set_many({"a": 1, "b": 2, "boom": 3})
    assert drain(q) == []
    assert cache.get("a") is None  # really rolled back


def test_throwing_listener_does_not_fail_write_and_others_still_run(cache, caplog):
    def bad(record):
        raise RuntimeError("listener bug")

    cache.on_change(bad)
    q, _ = collector(cache)
    with caplog.at_level(logging.ERROR, logger="lytecache"):
        cache.set("a", 1)
        assert len(drain(q)) == 1
    assert cache.get("a") == 1
    assert any("listener bug" in r.getMessage() or r.exc_info for r in caplog.records)


def test_slow_listener_does_not_block_writers(cache):
    release = threading.Event()
    cache.on_change(lambda r: release.wait(5))
    t = time.perf_counter()
    for i in range(20):
        cache.set(f"k{i}", i)
    assert time.perf_counter() - t < 2
    release.set()


def test_listener_may_write_to_cache_without_deadlock(cache):
    done = threading.Event()

    def mirror(record):
        if record.keys == ("src",):
            cache.set("mirror", cache.get("src"))
            done.set()

    cache.on_change(mirror, events={"set"})
    cache.set("src", 42)
    assert done.wait(5)
    assert cache.get("mirror") == 42


def test_depth_guard_stops_runaway_recursion(cache, caplog):
    calls = []

    def recurse(record):
        calls.append(record)
        cache.incr("depth")  # each write triggers this listener again

    cache.on_change(recurse, events={"set"})
    with caplog.at_level(logging.WARNING, logger="lytecache"):
        cache.set("start", 1)
        time.sleep(1.0)
    assert len(calls) == 8  # MAX_DISPATCH_DEPTH, then dropped
    assert cache.get("depth") == 8
    assert any("levels deep" in r.getMessage() for r in caplog.records)


def test_unsubscribe(cache):
    q, unsubscribe = collector(cache)
    cache.set("a", 1)
    first = q.get(timeout=5)
    unsubscribe()
    unsubscribe()  # idempotent
    cache.set("b", 1)
    assert first.keys == ("a",) and drain(q) == []


def test_unsubscribe_takes_effect_immediately(cache):
    gate = threading.Event()
    cache.on_change(lambda r: gate.wait(5))  # holds the dispatch thread
    q, unsubscribe = collector(cache)
    cache.set("a", 1)
    unsubscribe()  # the queued record is not delivered after this returns
    gate.set()
    assert drain(q) == []


def test_no_dispatch_thread_until_first_listener_and_none_after_close(db_path):
    c = LyteCache(db_path, sweep_interval=None)
    c.set("a", 1)
    assert dispatch_threads() == []
    c.on_change(lambda r: None)
    assert len(dispatch_threads()) == 1
    c.close()
    assert dispatch_threads() == []


# -- publish_changes (cross-process, costs a write) --------------------------


def test_publish_changes_unset_writes_no_events(cache, db_path):
    cache.set("a", 1)
    cache.set_many({"b": 1})
    cache.delete("a")
    cache.flush()
    assert events_rows(db_path) is None  # table never created


def test_publish_changes_per_operation(db_path):
    c = LyteCache(db_path, sweep_interval=None, publish_changes={"delete", "flush"})
    try:
        with pytest.raises(ValueError):
            c.notify("__cache__", 1)  # reserved: only lytecache publishes there
        with c.watch("__cache__") as sub:
            c.set("a", 1)  # not published
            c.set_many({f"k{i}": i for i in range(150)})  # not published
            c.delete("a", "missing")
            c.delete(*[f"k{i}" for i in range(120)])
            c.flush()
            got = [sub.poll(5).payload for _ in range(3)]
            assert sub.poll(0.3) is None
        assert got == [
            {"op": "delete", "namespace": "default", "keys": ["a", "missing"], "count": 1,
             "truncated": False},
            {"op": "delete", "namespace": "default", "keys": None, "count": 120,
             "truncated": True},
            {"op": "flush", "namespace": "default", "keys": None, "count": 30,
             "truncated": True},
        ]
    finally:
        c.close()


def test_publish_changes_rejects_unknown_ops(db_path):
    with pytest.raises(ValueError):
        LyteCache(db_path, publish_changes={"expire"})


def test_invalidation_recipe_across_two_instances(db_path):
    """The headline example, as written in docs/notifications.md."""
    worker_a = LyteCache(db_path, sweep_interval=None)
    worker_b = LyteCache(db_path, sweep_interval=None)
    local_memo = {"balance:42": 100, "balance:7": 5}
    try:
        with worker_b.watch("invalidate") as events:
            # worker A, after committing a DB change
            worker_a.delete("balance:42")
            worker_a.notify("invalidate", {"key": "balance:42"})
            # workers B..N
            event = events.poll(5)
            local_memo.pop(event.payload["key"], None)
        assert local_memo == {"balance:7": 5}
    finally:
        worker_a.close()
        worker_b.close()
