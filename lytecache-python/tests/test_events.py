"""notify/watch: local, polled, durable event notification (SPEC.md, "Events")."""

from __future__ import annotations

import json
import multiprocessing
import os
import pickle
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from _workers import (
    cursor_subscriber_worker,
    notifier_worker,
    publisher_worker,
    setter_worker,
    subscriber_worker,
)

from lytecache import GapError, LyteCache, SerializationError
from lytecache._schema import DDL


def table_exists(db_path, name="events"):
    conn = sqlite3.connect(str(db_path))
    try:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
            ).fetchone()
            is not None
        )
    finally:
        conn.close()


def watch_threads():
    return [t for t in threading.enumerate() if t.name.startswith("lytecache-watch")]


def wait_for(cond, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return False


# -- basics -------------------------------------------------------------------


def test_notify_returns_increasing_seq_and_watch_receives(cache):
    with cache.watch("orders") as sub:
        s1 = cache.notify("orders", {"id": 1})
        s2 = cache.notify("orders", {"id": 2})
        assert s2 > s1
        e1, e2 = sub.poll(5), sub.poll(5)
    assert (e1.seq, e1.channel, e1.payload) == (s1, "orders", {"id": 1})
    assert e2.payload == {"id": 2} and e1.namespace == "default"
    assert isinstance(e1.created_at, int) and e1.created_at > 0
    assert sub.last_seq == s2


def test_from_seq_none_means_only_new_events(cache):
    cache.notify("c", "before")
    with cache.watch("c") as sub:
        assert sub.poll(0.2) is None
        cache.notify("c", "after")
        assert sub.poll(5).payload == "after"


def test_from_seq_zero_replays_everything_retained(cache):
    seqs = [cache.notify("c", i) for i in range(5)]
    with cache.watch("c", from_seq=0) as sub:
        assert [sub.poll(5).seq for _ in range(5)] == seqs


def test_from_seq_n_resumes_after_n(cache):
    seqs = [cache.notify("c", i) for i in range(5)]
    with cache.watch("c", from_seq=seqs[1]) as sub:
        assert [sub.poll(5).payload for _ in range(3)] == [2, 3, 4]
        assert sub.poll(0.2) is None


def test_try_next_is_non_blocking(cache):
    with cache.watch("c") as sub:
        assert sub.try_next() is None
        cache.notify("c", 1)
        assert wait_for(lambda: not sub._queue.empty())
        assert sub.try_next().payload == 1


def test_blocking_iteration_ends_when_closed_from_another_thread(cache):
    sub = cache.watch("c")
    got = []

    def consume():
        for event in sub:
            got.append(event.payload)

    t = threading.Thread(target=consume)
    t.start()
    cache.notify("c", "x")
    assert wait_for(lambda: got == ["x"])
    sub.close()
    t.join(timeout=5)
    assert not t.is_alive()


def test_close_is_idempotent(cache):
    sub = cache.watch("c")
    sub.close()
    sub.close()
    assert list(sub) == []
    assert sub.poll(0.05) is None


def test_ordering_is_total_across_channels(cache):
    with cache.watch("a", "b") as sub:
        plan = [("a", 1), ("b", 2), ("a", 3), ("b", 4)]
        seqs = [cache.notify(c, p) for c, p in plan]
        got = [sub.poll(5) for _ in plan]
    assert [e.seq for e in got] == seqs
    assert [(e.channel, e.payload) for e in got] == plan


def test_fan_out_every_subscriber_sees_every_event(cache):
    with cache.watch("c") as a, cache.watch("c") as b:
        for i in range(10):
            cache.notify("c", i)
        assert [a.poll(5).payload for _ in range(10)] == list(range(10))
        assert [b.poll(5).payload for _ in range(10)] == list(range(10))


def test_notify_many_one_transaction_in_order(cache):
    with cache.watch("*") as sub:
        seqs = cache.notify_many([("a", 1), ("b", 2), ("c", None)])
        assert seqs == sorted(seqs) and len(set(seqs)) == 3
        assert [(e.channel, e.payload) for e in (sub.poll(5) for _ in seqs)] == [
            ("a", 1),
            ("b", 2),
            ("c", None),
        ]
    assert cache.notify_many([]) == []


def test_namespaces_are_isolated(db_path):
    a = LyteCache(db_path, sweep_interval=None, namespace="a")
    b = LyteCache(db_path, sweep_interval=None, namespace="b")
    try:
        with a.watch("c") as sub:
            b.notify("c", "from-b")
            a.notify("c", "from-a")
            e = sub.poll(5)
            assert (e.payload, e.namespace) == ("from-a", "a")
            assert sub.poll(0.2) is None
    finally:
        a.close()
        b.close()


@pytest.mark.parametrize("bad", ["", "has*star", "__cache__", "__anything", None, 5])
def test_invalid_and_reserved_channels_rejected(cache, bad):
    with pytest.raises(ValueError):
        cache.notify(bad, 1)


def test_watch_argument_validation(cache):
    with pytest.raises(ValueError):
        cache.watch()
    with pytest.raises(ValueError):
        cache.watch("c", from_seq=-1)
    with pytest.raises(ValueError):
        cache.watch("c", poll_interval=0)


# -- channel matching -------------------------------------------------------


def test_exact_and_trailing_glob_matching(cache):
    with cache.watch("cache:invalidate:*", "orders") as sub, cache.watch("*") as everything:
        plan = [
            ("cache:invalidate:user", 1),
            ("cache:invalidate:", 2),  # empty suffix still matches the prefix
            ("cache:invalidatex", 3),  # no
            ("orders", 4),
            ("orders:new", 5),  # exact name: no
            ("cache:other", 6),  # no
        ]
        for c, p in plan:
            cache.notify(c, p)
        got = [sub.poll(5).payload for _ in range(3)]
        assert got == [1, 2, 4]
        assert sub.poll(0.3) is None
        assert [everything.poll(5).payload for _ in plan] == [1, 2, 3, 4, 5, 6]


def test_star_in_the_middle_is_literal(cache):
    with cache.watch("a*b") as sub:
        cache.notify("axb", 1)
        assert sub.poll(0.3) is None


def test_non_matching_watcher_receives_nothing(cache):
    with cache.watch("nobody-publishes-here") as sub:
        for i in range(20):
            cache.notify("busy", i)
        assert sub.poll(0.4) is None


# -- payload encoding ---------------------------------------------------------


def test_payload_round_trip_for_every_portable_type(cache):
    values = [b"\x00raw", "text", 42, -7, 3.25, {"a": [1, None]}, [1, "x"], True, None]
    with cache.watch("t") as sub:
        for v in values:
            cache.notify("t", v)
        got = [sub.poll(5).payload for _ in values]
    assert got == values
    assert [type(g) for g in got] == [type(v) for v in values]


def test_pickle_payload_and_foreign_codes(db_path):
    c = LyteCache(db_path, sweep_interval=None, serializer="pickle")
    try:
        with c.watch("t", from_seq=0) as sub:
            c.notify("t", {1, 2})  # a set is not JSON: pickled (code 5)
            assert sub.poll(5).payload == {1, 2}
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                "INSERT INTO events (channel, namespace, payload, value_type, created_at) "
                "VALUES ('t', 'default', x'aced', 6, 0)"
            )
            conn.commit()
            conn.close()
            e = sub.poll(5)
            with pytest.raises(SerializationError):
                _ = e.payload
    finally:
        c.close()


def test_events_written_by_hand_per_spec_are_readable(cache, db_path):
    """Cross-language fixture: rows built from SPEC.md alone, as another library would."""
    cache.watch("x").close()  # creates the table
    conn = sqlite3.connect(str(db_path))
    rows = [
        ("fx", b"bytes", 0),
        ("fx", b"str", 1),
        ("fx", b"42", 2),
        ("fx", b"1.5", 3),
        ("fx", b'{"k":[1,2]}', 4),
        ("fx", None, 4),  # NULL payload = None
    ]
    conn.executemany(
        "INSERT INTO events (channel, namespace, payload, value_type, created_at) "
        "VALUES (?, 'default', ?, ?, 1700000000000)",
        rows,
    )
    conn.commit()
    conn.close()
    with cache.watch("fx", from_seq=0) as sub:
        got = [sub.poll(5).payload for _ in rows]
    assert got == [b"bytes", "str", 42, 1.5, {"k": [1, 2]}, None]


def test_notify_stores_spec_encoding(cache, db_path):
    cache.notify("c", {"a": 1})
    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT channel, namespace, payload, value_type FROM events").fetchone()
    conn.close()
    assert row == ("c", "default", b'{"a":1}', 4)


# -- durability: cursors, resume, gaps ---------------------------------------


def test_cursor_helpers(cache):
    assert cache.load_cursor("w") is None
    cache.save_cursor("w", 41)
    cache.save_cursor("w", 42)
    assert cache.load_cursor("w") == 42


def test_resume_after_kill_receives_exactly_the_missed_events(db_path, tmp_path):
    cache = LyteCache(db_path, sweep_interval=None)
    cache.watch("jobs").close()  # create the table before the race
    log = tmp_path / "handled.txt"
    log.write_text("")
    ready = tmp_path / "ready"
    proc = multiprocessing.Process(
        target=cursor_subscriber_worker, args=(str(db_path), "worker", str(log), str(ready))
    )
    proc.start()
    try:
        assert wait_for(ready.exists, timeout=30)
        first = cache.notify("jobs", "probe")
        assert wait_for(lambda: cache.load_cursor("worker") == first, timeout=20)
        seqs = [cache.notify("jobs", i) for i in range(40)]
        assert wait_for(lambda: (cache.load_cursor("worker") or 0) >= seqs[10], timeout=20)
    finally:
        proc.kill()
        proc.join(timeout=10)
    saved = cache.load_cursor("worker")
    seqs += [cache.notify("jobs", i) for i in range(40, 60)]  # published while it is down

    with cache.watch("jobs", from_seq=saved) as sub:
        expected = [s for s in seqs if s > saved]
        got = [sub.poll(5).seq for _ in expected]
        assert got == expected
        assert sub.poll(0.3) is None
    handled = [int(x) for x in log.read_text().split()]
    assert handled == sorted(handled) and handled[0] == first
    cache.close()


def test_gap_on_resume_from_evicted_seq(db_path):
    c = LyteCache(db_path, sweep_interval=None, event_retention=0.05)
    try:
        old = [c.notify("c", i) for i in range(10)]
        time.sleep(0.1)
        new = c.notify("c", "fresh")
        c._sweep_once()
        sub = c.watch("c", from_seq=old[3])
        with pytest.raises(GapError) as info:
            sub.poll(5)
        err = info.value
        assert err.requested_seq == old[3] and err.oldest_seq == new
        assert err.resume_seq == new - 1
        assert watch_threads() == []  # the failed subscription is closed
        # Accepting the loss explicitly resumes at the oldest retained event.
        with c.watch("c", from_seq=err.resume_seq) as sub2:
            assert sub2.poll(5).payload == "fresh"
        # from_seq=0 never gaps: it means "everything retained".
        with c.watch("c", from_seq=0) as sub3:
            assert sub3.poll(5).seq == new
    finally:
        c.close()


def test_gap_when_all_events_were_deleted(db_path):
    c = LyteCache(db_path, sweep_interval=None, event_retention=0.05)
    try:
        seqs = [c.notify("c", i) for i in range(3)]
        time.sleep(0.1)
        c._sweep_once()
        with pytest.raises(GapError):
            next(iter(c.watch("c", from_seq=seqs[0])))
        with c.watch("c", from_seq=seqs[-1]) as ok:  # nothing after it was lost
            assert ok.poll(0.2) is None
    finally:
        c.close()


def test_gap_when_cursor_is_ahead_of_the_file(cache):
    cache.notify("c", 1)
    with pytest.raises(GapError) as info:
        cache.watch("c", from_seq=999).poll(1)
    assert info.value.ahead


def test_gap_detected_mid_stream_for_a_subscriber_that_falls_behind(db_path):
    sub_cache = LyteCache(db_path, sweep_interval=None)
    # A separate instance publishes, so nothing wakes the watcher early: it
    # reads once at start, then sleeps a full poll interval.
    pub = LyteCache(db_path, sweep_interval=None, event_max_count=5)
    try:
        pub.notify("c", "start")
        sub = sub_cache.watch("c", poll_interval=1.5)
        time.sleep(0.3)  # its first (empty) poll is done; the next is ~1.5 s away
        for i in range(20):
            pub.notify("c", i)
        pub._sweep_once()  # retention drops all but the newest 5
        with pytest.raises(GapError):
            sub.poll(10)
    finally:
        sub_cache.close()
        pub.close()


def test_count_cap_retention(db_path):
    c = LyteCache(db_path, sweep_interval=None, event_max_count=3)
    try:
        seqs = [c.notify("c", i) for i in range(10)]
        c._sweep_once()
        with c.watch("c", from_seq=0) as sub:
            assert [sub.poll(5).seq for _ in range(3)] == seqs[-3:]
    finally:
        c.close()


def test_retention_by_age_keeps_recent(db_path):
    c = LyteCache(db_path, sweep_interval=None, event_retention=3600)
    try:
        seqs = [c.notify("c", i) for i in range(5)]
        c._sweep_once()
        with c.watch("c", from_seq=0) as sub:
            assert [sub.poll(5).seq for _ in range(5)] == seqs
    finally:
        c.close()


# -- cross-process delivery ----------------------------------------------------


def test_cross_process_delivery_to_two_subscribers_in_seq_order(db_path, tmp_path):
    c = LyteCache(db_path, sweep_interval=None)
    c.watch("x").close()
    c.close()
    n = 200
    subs = []
    for i in range(2):
        out, ready = tmp_path / f"out{i}.json", tmp_path / f"ready{i}"
        p = multiprocessing.Process(
            target=subscriber_worker, args=(str(db_path), "orders*", n, str(out), str(ready))
        )
        p.start()
        subs.append((p, out, ready))
    for _, _, ready in subs:
        assert wait_for(ready.exists, timeout=20)
    plan = []
    for i in range(n):
        plan.append(["orders:new", i])
        plan.append(["unrelated", i])
    pub = multiprocessing.Process(target=publisher_worker, args=(str(db_path), plan))
    pub.start()
    pub.join(timeout=60)
    assert pub.exitcode == 0
    for p, out, _ in subs:
        p.join(timeout=60)
        assert p.exitcode == 0
        got = json.loads(out.read_text())
        assert [e["payload"] for e in got] == list(range(n))
        assert all(e["channel"] == "orders:new" for e in got)
        seqs = [e["seq"] for e in got]
        assert seqs == sorted(seqs)


def test_notify_stream_does_not_lose_cache_writes(db_path):
    c = LyteCache(db_path, sweep_interval=None)
    c.watch("stream").close()
    c.close()
    procs = [multiprocessing.Process(target=notifier_worker, args=(str(db_path), 400))]
    procs += [
        multiprocessing.Process(target=setter_worker, args=(str(db_path), f"p{i}:", 300))
        for i in range(3)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
        assert p.exitcode == 0  # no busy-timeout failures
    c = LyteCache(db_path, sweep_interval=None)
    try:
        for i in range(3):
            assert len(list(c.keys(f"p{i}:*"))) == 300
        with c.watch("stream", from_seq=0) as sub:
            assert [sub.poll(5).payload for _ in range(400)] == list(range(400))
    finally:
        c.close()


# -- opt-in cost, threads, schema compatibility -------------------------------


def test_cache_without_events_creates_no_table_and_no_threads(db_path):
    before = {t.name for t in threading.enumerate()}
    c = LyteCache(db_path, sweep_interval=None)
    for i in range(50):
        c.set(f"k{i}", i)
        c.get(f"k{i}")
    c.delete("k1")
    c.flush()
    c._sweep_once()
    assert not table_exists(db_path)
    assert {t.name for t in threading.enumerate()} == before
    c.close()


def test_no_watch_thread_before_watch_and_none_after_close(cache):
    assert watch_threads() == []
    cache.notify("c", 1)  # publishing alone starts no thread
    assert watch_threads() == []
    a, b = cache.watch("c"), cache.watch("d")
    assert len(watch_threads()) == 2
    a.close()
    assert wait_for(lambda: len(watch_threads()) == 1)
    cache.close()  # closes the remaining subscription too
    assert wait_for(lambda: watch_threads() == [])
    assert b.poll(0.05) is None


def test_v1_file_gains_events_table_on_first_use_and_stays_version_1(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.executescript(DDL)
    conn.execute("INSERT INTO meta (k, v) VALUES ('schema_version', '1')")
    conn.commit()
    conn.close()
    c = LyteCache(db_path, sweep_interval=None)
    c.set("k", "v")
    assert not table_exists(db_path)
    c.notify("c", 1)
    assert table_exists(db_path)
    conn = sqlite3.connect(str(db_path))
    meta = dict(conn.execute("SELECT k, v FROM meta").fetchall())
    conn.close()
    assert meta["schema_version"] == "1" and meta["events_version"] == "1"
    c.close()


# The last commit before events and hashes landed: a genuine "v1-era" library.
V1_ERA_COMMIT = "f55acbf"


def _released_v1_library(tmp_path: Path) -> Path | None:
    """Export the pre-events Python library from git, or None if unavailable."""
    repo = Path(__file__).resolve().parents[2]
    pkg = tmp_path / "v1lib" / "lytecache"
    try:
        files = subprocess.run(
            ["git", "-C", str(repo), "ls-tree", "--name-only", V1_ERA_COMMIT,
             "lytecache-python/src/lytecache/"],
            capture_output=True, text=True, check=True,
        ).stdout.split()
        pkg.mkdir(parents=True)
        for f in files:
            if f.endswith(".py"):
                src = subprocess.run(
                    ["git", "-C", str(repo), "show", f"{V1_ERA_COMMIT}:{f}"],
                    capture_output=True, check=True,
                ).stdout
                (pkg / Path(f).name).write_bytes(src)
    except (OSError, subprocess.CalledProcessError):
        return None
    return pkg.parent if (pkg / "core.py").exists() else None


def test_v1_era_library_still_reads_a_file_with_events(db_path, tmp_path):
    lib = _released_v1_library(tmp_path)
    if lib is None:
        pytest.skip("no pre-events library revision available in git")
    c = LyteCache(db_path, sweep_interval=None)
    c.set("shared", {"x": 1})
    c.notify("c", "an event")
    c.close()
    script = (
        "from lytecache import LyteCache\n"
        f"c = LyteCache({str(db_path)!r}, sweep_interval=None)\n"
        "assert c.get('shared') == {'x': 1}\n"
        "c.set('from_v1', 2)\n"
        "assert c.incr('n') == 1\n"
        "c.delete('shared')\n"
        "c._sweep_once()\n"
        "c.close()\n"
        "print('ok')\n"
    )
    env = {**os.environ, "PYTHONPATH": str(lib)}
    res = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True, cwd=tmp_path
    )
    assert res.returncode == 0, res.stderr
    assert res.stdout.strip() == "ok"
    c = LyteCache(db_path, sweep_interval=None)
    try:
        assert c.get("from_v1") == 2 and c.get("shared") is None
        with c.watch("c", from_seq=0) as sub:
            assert sub.poll(5).payload == "an event"
    finally:
        c.close()


# -- latency ------------------------------------------------------------------


def test_in_process_notify_wakes_watcher_immediately(cache):
    with cache.watch("c", poll_interval=5.0) as sub:
        time.sleep(0.1)
        t = time.perf_counter()
        cache.notify("c", 1)
        assert sub.poll(2) is not None
        assert time.perf_counter() - t < 0.5  # far below the 5 s poll interval


def test_pickle_not_used_by_default(cache):
    with pytest.raises(SerializationError):
        cache.notify("c", {1, 2})
    assert pickle  # imported for the pickle-mode test above
