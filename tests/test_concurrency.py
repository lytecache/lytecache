from __future__ import annotations

import multiprocessing
import time

from _workers import crash_writer, incr_worker, lock_worker

from litecache import LiteCache


def test_incr_atomic_across_processes(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    cache.set("counter", 0)
    cache.close()

    n_procs = 4
    per_proc = 250
    procs = [
        multiprocessing.Process(target=incr_worker, args=(str(db_path), "counter", per_proc))
        for _ in range(n_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0

    cache = LiteCache(db_path, sweep_interval=None)
    try:
        assert cache.get("counter") == n_procs * per_proc
    finally:
        cache.close()


def test_lock_mutual_exclusion_across_processes(db_path, tmp_path):
    log_path = tmp_path / "lock_log.txt"
    log_path.write_text("")

    n_procs = 5
    procs = [
        multiprocessing.Process(
            target=lock_worker, args=(str(db_path), "shared", 0.2, str(log_path))
        )
        for _ in range(n_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0

    lines = log_path.read_text().splitlines()
    assert len(lines) == n_procs * 2

    depth = 0
    for line in lines:
        depth += 1 if line == "enter" else -1
        assert depth in (0, 1)
    assert depth == 0


def test_crash_safety_file_usable_after_kill(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    cache.close()

    proc = multiprocessing.Process(target=crash_writer, args=(str(db_path), "k"))
    proc.start()
    time.sleep(0.3)
    proc.kill()
    proc.join(timeout=10)
    assert not proc.is_alive()

    cache = LiteCache(db_path, sweep_interval=None)
    try:
        cache.set("after_crash", "ok")
        assert cache.get("after_crash") == "ok"
    finally:
        cache.close()
