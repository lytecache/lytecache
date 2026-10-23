"""Top-level worker functions for multiprocessing-based concurrency tests.

These must live in a real, importable module (not a local closure) so that
multiprocessing can pickle references to them regardless of start method.
"""

from __future__ import annotations

import time


def incr_worker(path: str, key: str, times: int) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for _ in range(times):
        cache.incr(key)
    cache.close()


def lock_worker(path: str, name: str, hold_time: float, log_path: str) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    with cache.lock(name, timeout=15, poll=0.01):
        with open(log_path, "a") as f:
            f.write("enter\n")
        time.sleep(hold_time)
        with open(log_path, "a") as f:
            f.write("exit\n")
    cache.close()


def crash_writer(path: str, key: str) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    i = 0
    while True:
        cache.set(key, i)
        i += 1


def hset_field_worker(path: str, key: str, field: str, times: int) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for i in range(times):
        cache.hset(key, field, i)
    cache.close()


def hincrby_worker(path: str, key: str, field: str, times: int) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for _ in range(times):
        cache.hincrby(key, field, 1)
    cache.close()


def subscriber_worker(path: str, pattern: str, count: int, out_path: str, ready_path: str) -> None:
    import json
    from pathlib import Path

    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    got = []
    with cache.watch(pattern) as sub:
        Path(ready_path).write_text("ready")
        for event in sub:
            got.append({"seq": event.seq, "channel": event.channel, "payload": event.payload})
            if len(got) == count:
                break
    Path(out_path).write_text(json.dumps(got))
    cache.close()


def publisher_worker(path: str, plan: list) -> None:  # list of [channel, payload]
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for channel, payload in plan:
        cache.notify(channel, payload)
    cache.close()


def cursor_subscriber_worker(path: str, name: str, log_path: str, ready_path: str) -> None:
    """Handles events forever, saving its cursor after each one (killed by the test)."""
    from pathlib import Path

    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    with cache.watch("jobs", from_seq=cache.load_cursor(name)) as sub:
        Path(ready_path).write_text("ready")
        for event in sub:
            with open(log_path, "a") as f:
                f.write(f"{event.seq}\n")
            cache.save_cursor(name, event.seq)


def setter_worker(path: str, prefix: str, n: int) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for i in range(n):
        cache.set(f"{prefix}{i}", i)
    cache.close()


def notifier_worker(path: str, n: int) -> None:
    from lytecache import LyteCache

    cache = LyteCache(path, sweep_interval=None)
    for i in range(n):
        cache.notify("stream", i)
    cache.close()
