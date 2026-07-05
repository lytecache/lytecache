"""Top-level worker functions for multiprocessing-based concurrency tests.

These must live in a real, importable module (not a local closure) so that
multiprocessing can pickle references to them regardless of start method.
"""

from __future__ import annotations

import time


def incr_worker(path: str, key: str, times: int) -> None:
    from litecache import LiteCache

    cache = LiteCache(path, sweep_interval=None)
    for _ in range(times):
        cache.incr(key)
    cache.close()


def lock_worker(path: str, name: str, hold_time: float, log_path: str) -> None:
    from litecache import LiteCache

    cache = LiteCache(path, sweep_interval=None)
    with cache.lock(name, timeout=15, poll=0.01):
        with open(log_path, "a") as f:
            f.write("enter\n")
        time.sleep(hold_time)
        with open(log_path, "a") as f:
            f.write("exit\n")
    cache.close()


def crash_writer(path: str, key: str) -> None:
    from litecache import LiteCache

    cache = LiteCache(path, sweep_interval=None)
    i = 0
    while True:
        cache.set(key, i)
        i += 1
