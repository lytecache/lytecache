from __future__ import annotations

import time

import pytest

from litecache import LockTimeout


def test_memoize_caches_return_value(cache):
    calls = []

    @cache.memoize()
    def compute(x):
        calls.append(x)
        return x * 2

    assert compute(5) == 10
    assert compute(5) == 10
    assert calls == [5]


def test_memoize_distinguishes_args(cache):
    @cache.memoize()
    def compute(x):
        return x * 2

    assert compute(1) == 2
    assert compute(2) == 4


def test_memoize_distinguishes_kwargs(cache):
    calls = []

    @cache.memoize()
    def compute(x, y=0):
        calls.append((x, y))
        return x + y

    assert compute(1, y=2) == 3
    assert compute(1, y=3) == 4
    assert compute(1, y=2) == 3
    assert calls == [(1, 2), (1, 3)]


def test_memoize_respects_ttl(cache):
    calls = []

    @cache.memoize(ttl=0.05)
    def compute():
        calls.append(1)
        return "result"

    compute()
    time.sleep(0.15)
    compute()
    assert len(calls) == 2


def test_lock_mutual_exclusion_same_process(cache):
    with cache.lock("resource"):
        assert cache.exists("lock:resource")
        acquired_again = cache.add("lock:resource", "x")
        assert acquired_again is False
    assert cache.exists("lock:resource") is False


def test_lock_timeout_raises(cache):
    with cache.lock("resource", timeout=10), pytest.raises(LockTimeout), cache.lock(
        "resource", timeout=0.1, poll=0.02
    ):
        pass


def test_lock_non_blocking(cache):
    with cache.lock("resource", timeout=10), pytest.raises(LockTimeout), cache.lock(
        "resource", blocking=False
    ):
        pass


def test_lock_released_on_exception(cache):
    with pytest.raises(ValueError), cache.lock("resource"):
        raise ValueError("boom")
    assert cache.exists("lock:resource") is False
