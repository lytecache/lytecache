from __future__ import annotations

import time

import pytest

from litecache import CacheFullError, LiteCache

# last_accessed is millisecond-resolution (per SPEC.md), so operations that
# must be ordered relative to each other need to land in distinct ms buckets.
_STEP = 0.003


def test_lru_eviction_respects_order(db_path):
    cache = LiteCache(db_path, max_keys=3, eviction="lru", sweep_interval=None)
    try:
        cache.set("a", 1)
        time.sleep(_STEP)
        cache.set("b", 2)
        time.sleep(_STEP)
        cache.set("c", 3)
        time.sleep(_STEP)
        cache.get("a")  # "a" becomes most-recently-used
        cache._flush_lru_buffer()
        time.sleep(_STEP)
        cache.set("d", 4)  # over capacity -> evicts least-recently-used
        remaining = set(cache.keys("*"))
        assert remaining == {"a", "c", "d"}
    finally:
        cache.close()


def test_max_keys_enforced(db_path):
    cache = LiteCache(db_path, max_keys=5, eviction="lru", sweep_interval=None)
    try:
        for i in range(20):
            cache.set(f"k{i}", i)
        assert cache.stats()["key_count"] <= 5
    finally:
        cache.close()


def test_noeviction_raises_when_full(db_path):
    cache = LiteCache(db_path, max_keys=2, eviction="noeviction", sweep_interval=None)
    try:
        cache.set("a", 1)
        cache.set("b", 2)
        with pytest.raises(CacheFullError):
            cache.set("c", 3)
        # updating an existing key must still be allowed
        cache.set("a", 100)
        assert cache.get("a") == 100
        assert cache.get("c") is None
    finally:
        cache.close()


def test_noeviction_add_raises_when_full(db_path):
    cache = LiteCache(db_path, max_keys=1, eviction="noeviction", sweep_interval=None)
    try:
        cache.set("a", 1)
        with pytest.raises(CacheFullError):
            cache.add("b", 2)
    finally:
        cache.close()


def test_ttl_eviction_evicts_soonest_expiry_first(db_path):
    cache = LiteCache(db_path, max_keys=2, eviction="ttl", sweep_interval=None)
    try:
        cache.set("soon", 1, ttl=1000)
        cache.set("later", 2, ttl=9999)
        cache.set("no_ttl", 3)
        remaining = set(cache.keys("*"))
        assert "soon" not in remaining
        assert remaining == {"later", "no_ttl"}
    finally:
        cache.close()


def test_max_bytes_enforced(db_path):
    cache = LiteCache(db_path, max_bytes=400, eviction="lru", sweep_interval=None)
    try:
        for i in range(50):
            cache.set(f"k{i}", "x" * 20)
        assert cache.stats()["size_bytes"] <= 400
    finally:
        cache.close()


def test_invalid_eviction_policy_raises(db_path):
    with pytest.raises(ValueError):
        LiteCache(db_path, eviction="bogus")
