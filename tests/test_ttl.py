from __future__ import annotations

import time

from litecache import LiteCache


def test_ttl_none_means_no_expiry(cache):
    cache.set("k", "v")
    assert cache.ttl("k") == -1


def test_ttl_missing_key_is_none(cache):
    assert cache.ttl("nope") is None


def test_ttl_positive_float(cache):
    cache.set("k", "v", ttl=0.5)
    remaining = cache.ttl("k")
    assert remaining is not None
    assert 0 < remaining <= 0.5


def test_ttl_zero_expires_immediately(cache):
    cache.set("k", "v", ttl=0)
    assert cache.get("k") is None
    assert cache.exists("k") is False


def test_ttl_negative_expires_immediately(cache):
    cache.set("k", "v", ttl=-5)
    assert cache.get("k") is None


def test_expiry_boundary(cache):
    cache.set("k", "v", ttl=0.05)
    assert cache.get("k") == "v"
    time.sleep(0.08)
    assert cache.get("k") is None


def test_lazy_expiration_on_read(cache):
    cache.set("k", "v", ttl=0.02)
    time.sleep(0.05)
    assert cache.get("k") is None
    assert cache.exists("k") is False


def test_expire_sets_ttl(cache):
    cache.set("k", "v")
    assert cache.expire("k", 10) is True
    ttl = cache.ttl("k")
    assert ttl is not None and 0 < ttl <= 10


def test_expire_missing_key_returns_false(cache):
    assert cache.expire("nope", 10) is False


def test_expire_on_already_expired_key_returns_false(cache):
    cache.set("k", "v", ttl=0.01)
    time.sleep(0.05)
    assert cache.expire("k", 10) is False


def test_persist_removes_ttl(cache):
    cache.set("k", "v", ttl=10)
    assert cache.persist("k") is True
    assert cache.ttl("k") == -1


def test_persist_on_key_without_ttl_returns_false(cache):
    cache.set("k", "v")
    assert cache.persist("k") is False


def test_persist_missing_key_returns_false(cache):
    assert cache.persist("nope") is False


def test_touch_refreshes_ttl(cache):
    cache.set("k", "v", ttl=1)
    time.sleep(0.3)
    assert cache.touch("k", 10) is True
    ttl = cache.ttl("k")
    assert ttl is not None and ttl > 5


def test_sweeper_removes_expired_rows(db_path):
    cache = LiteCache(db_path, sweep_interval=0.1)
    try:
        cache.set("k", "v", ttl=0.05)
        time.sleep(0.5)
        stats = cache.stats()
        assert stats["key_count"] == 0
        assert stats["expired_removed"] >= 1
    finally:
        cache.close()


def test_opportunistic_maintenance_without_sweeper(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    try:
        cache.set("k", "v", ttl=0.01)
        time.sleep(0.05)
        for i in range(150):
            cache.set(f"filler:{i}", i)
        stats = cache.stats()
        assert stats["expired_removed"] >= 1
    finally:
        cache.close()
