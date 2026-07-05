from __future__ import annotations

import pytest

from litecache import LiteCache, LiteCacheError


def test_non_strict_get_degrades_to_miss_on_internal_error(db_path):
    cache = LiteCache(db_path, sweep_interval=None, strict=False)
    cache.set("k", "v")
    cache._get_conn().close()  # simulate a broken underlying connection
    assert cache.get("k", "fallback") == "fallback"
    cache.close()


def test_strict_get_raises_on_internal_error(db_path):
    cache = LiteCache(db_path, sweep_interval=None, strict=True)
    cache.set("k", "v")
    cache._get_conn().close()
    with pytest.raises(LiteCacheError):
        cache.get("k")
    cache.close()
