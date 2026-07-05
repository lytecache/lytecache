from __future__ import annotations

import pytest

from litecache import LiteCache, SerializationError


def test_set_get_roundtrip(cache):
    cache.set("k", "v")
    assert cache.get("k") == "v"


def test_get_missing_returns_default(cache):
    assert cache.get("nope") is None
    assert cache.get("nope", "fallback") == "fallback"


def test_roundtrip_types(cache):
    cache.set("s", "hello")
    cache.set("i", 42)
    cache.set("f", 3.14)
    cache.set("b", b"raw-bytes")
    cache.set("bool_true", True)
    cache.set("bool_false", False)
    cache.set("obj", {"x": [1, 2, 3], "y": None})

    assert cache.get("s") == "hello"
    assert cache.get("i") == 42 and isinstance(cache.get("i"), int)
    assert cache.get("f") == 3.14 and isinstance(cache.get("f"), float)
    assert cache.get("b") == b"raw-bytes" and isinstance(cache.get("b"), bytes)
    assert cache.get("bool_true") is True
    assert cache.get("bool_false") is False
    assert cache.get("obj") == {"x": [1, 2, 3], "y": None}


def test_delete_returns_count(cache):
    cache.set("a", 1)
    cache.set("b", 2)
    assert cache.delete("a", "b", "missing") == 2
    assert cache.get("a") is None


def test_delete_no_keys(cache):
    assert cache.delete() == 0


def test_exists(cache):
    cache.set("k", "v")
    assert cache.exists("k") is True
    assert cache.exists("nope") is False


def test_add_only_if_absent(cache):
    assert cache.add("k", "v1") is True
    assert cache.add("k", "v2") is False
    assert cache.get("k") == "v1"


def test_replace_only_if_present(cache):
    assert cache.replace("k", "v1") is False
    cache.set("k", "orig")
    assert cache.replace("k", "v2") is True
    assert cache.get("k") == "v2"


def test_get_set_atomic_swap(cache):
    assert cache.get_set("k", "new") is None
    assert cache.get_set("k", "newer") == "new"
    assert cache.get("k") == "newer"


def test_get_set_clears_ttl(cache):
    cache.set("k", "v", ttl=100)
    cache.get_set("k", "v2")
    assert cache.ttl("k") == -1


def test_set_many_get_many(cache):
    cache.set_many({"a": 1, "b": 2, "c": 3})
    result = cache.get_many(["a", "b", "c", "missing"])
    assert result == {"a": 1, "b": 2, "c": 3}


def test_set_many_empty_is_noop(cache):
    cache.set_many({})


def test_get_many_empty(cache):
    assert cache.get_many([]) == {}


def test_serialization_error_for_non_json(cache):
    class Unserializable:
        pass

    with pytest.raises(SerializationError):
        cache.set("k", Unserializable())


def test_keys_glob(cache):
    cache.set("user:1", "a")
    cache.set("user:2", "b")
    cache.set("order:1", "c")
    assert sorted(cache.keys("user:*")) == ["user:1", "user:2"]
    assert sorted(cache.keys("*")) == ["order:1", "user:1", "user:2"]


def test_keys_excludes_expired(cache):
    import time

    cache.set("a", 1, ttl=0.01)
    cache.set("b", 2)
    time.sleep(0.05)
    assert list(cache.keys("*")) == ["b"]


def test_flush_clears_namespace(cache):
    cache.set("a", 1)
    cache.set("b", 2)
    cache.flush()
    assert cache.get("a") is None
    assert list(cache.keys("*")) == []


def test_namespace_isolation_in_shared_file(db_path):
    c1 = LiteCache(db_path, namespace="ns1", sweep_interval=None)
    c2 = LiteCache(db_path, namespace="ns2", sweep_interval=None)
    try:
        c1.set("k", "from-ns1")
        c2.set("k", "from-ns2")
        assert c1.get("k") == "from-ns1"
        assert c2.get("k") == "from-ns2"
        c1.flush()
        assert c1.get("k") is None
        assert c2.get("k") == "from-ns2"
    finally:
        c1.close()
        c2.close()


def test_context_manager(db_path):
    with LiteCache(db_path, sweep_interval=None) as cache:
        cache.set("k", "v")
        assert cache.get("k") == "v"


def test_stats(cache):
    cache.set("k", "v")
    cache.get("k")
    cache.get("missing")
    stats = cache.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["hit_rate"] == 0.5
    assert stats["key_count"] == 1
    assert stats["size_bytes"] > 0
    assert "path" in stats
    assert stats["namespace"] == "default"
