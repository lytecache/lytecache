from __future__ import annotations

import pytest

from litecache import LiteCache, _paths


@pytest.fixture(autouse=True)
def _clear_litecache_path_env(monkeypatch):
    monkeypatch.delenv("LITECACHE_PATH", raising=False)


def test_default_path_creates_file_and_parent_dirs(tmp_path, monkeypatch):
    fake_cache_dir = tmp_path / "platform-cache"
    monkeypatch.setattr(_paths, "platform_cache_dir", lambda: fake_cache_dir)
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    monkeypatch.chdir(project_dir)

    assert not fake_cache_dir.exists()
    cache = LiteCache()
    try:
        cache.set("k", "v")
        assert cache.get("k") == "v"
        assert cache.path.exists()
        assert cache.path.parent.name == "litecache"
        assert cache.path.is_relative_to(fake_cache_dir)
    finally:
        cache.close()


def test_default_path_classmethod_matches_instance_path(tmp_path, monkeypatch):
    fake_cache_dir = tmp_path / "platform-cache"
    monkeypatch.setattr(_paths, "platform_cache_dir", lambda: fake_cache_dir)
    resolved = LiteCache.default_path()
    cache = LiteCache()
    try:
        assert cache.path == resolved
    finally:
        cache.close()


def test_two_different_cwds_resolve_to_different_paths(tmp_path, monkeypatch):
    fake_cache_dir = tmp_path / "platform-cache"
    monkeypatch.setattr(_paths, "platform_cache_dir", lambda: fake_cache_dir)

    project_a = tmp_path / "project-a"
    project_b = tmp_path / "project-b"
    project_a.mkdir()
    project_b.mkdir()

    monkeypatch.chdir(project_a)
    path_a = LiteCache.default_path()
    monkeypatch.chdir(project_b)
    path_b = LiteCache.default_path()

    assert path_a != path_b


def test_litecache_path_env_override(tmp_path, monkeypatch):
    override = tmp_path / "custom" / "location.db"
    monkeypatch.setenv("LITECACHE_PATH", str(override))
    cache = LiteCache()
    try:
        assert cache.path == override
        cache.set("k", "v")
        assert override.exists()
    finally:
        cache.close()


def test_explicit_path_still_works(tmp_path):
    explicit = tmp_path / "explicit.db"
    cache = LiteCache(explicit, sweep_interval=None)
    try:
        cache.set("k", "v")
        assert cache.get("k") == "v"
        assert cache.path == explicit
    finally:
        cache.close()
