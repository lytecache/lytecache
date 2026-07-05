from __future__ import annotations

import sqlite3

import pytest

from litecache import LiteCache, SchemaVersionError


def test_persistence_across_close_reopen(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    cache.set("k", "v")
    cache.set("counter", 5)
    cache.close()

    cache2 = LiteCache(db_path, sweep_interval=None)
    try:
        assert cache2.get("k") == "v"
        assert cache2.get("counter") == 5
    finally:
        cache2.close()


def test_vacuum_does_not_lose_data(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    try:
        cache.set("k", "v")
        cache.vacuum()
        assert cache.get("k") == "v"
    finally:
        cache.close()


def test_schema_version_rejected_when_too_new(db_path):
    cache = LiteCache(db_path, sweep_interval=None)
    cache.close()

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE meta SET v = '999' WHERE k = 'schema_version'")
    conn.commit()
    conn.close()

    with pytest.raises(SchemaVersionError):
        LiteCache(db_path, sweep_interval=None)
