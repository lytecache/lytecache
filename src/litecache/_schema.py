"""The on-disk schema, treated as a public, versioned spec. See SPEC.md."""

from __future__ import annotations

SCHEMA_VERSION = 1

PRAGMAS: tuple[str, ...] = (
    # busy_timeout must be set first so that every subsequent statement on this
    # connection -- including the journal_mode switch itself -- waits out
    # contention from other threads/processes instead of failing immediately.
    "PRAGMA busy_timeout=5000",
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
)

DDL = """
CREATE TABLE IF NOT EXISTS cache (
  key            TEXT    NOT NULL,
  namespace      TEXT    NOT NULL DEFAULT 'default',
  value          BLOB    NOT NULL,
  value_type     INTEGER NOT NULL DEFAULT 0,
  created_at     INTEGER NOT NULL,
  expires_at     INTEGER,
  last_accessed  INTEGER NOT NULL,
  access_count   INTEGER NOT NULL DEFAULT 0,
  size_bytes     INTEGER NOT NULL,
  PRIMARY KEY (namespace, key)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cache_lru ON cache(namespace, last_accessed);

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""

# value_type codes
TYPE_BYTES = 0
TYPE_STR = 1
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_JSON = 4
