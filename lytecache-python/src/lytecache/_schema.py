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

# The events table backs notify/watch. It is created lazily, the first time a
# cache uses notify or watch, never at open: a cache that never uses events
# pays nothing. Adding it does NOT bump schema_version (still 1) -- every
# released lytecache refuses a file whose schema_version is newer than its
# own, and v1 libraries must keep opening these files for ordinary cache use.
# The events_version meta row advertises the table instead. See SPEC.md.
EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  channel    TEXT    NOT NULL,
  namespace  TEXT    NOT NULL DEFAULT 'default',
  payload    BLOB,
  value_type INTEGER NOT NULL DEFAULT 4,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_seq_channel ON events(channel, seq);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
INSERT OR IGNORE INTO meta (k, v) VALUES ('events_version', '1');
"""

# value_type codes -- see SPEC.md for the full table. 0-4 are cross-language
# portable; 5 is a Python-only opt-in escape hatch; 6 is reserved for other
# languages' native serializers (lytecache never writes it, only recognizes it
# on read so it can raise a clear error instead of returning raw bytes).
TYPE_BYTES = 0
TYPE_STR = 1
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_JSON = 4
TYPE_PICKLE = 5
TYPE_JAVA = 6
