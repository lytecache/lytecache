# litecache file format spec (schema version 1)

This document describes the on-disk format of a litecache database file. It
is a public, versioned spec: any tool or library, in any language, that reads
and writes to this schema and follows these semantics should be able to
interoperate with a litecache-created file.

## File

A litecache database is a single SQLite file. It is opened with:

```
PRAGMA busy_timeout=5000;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
```

`busy_timeout` is set before `journal_mode`, so that the WAL switch itself
waits out contention from other connections instead of failing immediately.
WAL mode means the directory alongside the `.db` file will also contain
`-wal` and `-shm` sidecar files during normal operation; these are part of
the database and should not be deleted while the database is in use.

## Tables

```sql
CREATE TABLE IF NOT EXISTS cache (
  key            TEXT    NOT NULL,
  namespace      TEXT    NOT NULL DEFAULT 'default',
  value          BLOB    NOT NULL,
  value_type     INTEGER NOT NULL DEFAULT 0,  -- 0=bytes 1=str 2=int 3=float 4=json
  created_at     INTEGER NOT NULL,            -- unix ms
  expires_at     INTEGER,                     -- unix ms, NULL = no expiry
  last_accessed  INTEGER NOT NULL,            -- unix ms
  access_count   INTEGER NOT NULL DEFAULT 0,
  size_bytes     INTEGER NOT NULL,
  PRIMARY KEY (namespace, key)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cache_lru ON cache(namespace, last_accessed);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
```

`meta` holds a single row `('schema_version', '1')`, written the first time
the database is created. A reader/writer that encounters a
`schema_version` greater than the highest version it supports MUST refuse to
open the file (litecache raises `SchemaVersionError`). Readers may safely
open files with an equal or lower schema version.

### Column semantics

- **key / namespace**: together form the primary key. `namespace` is a
  caller-chosen logical partition within one file (litecache's `namespace=`
  constructor argument); there is no cross-namespace uniqueness constraint.
- **value / value_type**: see "Serialization" below.
- **created_at**: unix ms timestamp of the row's most recent *write*
  (`set`/`add`/`replace`/`get_set`/`set_many` all refresh it — it does not
  track "first written", it tracks "last written").
- **expires_at**: unix ms absolute expiry time, or `NULL` for no expiry. A
  row with `expires_at <= now` is expired.
- **last_accessed**: unix ms timestamp of the most recent successful `get`
  (or equivalent read). Used only for LRU eviction ordering. Because it is
  millisecond-resolution, operations that occur within the same millisecond
  are not distinguishable by this column alone; ties are broken by an
  unspecified but stable order (in practice, primary key order, since `cache`
  is a `WITHOUT ROWID` table clustered on `(namespace, key)`).
- **access_count**: incremented on each read; informational only, not used
  by any eviction policy in this version.
- **size_bytes**: `LENGTH(value)` in bytes, kept in sync on every write, used
  to enforce `max_bytes` without a full table scan.

## TTL semantics

- `ttl=None` means no expiry (`expires_at = NULL`).
- `ttl<=0` (including `0` and negative values) produces an `expires_at` at or
  before the current time, so the key reads as immediately expired. It is
  still written to the table; expiration is judged purely by comparing
  `expires_at` to the current time at read time, and by the background
  sweeper.
- **Lazy expiration**: every read path (`get`, `get_many`, `exists`, `ttl`,
  `keys`, ...) treats a row with `expires_at <= now` as if it were absent,
  without deleting it inline.
- **Active expiration**: a background sweeper (or, when
  `sweep_interval=None`, opportunistic maintenance every ~100 operations)
  periodically `DELETE`s expired rows in bounded batches (500 rows/pass).
- `ttl(key)` returns `None` for a missing *or expired* key, `-1` for a key
  with no expiry, and the remaining seconds (float) otherwise.

## Eviction order

Eviction only runs when `max_keys` and/or `max_bytes` is configured, and is
skipped entirely for `eviction="noeviction"` (which instead raises
`CacheFullError` when a *new* key would grow the namespace past its limit;
updates to existing keys never trigger it). When over capacity, rows are
deleted oldest-target-first according to the policy's `ORDER BY`:

| policy | order |
|---|---|
| `lru` (default) | `last_accessed ASC` |
| `ttl` | `(expires_at IS NULL) ASC, expires_at ASC` (soonest-to-expire first; no-expiry rows evicted last) |
| `random` | `RANDOM()` |
| `noeviction` | never evicts; raises `CacheFullError` instead |

`lfu` (least-frequently-used, using `access_count`) is a documented TODO for
a future schema-compatible release.

## Serialization / value_type codes

| code | type | encoding |
|---|---|---|
| 0 | `bytes` | stored as-is |
| 1 | `str` | UTF-8 encoded |
| 2 | `int` | decimal ASCII text, e.g. `b"42"`, `b"-3"` |
| 3 | `float` | `repr(float)`-style decimal ASCII text, e.g. `b"3.14"` |
| 4 | `json` | UTF-8 encoded `json.dumps(...)` output |

`str`/`int`/`float`/`bytes` round-trip exactly. `bool` is stored as JSON
(code 4, `b"true"`/`b"false"`) since it is a subtype of `int` in Python and
would otherwise lose its boolean identity on read-back. Any other value is
JSON-encoded; values that are not JSON-serializable raise
`SerializationError` rather than falling back to pickle (pickle is
intentionally never used, for security reasons).

Atomic counters (`incr`/`decr`/`incr_float`) operate directly on the decimal
text form via a single SQL `UPSERT`, using `CAST(value AS TEXT)` arithmetic;
this is why int/float values are stored as decimal text rather than SQLite's
native binary integer/real encoding. A counter operation against a row whose
`value_type` is not numeric-compatible (`str`, `bytes`, or `json`) fails the
UPSERT's `WHERE` guard and the caller receives a Python `TypeError` — no
partial write occurs.

## Concurrency

- WAL mode allows one writer and many concurrent readers across processes.
- All read-then-write sequences that must be atomic (`add`, `replace`,
  `get_set`, counters, `set_many`, lock acquire/release) are implemented
  either as a single SQL statement, or as an explicit `BEGIN IMMEDIATE`
  transaction that holds SQLite's write lock for the whole sequence so no
  other connection can interleave a conflicting write.
- Per-process in-memory statistics (`hits`, `misses`, `evictions`,
  `expired_removed`) are exactly that — per-process. They are not
  synchronized across other processes sharing the same file.
