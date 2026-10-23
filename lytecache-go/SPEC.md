# lytecache (Go) Storage Specification

This document defines the storage schema and type encoding for the Go implementation of lytecache. It is the canonical reference for cross-language interop with the Python, Java, and Node.js implementations in this repository, and any other implementation that wants to read/write the same files.

## Schema Version

- **Current**: 1
- **Compatibility**: Higher versions are rejected on open with an error wrapping `ErrSchemaVersion`.

- **Events table**: a file may also contain an `events` table and an `('events_version', '1')` row in `meta`, created the first time `notify`/`watch` is used. This is "schema version 2" in the root [SPEC.md](../SPEC.md#schema-versions). It is purely additive, so `schema_version` stays `1` and older libraries keep opening the file.

## Database Schema

```sql
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

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
```

Byte-for-byte identical to the Python, Java, and Node.js schemas. `created_at`/`expires_at`/`last_accessed` are all Unix milliseconds.

## Type Codes

| Code | Name | Encoding | Notes |
|------|------|----------|-------|
| 0 | BYTES | Raw bytes | A `[]byte`, stored as-is |
| 1 | STR | UTF-8 string | A Go `string` |
| 2 | INT | UTF-8 decimal text | e.g. `"42"`, `"-7"` -- **not** binary; see below |
| 3 | FLOAT | UTF-8 decimal text | e.g. `"3.14"` -- **not** binary; see below |
| 4 | JSON | UTF-8 JSON | Structs, maps, slices, bools, nil, `time.Time` |
| 5 | PYTHON_PICKLE | Python pickle | Python-only escape hatch; reading it wraps `ErrSerialization` |
| 6 | JAVA_SERIALIZED | Java native serialization | Reserved; never written by any implementation; reading it wraps `ErrSerialization` |

### Why INT/FLOAT are text, not binary

`Incr`/`Decr`/`IncrFloat` are implemented as a **single SQL UPSERT** in every language's implementation -- never a read-modify-write in application code -- so that concurrent goroutines or processes sharing one SQLite file never lose an update:

```sql
value = CAST(CAST(CAST(cache.value AS TEXT) + @amount AS TEXT) AS BLOB)
```

This only works if the stored bytes are already the decimal digits of the number: `CAST(value AS TEXT)` reads them, SQLite coerces the text to a number for the `+`, and the outer `CAST(... AS TEXT)` converts the result back to decimal digits before storing it as a BLOB again. Every implementation in this repository uses this identical SQL, which is what makes a counter written by one language readable and incrementable by another.

### Integer precision

Go has distinct sized integer types (`int`, `int8`...`int64`, `uint`...`uint64`), unlike Node.js's single `number`. `encodeValue` accepts any of them and stores the value as decimal text via `int64`. A `uint64` value beyond `math.MaxInt64` is rejected with an error wrapping `ErrSerialization` rather than silently truncated or reinterpreted -- there is no lossless way to represent it in the shared `INT` encoding, which is bounded to signed 64-bit (matching SQLite's own `INTEGER` range and every other implementation's integer type).

Reading an `INT` value decodes into any integer or floating-point pointer destination via reflection (see `assignInt64` in `serialize.go`), matching Go's normal numeric-conversion ergonomics; a value that would overflow the destination's type returns an error wrapping `ErrSerialization`.

### `NaN` and `Infinity`

Go's `math.NaN()`, `math.Inf(1)`, and `math.Inf(-1)` are rejected at write time (an error wrapping `ErrSerialization`) rather than silently coerced -- `encoding/json` would otherwise reject them anyway, but this library rejects them before ever reaching JSON, for both the `FLOAT` and `JSON` write paths. On read, `FLOAT`-coded values accept Python's (`nan`/`inf`) and Java's (`NaN`/`Infinity`) spellings case-insensitively, so a float value written by either language is still readable even though this implementation never writes those spellings itself.

## Serialization Rules

### Go to Storage

| Go Value | Type Code | Encoding |
|---|---|---|
| `[]byte` | 0 | Raw bytes |
| `string` | 1 | UTF-8 bytes |
| `int`, `int8`...`int64`, `uint`...`uint64` (within int64 range) | 2 | UTF-8 decimal text |
| `float32`, `float64` (finite) | 3 | UTF-8 decimal text |
| `bool`, `nil`, struct, map, slice, `time.Time` | 4 | UTF-8 JSON (`encoding/json`, honoring a `MarshalJSON`/`json` tag) |
| A `uint64` beyond `math.MaxInt64`, `NaN`/`Inf`, or anything `encoding/json` cannot marshal (channel, func, cyclic structure) | -- | error wrapping `ErrSerialization` |

Notes:
- `time.Time` serializes via its own `MarshalJSON`, which produces an RFC 3339 string -- matching the cross-language convention used by Python's `isoformat()` and Java's `Instant`/`toString()`.
- Struct fields follow standard `encoding/json` `json:"..."` tags, exactly as `json.Marshal` would treat them directly.
- A struct field whose value is the Go zero value is still included (unlike Python/JS `undefined`-dropping) unless it carries `json:",omitempty"` -- this is ordinary `encoding/json` behavior, not lytecache-specific.

### Storage to Go

`Get(key, dest)` decodes according to the stored type code into `dest`, which must be a non-nil pointer:

- Codes 0-3 (bytes/string/int/float) decode into a matching primitive pointer type, an `*any`, or (for int/float) any other numeric pointer type via reflection.
- Code 4 (JSON) decodes via `json.Unmarshal(data, dest)` -- `dest` can be a pointer to a struct, map, slice, or primitive, exactly as with any other `encoding/json.Unmarshal` call.
- Codes 5 and 6 always return an error wrapping `ErrSerialization` naming the code -- this implementation never returns raw bytes for a `value_type` it doesn't understand.

`GetMany` returns `map[string]RawValue` rather than raw bytes directly, since a `[]byte` alone can't carry the type code a generic decode needs; call `RawValue.Decode(dest)` per key, which is exactly `Get`'s decode path.

## Expiration Semantics

- TTLs are always a `time.Duration`, matching the language's own idiom (contrast Python's float seconds or Node's plain number of seconds).
- Zero or negative expires the key immediately (the next read is a miss).
- **Lazy expiration**: every read path (`Get`, `GetMany`, `Exists`, `TTLOf`) treats an expired row as absent and deletes it on the spot. `Keys` excludes expired rows from its results but does not proactively delete them (they're cleaned up by the next read that touches them, or by the sweeper).
- **Active expiration**: a background goroutine (started by `New` unless `WithSweepInterval(0)`) deletes expired rows in bounded batches (500 rows/pass) every `WithSweepInterval` (default 60s). `WithSweepInterval(0)` disables the goroutine and sweeps opportunistically instead, piggybacked on roughly every 100th operation.

## Eviction Policies

| Policy | Behavior |
|---|---|
| `LRU` (default) | Evict the lowest `last_accessed` first. |
| `TTLPolicy` | Evict the soonest-to-expire first (rows with no TTL are evicted last). |
| `Random` | Evict an arbitrary row. |
| `NoEviction` | Reject the write outright (an error wrapping `ErrCacheFull`) instead of evicting -- checked *before* the write for a **new** key, so a rejected write never has a side effect. Updating an existing key is always allowed, since it never grows the dataset. |

## Concurrency Model

Each `*Cache` opens two `database/sql` connection pools against the same file: a `readDB` (several connections, safe under WAL since readers never block a writer or each other) and a `writeDB` capped at exactly one connection (`SetMaxOpenConns(1)`), which serializes this *process's* writes rather than relying on SQLite to arbitrate concurrent write attempts from the same process. Cross-*process* safety is separate and still mandatory, since multiple processes (or multiple `*Cache` instances in one process) may share one file: every read-modify-write operation (`Incr`/`Decr`/`IncrFloat`, `Add`, `Replace`, `GetSet`) is a single SQL statement or an explicit transaction -- never a Go-side read-then-write -- so it stays atomic across processes, not just within one.

`last_accessed`/`access_count` updates from reads are buffered in memory and flushed in batches (every 200 buffered keys, on sweep, when an LRU eviction check runs, or on `Close`) rather than written synchronously on every `Get`.

### PRAGMAs

```
PRAGMA busy_timeout = 5000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
```

These are applied automatically to every new connection via `modernc.org/sqlite`'s `_pragma=...` DSN query parameters (see `pragmaDSNParams` in `cache.go`), rather than by executing `PRAGMA` statements by hand after each connection is opened.

Multiple processes creating the *same brand-new* WAL-mode file at the exact same moment can still hit `SQLITE_BUSY` on the initial `journal_mode` switch, before `busy_timeout` has had a chance to matter (a well-known SQLite cold-start race, not specific to this implementation) -- `New` retries schema initialization on that specific error before giving up (see `initSchemaWithRetry`).

## Key Scanning

`Keys(pattern)` uses SQLite's native `GLOB` operator (`*`, `?`, `[...]`), not SQL `LIKE`'s `%`/`_` wildcards -- this matches the pattern syntax used by the Python, Java, and Node.js implementations, so `cache.Keys("session:*")` means the same thing in every language. Iteration is cursor-based (keyset pagination, 500 keys per page), so it never loads an entire namespace into memory.

## Namespace Isolation

The `namespace` column lets multiple independent caches share one file. Every query is scoped to `namespace`; different namespaces never see each other's keys, including for `Flush`.


## Events and Hashes

The normative rules for both features live in the root [SPEC.md](../SPEC.md): the
`events` table and polling query, delivery semantics, the gap rule, change records,
the hash path construction, type guards, TTL and `size_bytes` handling, and the JSON1
fallback. This implementation follows them exactly. The Go-specific surface:

**Events** (single host only, polled, durable; see [docs/notifications.md](../docs/notifications.md)):
- `Notify(channel, payload) (seq, error)` and `NotifyMany([]Notification)`: the payload is encoded by the same rules as `Set`; a nil payload is stored as JSON `null`.
- `Watch(patterns, FromSeq(n), PollInterval(d)) (*Subscription, error)`: one goroutine and one `query_only` connection per subscription. `Events()` is a channel closed on `Close` or on a gap; `Err()` returns the `*GapError`; `Next(ctx)` and `TryNext()` are the pull forms.
- `SaveCursor` / `LoadCursor` use the `meta` key `cursor:<namespace>:<name>`.
- `OnChange(fn, ops...)`: one dispatch goroutine, started with the first listener. The depth guard recognizes the dispatch goroutine by its id, since Go has no goroutine-locals. A panicking listener is recovered and logged.
- `EventStats()` and `RecentEvents(limit, afterSeq)` are cross-namespace, read-only admin helpers, used by the CLI's `events` command and the UI's Events tab.

**Hashes** (atomic field writes over value_type-4 JSON objects; no schema change):
- `HSet`, `HSetNX`, `HSetMany`, `HGet(key, field, dest)`, `HGetRaw`, `HMGet`, `HGetAll` (`map[string]json.RawMessage`), `HDel`, `HExists`, `HKeys`, `HVals`, `HLen`, `HIncrBy`, `HIncrByFloat`.
- `[]byte` field values are rejected rather than silently stored as base64.
- The fallback keeps each field's exact JSON text and key order (it does not round-trip through a Go map), so it writes the same bytes as the JSON1 path.

`HashMode()` / `Stats().HashMode` reports `"json1"` or `"fallback"`. `LYTECACHE_FORCE_JSON_FALLBACK=1` forces the fallback.


## Example: Cross-Language Read

A JSON value written by Python:

```python
cache.set("config", {"theme": "dark", "timeout": 30})
```

Stored as `value_type = 4`, `value = b'{"theme":"dark","timeout":30}'`.

Read from Go:

```go
var config struct {
    Theme   string `json:"theme"`
    Timeout int    `json:"timeout"`
}
_, err := cache.Get("config", &config)
```

A counter incremented from Node.js:

```ts
cache.incr("hits", 5); // stores value_type=2, value="5"
```

Read (and further incremented) from Go:

```go
n, _ := cache.GetInt64("hits") // 5
n, _ = cache.Incr("hits", 1)   // 6 -- same atomic UPSERT, same wire format
```
