# lytecache (Node.js) Storage Specification

This document defines the storage schema and type encoding for the Node.js implementation of lytecache. It is the canonical reference for cross-language interop with the Python and Java implementations in this repository, and any other implementation that wants to read/write the same files.

## Schema Version

- **Events table**: a file may also contain an `events` table and an `('events_version', '1')` row in `meta`, created the first time `notify`/`watch` is used. This is "schema version 2" in the root [SPEC.md](../SPEC.md#schema-versions). It is purely additive, so `schema_version` stays `1` and older libraries keep opening the file.

- **Current**: 1
- **Compatibility**: Higher versions are rejected on open with `SchemaVersionError`.

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

Byte-for-byte identical to the Python and Java schemas. `created_at`/`expires_at`/`last_accessed` are all Unix milliseconds.

## Type Codes

| Code | Name | Encoding | Notes |
|------|------|----------|-------|
| 0 | BYTES | Raw bytes | A `Buffer`/`Uint8Array`, stored as-is |
| 1 | STR | UTF-8 string | |
| 2 | INT | UTF-8 decimal text | e.g. `b"42"`, `b"-7"` -- **not** binary; see below |
| 3 | FLOAT | UTF-8 decimal text | e.g. `b"3.14"` -- **not** binary; see below |
| 4 | JSON | UTF-8 JSON | Objects, arrays, booleans, `null`, class instances |
| 5 | PYTHON_PICKLE | Python pickle | Python-only escape hatch; reading it throws `SerializationError` |
| 6 | JAVA_SERIALIZED | Java native serialization | Reserved; never written by any implementation; reading it throws `SerializationError` |

### Why INT/FLOAT are text, not binary

`incr`/`decr`/`incrFloat` are implemented as a **single SQL UPSERT** in every language's implementation -- never a read-modify-write in application code -- so that concurrent processes sharing one SQLite file never lose an update:

```sql
value = CAST(CAST(CAST(cache.value AS TEXT) + :amount AS TEXT) AS BLOB)
```

This only works if the stored bytes are already the decimal digits of the number: `CAST(value AS TEXT)` reads them, SQLite coerces the text to a number for the `+`, and the outer `CAST(... AS TEXT)` converts the result back to decimal digits before storing it as a BLOB again. Every implementation in this repository uses this identical SQL, which is what makes a counter written by one language readable and incrementable by another.

### A Node-specific wrinkle: one numeric type

Unlike Python (`int`/`float`) or Java (`long`/`double`), JavaScript has a single `number` type -- `3` and `3.0` are the same value, and `Number.isInteger(3.0) === true`. This library therefore decides the type code by **shape, not intent**: any integer-valued `number` (or `bigint` within signed-64-bit range) is stored as type 2; any non-integer `number` is stored as type 3. There is no way to force a whole number to store as a float from Node -- this is an inherent consequence of the language, not a bug, and is worth knowing if you're comparing byte-for-byte output against the Python or Java implementations for a whole-number float.

`NaN` and `Infinity` are rejected at write time (`SerializationError`) rather than silently coerced (`JSON.stringify(NaN)` would otherwise produce `"null"`, silently losing the value). On read, this implementation accepts Python's (`nan`/`inf`) and Java's (`NaN`/`Infinity`) spellings case-insensitively, so a float value written by either language is still readable even though this implementation never writes those spellings itself.

### Integer precision: `number` vs `bigint`

- Writing: an integer-valued `number` within `[Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]` is accepted directly. A `number` *outside* that range is rejected with `SerializationError` -- it may have already lost precision as a `number`, so silently storing it would be dishonest. Pass a `bigint` instead to store an exact value beyond the safe-integer range (up to signed 64-bit: `-2^63` to `2^63 - 1`, matching SQLite's own integer range).
- Reading: an INT value that fits in `Number.MAX_SAFE_INTEGER` is returned as a `number`; one that doesn't is returned as a `bigint`. `incr`/`decr` follow the same rule for their return value.

## Serialization Rules

### Node.js to Storage

| Node Value | Type Code | Encoding |
|---|---|---|
| `Buffer` / `Uint8Array` | 0 | Raw bytes |
| `string` | 1 | UTF-8 bytes |
| Integer-valued `number` (within safe range) or `bigint` (within int64 range) | 2 | UTF-8 decimal text |
| Non-integer, finite `number` | 3 | UTF-8 decimal text (`Number.prototype.toString()`) |
| `boolean`, `null`, plain object, array, `Date`, class instance | 4 | UTF-8 JSON (`JSON.stringify`, honoring `toJSON()`) |
| `undefined`, `function`, `symbol`, `Map`, `Set`, circular reference, `NaN`/`Infinity` | -- | `SerializationError` |

Notes:
- `Date` serializes as an ISO-8601 string (via its own `toJSON()`), whether it's the top-level value or nested inside an object/array.
- `undefined` *properties inside an object* are dropped, matching plain `JSON.stringify` behavior (e.g. `{a: 1, b: undefined}` stores as `{"a":1}`). A **top-level** `undefined` value passed to `set()` is rejected outright, since there's no way to tell "store nothing meaningful" apart from a mistake.
- `Map` and `Set` are rejected rather than silently serialized as `{}` (`JSON.stringify`'s default, data-losing behavior for these types) -- convert them first: `Object.fromEntries(map)` or `Array.from(set)`.
- A class instance serializes via its own `toJSON()` if defined, else its enumerable own properties -- identical to `JSON.stringify`'s normal behavior for any object.

### Storage to Node.js

`get(key)` returns native values for codes 0-3 directly; type-code-4 (JSON) values return a plain object/array/primitive by default. Two options refine JSON reads:

- `get(key, default, { reviver })` -- passed straight through as `JSON.parse`'s second argument.
- `get(key, default, { into: SomeClass })` -- rehydrates a stored JSON *object* (not array/primitive) as `Object.assign(Object.create(SomeClass.prototype), data)`, so instance methods work on the result. Throws `SerializationError` if the stored value isn't a JSON object.

Reading type code 5 or 6 always throws `SerializationError` naming the code -- this implementation never returns raw bytes for a value_type it doesn't understand.

## Expiration Semantics

- `ttl` is always **seconds** (a `number`, fractional allowed), matching the Python implementation; matching Java's `Duration`-based API in spirit.
- `ttl: 0` or a negative `ttl` expires the key immediately (the next read is a miss).
- **Lazy expiration**: every read path (`get`, `getMany`, `exists`, `keys`) treats an expired row as absent and deletes it on the spot.
- **Active expiration**: a background sweeper (`setInterval(..., sweepInterval * 1000).unref()`, so it never keeps the process alive on its own) deletes expired rows in bounded batches (500 rows/pass) every `sweepInterval` seconds (default 60). `sweepInterval: null` disables the timer and sweeps opportunistically every ~100 operations instead.

## Eviction Policies

| Policy | Behavior |
|---|---|
| `lru` (default) | Evict the lowest `last_accessed` first. |
| `ttl` | Evict the soonest-to-expire first (rows with no TTL are evicted last). |
| `random` | Evict arbitrary rows. |
| `noeviction` | Reject the write outright (`CacheFullError`) instead of evicting -- checked *before* the write for a **new** key, so a rejected write never has a side effect. Updating an existing key is always allowed, since it never grows the dataset. |

## Pragmas

```sql
PRAGMA busy_timeout = 5000;  -- set first, so it applies even while switching journal_mode itself
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
```

Multiple processes creating the *same brand-new* WAL-mode file at the exact same moment can still hit `SQLITE_BUSY` on the initial `journal_mode` switch, before `busy_timeout` has had a chance to matter (a well-known SQLite cold-start race, not specific to this implementation) -- the constructor retries this specific step on `SQLITE_BUSY` before giving up.

## Concurrency Model

better-sqlite3 is synchronous: there is one connection per `LyteCache` instance, and every call blocks until it completes. This makes in-process concurrency trivial (no interleaving between two calls on the same instance), but cross-*process* safety is still mandatory, since multiple processes (e.g. a PM2 cluster, or worker processes) may share one file. Every read-modify-write operation (`incr`/`decr`/`incrFloat`, `add`, `replace`, `getSet`) is a single SQL statement or an explicit `BEGIN IMMEDIATE` transaction -- never a JS-side read-then-write -- so it stays atomic across processes, not just within one.

`last_accessed`/`access_count` updates from reads are buffered in memory and flushed in batches (every 200 buffered updates, on sweep, or on `close()`) rather than written synchronously on every `get()`.

## Namespace Isolation

The `namespace` column lets multiple independent caches share one file. Every query is scoped to `namespace`; different namespaces never see each other's keys, including for `flush()`.


## Events and Hashes

The normative rules for both features live in the root [SPEC.md](../SPEC.md): the
`events` table and polling query, delivery semantics, the gap rule, change records,
the hash path construction, type guards, TTL and `size_bytes` handling, and the JSON1
fallback. This implementation follows them exactly. The Node.js-specific surface:

**Events** (single host only, polled, durable; see [docs/notifications.md](../docs/notifications.md)):
- `notify(channel, payload = null)` and `notifyMany([[channel, payload], ...])`: the payload is encoded by the same rules as `set`.
- `watch(channels, { fromSeq, pollInterval })`: one timer and one read-only better-sqlite3 connection per subscription. The pull forms are the async iterator, `next(timeoutMs)` and `tryNext()`. The push form is `.on("event")`: while an `"event"` listener is attached, events are emitted instead of queued, and a gap is emitted as `"error"`. An open subscription's timer keeps the process alive until `close()`.
- `onChange(listener, { events })` delivers from a `setImmediate` callback. The depth guard sees only synchronous re-entry: a listener's writes after an `await` start again at depth 1.

**Hashes** (atomic field writes over value_type-4 JSON objects; no schema change):
- `hset`, `hsetnx`, `hsetMany`, `hget`, `hmget`, `hgetAll`, `hdel`, `hexists`, `hkeys`, `hvals`, `hlen`, `hincrby` (`number | bigint`), `hincrbyfloat`.
- `hincrby` binds its amount as a `bigint`, because better-sqlite3 binds JS numbers as REAL. The result is read back as decimal text, so int64 values stay exact.
- Buffers are rejected as field values. The fallback keeps each field's exact JSON text and key order (it does not round-trip through `JSON.parse`).

`cache.hashMode` / `stats().hashMode` reports `"json1"` or `"fallback"`. `LYTECACHE_FORCE_JSON_FALLBACK=1` forces the fallback.


## Example: Cross-Language Read

A JSON value written by Python:

```python
cache.set("config", {"theme": "dark", "timeout": 30})
```

Stored as `value_type = 4`, `value = b'{"theme":"dark","timeout":30}'`.

Read from Node.js:

```ts
const config = cache.get<{ theme: string; timeout: number }>("config");
// { theme: "dark", timeout: 30 }
```

A counter incremented from Java:

```java
cache.incr("hits", 5); // stores value_type=2, value=b"5"
```

Read (and further incremented) from Node.js:

```ts
cache.get("hits"); // 5
cache.incr("hits"); // 6 -- same atomic UPSERT, same wire format
```
