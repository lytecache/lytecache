# lytecache (PHP) Storage Specification

This document defines the storage schema and type encoding for the PHP implementation of lytecache. It is the canonical reference for cross-language interop with the Python, Java, Node.js, and Go implementations in this repository, and any other implementation that wants to read/write the same files.

## Schema Version

- **Events table**: a file may also contain an `events` table and an `('events_version', '1')` row in `meta`, created the first time `notify`/`watch` is used. This is "schema version 2" in the root [SPEC.md](../SPEC.md#schema-versions). It is purely additive, so `schema_version` stays `1` and older libraries keep opening the file.

- **Current**: 1
- **Compatibility**: Higher versions are rejected on open with `SchemaVersionException`.

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

Byte-for-byte identical to the Python, Java, Node.js, and Go schemas. `created_at`/`expires_at`/`last_accessed` are all Unix milliseconds.

## Type Codes

| Code | Name | Encoding | Notes |
|------|------|----------|-------|
| 0 | BYTES | Raw bytes | A `Lytecache\Bytes` wrapper around a PHP string, stored as-is |
| 1 | STRING | UTF-8 string | A plain PHP `string` |
| 2 | INT | UTF-8 decimal text | e.g. `"42"`, `"-7"` -- **not** binary; see below |
| 3 | FLOAT | UTF-8 decimal text | e.g. `"3.14"` -- **not** binary; see below |
| 4 | JSON | UTF-8 JSON | bool, array, `null`, objects, enums, `DateTimeInterface` |
| 5 | PYTHON_PICKLE | Python pickle | Python-only escape hatch; reading it throws `SerializationException` |
| 6 | JAVA_SERIALIZED | Java native serialization | Reserved; never written by any implementation; reading it throws `SerializationException` |

### Why a `Bytes` wrapper for binary data

PHP strings are byte arrays with no distinct "binary" type, so `set($key, $rawBytes)` would otherwise be ambiguous between type 0 (raw bytes) and type 1 (UTF-8 text). `Lytecache\Bytes` disambiguates: `$cache->set('blob', new Bytes($rawBytes))` always stores type 0 and `get()` always returns a `Bytes` instance for it; a plain PHP `string` always stores type 1.

### Why INT/FLOAT are text, not binary

`incr`/`decr`/`incrFloat` are implemented as a **single SQL UPSERT** -- never a read-modify-write in PHP -- so that concurrent PHP-FPM worker processes sharing one SQLite file never lose an update:

```sql
value = CAST(CAST(CAST(cache.value AS TEXT) + :amount AS TEXT) AS BLOB)
```

This only works if the stored bytes are already the decimal digits of the number: `CAST(value AS TEXT)` reads them, SQLite coerces the text to a number for the `+`, and the outer `CAST(... AS TEXT)` converts the result back to decimal digits before storing it as a BLOB again. Every implementation in this repository uses this identical SQL, which is what makes a counter written by one language readable and incrementable by another.

### `NaN` and `Infinity`

PHP's `NAN`, `INF`, and `-INF` are rejected at write time (`SerializationException`) rather than silently coerced, for both the `FLOAT` and `JSON` write paths -- `json_encode` would otherwise reject them anyway (`JSON_THROW_ON_ERROR`), but this library rejects them before ever reaching that call.

## Serialization Rules

### PHP to Storage

| PHP Value | Type Code | Encoding |
|---|---|---|
| `Lytecache\Bytes` | 0 | Raw bytes (`->value`) |
| `string` | 1 | UTF-8 bytes, as-is |
| `int` | 2 | UTF-8 decimal text |
| `float` (finite) | 3 | UTF-8 decimal text (shortest round-trip representation) |
| `bool`, `array`, `null`, objects, enums, `DateTimeInterface` | 4 | UTF-8 JSON (`json_encode` with `JSON_UNESCAPED_UNICODE`) |
| `NAN`/`INF`/`-INF`, or anything `json_encode` cannot serialize | -- | `SerializationException` |

Notes on the type-4 (JSON) path:

- `DateTimeInterface` (`DateTime`/`DateTimeImmutable`) serializes to an RFC 3339 / ISO-8601 string, matching the cross-language convention used by Python's `isoformat()`, Java's `Instant`, and Go's `time.Time`.
- A `BackedEnum` serializes to its backing value (`->value`); a plain (non-backed) `UnitEnum` serializes to its case name (`->name`).
- An object implementing `JsonSerializable` is serialized via its `jsonSerialize()` result, exactly as `json_encode` would treat it directly.
- Any other object is serialized via its public properties (`get_object_vars`), recursively applying these same rules to nested values.
- Never PHP's native `serialize()`/`unserialize()` -- only `json_encode`/`json_decode`, so the wire format stays legible to every other language's implementation.

### Storage to PHP

`get($key, ?string $class = null)` decodes according to the stored type code:

- Codes 0-3 (bytes/string/int/float) decode into a `Bytes`, `string`, `int`, or `float` respectively.
- Code 4 (JSON) decodes via `json_decode` into an array/scalar/`null` by default. Passing `class` (`get($key, class: SomeClass::class)`) instead rehydrates a typed instance: constructor named-argument binding if the class has a constructor, else public-property assignment; nested typed properties/parameters are rehydrated recursively (including nested classes, backed enums, and `DateTimeInterface`). A decoded shape that doesn't match the requested class throws `SerializationException`.
- Codes 5 and 6 always throw `SerializationException` naming the code -- this implementation never returns raw bytes for a `value_type` it doesn't understand.

`getMany($keys)` returns a `key => value` array using the same untyped decode as `get()` without a `class` argument; it never throws on serialization mismatches -- an entry that fails to decode in non-strict mode (see below) is simply omitted.

### Strict mode

Selectively rethrowing `SerializationException` on a decode failure (rather than treating it as a miss) is controlled by the `strict` constructor argument: `strict: true` rethrows; the default `strict: false` treats an undecodable value as absent, so that a file written by a newer/different implementation's not-yet-understood encoding doesn't crash an older reader.

## Expiration Semantics

- TTLs are always a `?float` number of seconds (`null` = no expiry), matching PHP's own idiom for the standard library's timeout-style parameters.
- Zero or negative expires the key immediately (the next read is a miss).
- **Lazy expiration**: every read path (`get`, `getMany`, `has`, `ttl`) treats an expired row as absent and deletes it on the spot.
- **Opportunistic maintenance**: PHP has no background threads within a single request, so there is no active sweeper goroutine/thread as in the Go/Java/Node.js implementations. Instead, `maintain()` (bounded batches of 500 expired rows removed per call) runs opportunistically after roughly every 100 operations on a given `LyteCache` instance, throttled by `$sweepInterval` (default 60.0 seconds) acting as a *minimum time between passes*, not a hard on/off switch -- there is no thread to disable, so `sweepInterval: null` simply means "no minimum wait, maintain as often as the operation-count trigger allows." Laravel apps should also register the `lytecache:maintain` Artisan command with the scheduler (e.g. `$schedule->command('lytecache:maintain')->everyMinute()`) so expired keys are reclaimed even on a quiet cache with no traffic to piggyback on.

## Eviction Policies

| Policy | Behavior |
|---|---|
| `Eviction::LRU` (default) | Evict the lowest `last_accessed` first. |
| `Eviction::TTL` | Evict the soonest-to-expire first (rows with no TTL are evicted last). |
| `Eviction::Random` | Evict an arbitrary row. |
| `Eviction::NoEviction` | Reject the write outright (`CacheFullException`) instead of evicting -- checked *before* the write for a **new** key, so a rejected write never has a side effect. Updating an existing key is always allowed, since it never grows the dataset. |

`last_accessed`/`access_count` updates from reads are buffered in memory and flushed in bounded batches rather than written synchronously on every `get()` -- and, since LRU bookkeeping is inherently best-effort, a flush that hits `SQLITE_BUSY` under contention is silently dropped rather than retried or surfaced as an error.

## Concurrency Model

Unlike the Go implementation's separate read/write connection pools, a PHP process is single-threaded per request, so one `PDO` connection per `LyteCache` instance suffices for in-process safety. The realistic deployment this library targets -- many PHP-FPM worker processes sharing one SQLite file -- makes **cross-process** safety the actual concern, addressed the same way as every other implementation: every read-modify-write operation (`incr`/`decr`/`incrFloat`, `add`, `replace`, `getSet`, `setMany`) is a single SQL statement or an explicit `BEGIN IMMEDIATE` transaction -- never a PHP-side read-then-write -- so it stays atomic across processes.

### PRAGMAs

```
PRAGMA busy_timeout = 5000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
```

Applied on every new connection. Every write path that opens its own transaction does so via an explicit `BEGIN IMMEDIATE` (acquiring the write lock upfront) rather than an implicit deferred transaction, wrapped in a small application-level retry loop (jittered backoff, on top of what `busy_timeout` already waits for at the SQLite level) as a safety net against many PHP-FPM workers being released from a wait at the same instant and immediately colliding again.

Multiple processes creating the *same brand-new* WAL-mode file at the exact same moment can still hit `SQLITE_BUSY` on the initial `journal_mode` switch, before `busy_timeout` has had a chance to matter (a well-known SQLite cold-start race, not specific to this implementation) -- the constructor retries schema initialization on that specific error before giving up.

A statement that returns a single row (e.g. the read-back inside `incr`'s atomic UPSERT) always calls `PDOStatement::closeCursor()` once its result has been consumed. A cached, reused `PDOStatement` left with an un-exhausted cursor can otherwise cause a *same-connection* self-conflict against a subsequent `BEGIN IMMEDIATE` on that same connection -- SQLite returns `SQLITE_BUSY` for this immediately, without invoking the `busy_timeout` handler at all (waiting cannot resolve a conflict against your own connection), which looks identical to cross-process lock contention from the exception message alone but is a distinct failure mode with a distinct fix.

## Key Scanning

`keys($pattern = '*')` uses SQLite's native `GLOB` operator (`*`, `?`, `[...]`), not SQL `LIKE`'s `%`/`_` wildcards -- this matches the pattern syntax used by the Python, Java, Node.js, and Go implementations, so `$cache->keys('session:*')` means the same thing in every language. It returns a `Generator`, so it never loads an entire namespace into memory at once.

## Namespace Isolation

The `namespace` column lets multiple independent caches share one file. Every query is scoped to `namespace`; different namespaces never see each other's keys, including for `flush()`.


## Events and Hashes

The normative rules for both features live in the root [SPEC.md](../SPEC.md): the
`events` table and polling query, delivery semantics, the gap rule, change records,
the hash path construction, type guards, TTL and `size_bytes` handling, and the JSON1
fallback. This implementation follows them exactly. The PHP-specific surface:

**Events** (single host only, polled, durable; see [docs/notifications.md](../docs/notifications.md)):
- `notify(string $channel, mixed $payload = null): int` and `notifyMany(array $events): array`, with each event a `[channel, payload]` pair.
- `watch(string|array $channels, ?int $fromSeq = null, float $pollInterval = 0.05): Subscription`. PHP has no threads, so the subscription polls inside the consuming call (`foreach`, `poll(?float $timeout)`, `tryNext()`) on its own `query_only` PDO connection, with the same adaptive backoff. `Event::payload()` decodes lazily.
- `onChange(callable, array $events)` calls listeners synchronously right after the mutating call's COMMIT, never inside the transaction. A counter is the depth guard.
- Constructor options: `publishChanges`, `eventRetention`, `eventMaxCount`, `changeKeyCap`, and `logger` (a `Closure(string)`, default `error_log`).

**Hashes** (atomic field writes over value_type-4 JSON objects; no schema change):
- `hset`, `hsetnx`, `hsetMany`, `hget`, `hmget`, `hgetAll`, `hdel`, `hexists`, `hkeys`, `hvals`, `hlen`, `hincrBy`, `hincrByFloat`. JSON objects decode to arrays, as with `get()`.
- PDO binds untyped values as TEXT, which `json_object()` would store as strings. Integers are therefore bound with `PDO::PARAM_INT`, and the float amount is passed as text and `CAST(... AS REAL)` in SQL.
- The fallback keeps each field's exact JSON text and key order (it does not round-trip through `json_decode`, which would turn `{}` into `[]` and round big integers).

`hashMode()` / `CacheStats::$hashMode` reports `"json1"` or `"fallback"`. `LYTECACHE_FORCE_JSON_FALLBACK=1` forces the fallback.


## Example: Cross-Language Read

A JSON value written by Python:

```python
cache.set("config", {"theme": "dark", "timeout": 30})
```

Stored as `value_type = 4`, `value = b'{"theme":"dark","timeout":30}'`.

Read from PHP:

```php
$config = $cache->get('config'); // ['theme' => 'dark', 'timeout' => 30]
```

A counter incremented from Go:

```go
n, _ := cache.Incr("hits", 5) // stores value_type=2, value="5"
```

Read (and further incremented) from PHP:

```php
$n = (int) $cache->get('hits'); // "5" decoded as the string "5"; cast for int use
$n = $cache->incr('hits', 1);   // 6 -- same atomic UPSERT, same wire format
```
