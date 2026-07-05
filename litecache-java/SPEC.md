# LiteCache Storage Specification

This document defines the storage schema and type encoding for LiteCache. It is the canonical reference for cross-language implementations.

## Schema Version

- **Current**: 1
- **Compatibility**: Higher versions will be rejected on open with `SchemaVersionException`.

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

### Column Definitions

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `key` | TEXT | NO | Cache key within the namespace |
| `namespace` | TEXT | NO | Namespace for isolating cache instances |
| `value` | BLOB | NO | Serialized value (see Type Codes) |
| `value_type` | INTEGER | NO | Type code (0–6) |
| `created_at` | INTEGER | NO | Unix milliseconds when inserted |
| `expires_at` | INTEGER | YES | Unix milliseconds when value expires; NULL = no expiry |
| `last_accessed` | INTEGER | NO | Unix milliseconds of last read |
| `access_count` | INTEGER | NO | Number of times accessed (cache statistics) |
| `size_bytes` | INTEGER | NO | Serialized size of the value in bytes |

## Type Codes

All values are tagged with a type code that describes their encoding:

| Code | Name | Encoding | Notes |
|------|------|----------|-------|
| 0 | BYTES | Raw bytes | Stored as-is; no interpretation |
| 1 | STRING | UTF-8 string | Bytes interpreted as UTF-8 text |
| 2 | INT64 | UTF-8 decimal text | e.g. `b"42"`, `b"-7"` -- **not** binary; see below |
| 3 | FLOAT64 | UTF-8 decimal text | e.g. `b"3.14"`, `b"-1.0"` -- **not** binary; see below |
| 4 | JSON | UTF-8 JSON | Bytes interpreted as UTF-8 JSON text; can be any valid JSON (object, array, string, number, bool, null) |
| 5 | PYTHON_PICKLE | Python pickle | Reserved; reading throws `SerializationException` |
| 6 | JAVA_SERIALIZED | Java native | Reserved; reading throws `SerializationException` |

### Type Code Rationale

- **0–3**: Primitive and native types, stored in a language-neutral format.
- **4**: JSON is the lingua franca for cross-language interop. All complex objects (POJOs, records, maps, lists, etc.) are stored as JSON.
- **5–6**: Reserved for language-specific serialization formats. These MUST NOT be used; they are rejected on read to prevent security issues and ensure portability.

### Why INT64/FLOAT64 are text, not binary

Codes 2 and 3 store the decimal string representation of the number (UTF-8 bytes of e.g. `"42"`), not a fixed-width binary encoding. This is deliberate, and load-bearing for `incr`/`decr`/`incrDouble`:

Both the Python and Java implementations perform atomic counter increments as a **single SQL UPSERT** -- never a Java/Python-side read-modify-write -- so that concurrent processes sharing one SQLite file never lose an update. SQLite has no built-in way to reinterpret an arbitrary binary blob as an integer for arithmetic, but it *does* do numeric coercion on TEXT values. Storing the counter as text lets the UPSERT do the arithmetic entirely in SQL:

```sql
value = CAST(CAST(CAST(cache.value AS TEXT) + :amount AS TEXT) AS BLOB)
```

`CAST(value AS TEXT)` reads the stored digits, `+ :amount` lets SQLite coerce and add them, and the outer `CAST(... AS TEXT)` converts the result back to decimal text before it's stored as a BLOB again. Both implementations use this identical pattern, which is what makes a counter written by one language readable and incrementable by the other.

## Serialization Rules

### Java to Storage

| Java Type | Type Code | Encoding |
|-----------|-----------|----------|
| `null` | 1 (STRING) | Empty bytes |
| `String` | 1 | UTF-8 bytes |
| `byte[]` | 0 | Raw bytes |
| `Long`, `Integer` | 2 | UTF-8 decimal text (e.g. `Long.toString(value)`) |
| `Double`, `Float` | 3 | UTF-8 decimal text (e.g. `Double.toString(value)`) |
| Everything else | 4 | UTF-8 JSON (via Jackson) |

### Storage to Java

Reading with `cache.get(key, Type.class)`:

| Type Code | Target Class | Result |
|-----------|--------------|--------|
| 0 (BYTES) | `byte[]` | Bytes as-is |
| 0 (BYTES) | `String` | UTF-8 decode |
| 1 (STRING) | `String` | UTF-8 decode |
| 1 (STRING) | `byte[]` | UTF-8 encode |
| 2 (INT64) | `Long` | Parse decimal text as long |
| 2 (INT64) | `Integer` | Parse decimal text as long, cast to int |
| 3 (FLOAT64) | `Double` | Parse decimal text as double |
| 3 (FLOAT64) | `Float` | Parse decimal text as double, cast to float |
| 4 (JSON) | Any class | Jackson deserialize |
| 4 (JSON) | `Map.class` | Jackson deserialize to Map<String, Object> |
| 4 (JSON) | `List.class` | Jackson deserialize to List<Object> |
| 5 | Any | Throw `SerializationException` |
| 6 | Any | Throw `SerializationException` |

For a fully-parameterized generic type that a raw `Class` can't express (`Map<String, MyRecord>`, `List<MyRecord>`, etc.), use `cache.get(key, new TypeReference<Map<String, MyRecord>>() {})` instead. This overload only supports type code 4 (JSON); other type codes throw `SerializationException`.

**Type mismatch** (e.g., reading type code 1 as `Long`) throws `SerializationException`.

## JSON Conventions

When Java values are serialized to JSON (type code 4), the following Jackson conventions apply:

### Date/Time (java.time)

Registered via `jackson-datatype-jsr310` with timestamps disabled:
- `java.time.LocalDateTime`: ISO-8601 string (e.g., `"2024-01-15T10:30:00"`)
- `java.time.ZonedDateTime`: ISO-8601 string with zone (e.g., `"2024-01-15T10:30:00Z"`)
- `java.time.LocalDate`: ISO-8601 date (e.g., `"2024-01-15"`)
- `java.time.Duration`: ISO-8601 duration (e.g., `"PT5M"`)

### Numeric Types

- `BigDecimal`: String (to preserve precision across languages)
- `BigInteger`: String or number depending on magnitude

### Collections

- `Set`: JSON array (order not guaranteed on round-trip)
- `List`: JSON array
- `Map`: JSON object

### Custom Objects

- Records and POJOs with public fields or getters: Jackson's default ObjectMapper behavior
- No-arg constructor required for deserialization into POJOs
- Nested objects supported

## Expiration Semantics

### Lazy Expiration

On every `get()`:
1. Fetch the value from the cache.
2. Check `expires_at` against the current timestamp (in milliseconds).
3. If `expires_at IS NOT NULL AND expires_at <= now`, treat as a cache miss (don't return the value).
4. Delete the expired row from the database (best-effort, logs warnings if strict=false).

### Active Expiration

A background sweeper thread (enabled by default, configurable via `sweepInterval(Duration)`) runs periodically:
1. Deletes rows where `expires_at <= now` in batches of up to 500 rows per run.
2. Enforces `maxKeys` and `maxBytes` capacity limits with the configured eviction policy.
3. Flushes buffered LRU metadata.

If `sweepInterval` is set to `null`, the sweeper is disabled and cleanup is opportunistic (triggered every N reads or on explicit `close()`).

## Eviction Policies

When capacity limits (`maxKeys` or `maxBytes`) are exceeded and a new entry is added:

| Policy | Behavior |
|--------|----------|
| **LRU** | Evict entries with the lowest `last_accessed` timestamp. |
| **TTL** | Evict entries with the soonest `expires_at` (expired entries first, then those with soonest expiry). NULL `expires_at` are kept longest. |
| **RANDOM** | Randomly select entries to evict. |
| **NOEVICTION** | Throw `CacheFullException` instead of evicting. |

Eviction is implemented in a single SQL query to ensure atomicity.

## Namespace Isolation

The `namespace` column allows multiple independent cache instances to coexist in the same SQLite file. Queries are always scoped to the namespace:

```sql
SELECT * FROM cache WHERE namespace = ? AND key = ?
```

Different namespaces do not interfere with each other.

## Pragmas

On every connection, the following pragmas are set for correctness and performance:

```sql
PRAGMA journal_mode=WAL;        -- Write-Ahead Logging for concurrent reads/writes
PRAGMA synchronous=NORMAL;      -- Balanced durability and performance
PRAGMA busy_timeout=5000;       -- 5-second lock wait timeout
```

## Concurrency Model

- **Writes**: Serialized via a single writer connection (Java-side lock or SQLite's IMMEDIATE transaction isolation).
- **Reads**: Multiple concurrent read connections allowed; each thread gets its own connection.
- **WAL mode**: Enables readers to proceed while writes are in progress.

## Statistics Metadata

The `meta` table stores deployment metadata:

| Key | Value | Notes |
|-----|-------|-------|
| `schema_version` | `1` (string) | Version of the schema; higher versions are incompatible |

Future extensions may add additional metadata rows.

## Example: Cross-Language Read

### Written by Python

```python
import json
cache.set("config", {"theme": "dark", "timeout": 30}, ttl=None)
```

Stored in SQLite as:
- `key`: `"config"`
- `value`: `b'{"theme": "dark", "timeout": 30}'` (UTF-8 bytes)
- `value_type`: `4` (JSON)

### Read by Java

```java
Map<String, Object> config = cache.get("config", Map.class);
// config = {"theme": "dark", "timeout": 30}
```

Or as a JsonNode:
```java
JsonNode node = cache.get("config", JsonNode.class);
String theme = node.get("theme").asText(); // "dark"
int timeout = node.get("timeout").asInt(); // 30
```

## Data Integrity

- **No corruption on concurrent access**: WAL mode + separate connections ensure readers don't interfere with writers.
- **No partial writes**: SQLite's atomicity guarantees ensure each cache entry is written entirely or not at all.
- **No silent data loss on type mismatch**: Type mismatches throw `SerializationException` rather than silently converting or returning garbage.

## Future Considerations

- LFU (Least Frequently Used) eviction may be added as a policy type 4 in a future revision.
- Compression of `value` for large entries could be added as a type code variant (e.g., type codes 10–15 for compressed variants).
- Distributed snapshots for backup/replication are out of scope.

---

All implementations must adhere to this spec to ensure interoperability.
