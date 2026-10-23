# LyteCache for Java

**Redis-like caching for Java with zero infrastructure — no server, just a local SQLite file.**

LyteCache is an embedded, production-grade caching library for Java applications. It provides a familiar Redis-like API (set/get, TTLs, atomic counters, eviction) backed by a portable SQLite database. Perfect for single-node applications that need persistent, distributed-lock-safe caching without the operational overhead of a dedicated Redis server.

## Features

- **Zero Configuration**: `new LyteCache()` works immediately; automatic database creation and schema initialization.
- **Portable Persistence**: Values stored as portable JSON, readable by LyteCache implementations in other languages.
- **Thread-Safe**: WAL mode + connection pooling for safe concurrent access.
- **Redis-Like API**: `set(key, value, ttl)`, `get(key, type)`, `incr(counter)`, `memoize()`, and more.
- **TTL & Expiration**: Lazy deletion on read + background sweeper for active cleanup.
- **Eviction Policies**: LRU (default), TTL, RANDOM, or NOEVICTION.
- **Atomic Counters**: Multi-threaded safe `incr`, `decr`, `incrDouble` via single SQL UPSERT.
- **Distributed Locks**: Process-safe locking built on cache primitives.
- **Type-Safe Serialization**: Jackson integration for POJOs, records, and cross-language compatibility.

## Installation

### Gradle

```gradle
implementation 'io.github.lytecache:lytecache:0.2.0'
```

### Maven

```xml
<dependency>
    <groupId>io.github.lytecache</groupId>
    <artifactId>lytecache</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Quick Start

```java
import io.lytecache.LyteCache;
import java.time.Duration;

try (LyteCache cache = new LyteCache()) {
    // Set a value with 5-minute expiry
    cache.set("user:42", "Alice", Duration.ofMinutes(5));
    
    // Get the value
    String user = cache.getString("user:42");
    System.out.println(user); // Alice
    
    // Atomic counter
    long visits = cache.incr("visits");
    
    // Complex objects (serialized as JSON)
    cache.set("config", Map.of("theme", "dark", "lang", "en"));
}
// Cache automatically closes, flushing any pending state
```

## API Overview

### Core Operations

#### Set & Get
```java
// Strings
cache.set("key", "value");
cache.set("key", "value", Duration.ofMinutes(5)); // with TTL
String val = cache.getString("key");              // returns null if missing

// Numbers
cache.set("count", 42L);
cache.set("price", 19.99);
Long count = cache.getLong("count");
Double price = cache.getDouble("price");

// Bytes
cache.set("blob", new byte[] {1, 2, 3});
byte[] data = cache.getBytes("blob");

// Objects (any serializable type)
cache.set("user", user);
User retrieved = cache.get("user", User.class);

// Generic types (Map<String, ...>, List<...>, etc.) via TypeReference -- a raw Class
// can't express these due to type erasure
Map<String, Long> scores = cache.get("scores", new TypeReference<Map<String, Long>>() {});

// Optional variant
Optional<String> val = cache.findString("key");
```

#### Delete & Exists
```java
cache.delete("key1", "key2", "key3");
boolean exists = cache.exists("key");
```

#### Add & Replace (Atomic)
```java
cache.add("key", "value", ttl);       // Set only if absent (SET NX)
cache.replace("key", "newval", ttl);  // Set only if present (SET XX)
String old = cache.getSet("key", "newval"); // Atomic read-replace
```

#### TTL & Expiration
```java
cache.set("key", "value", Duration.ofMinutes(5));
boolean ok = cache.expire("key", Duration.ofHours(1));    // Reset TTL
boolean ok = cache.persist("key");                         // Remove expiry (never expires)
Duration remaining = cache.ttl("key");                     // Remaining time; Duration.ofSeconds(-1)
                                                             // if no TTL, null if key doesn't exist
cache.touch("key", Duration.ofMinutes(5));                  // Sliding expiration: refresh the TTL
```

TTLs are `Duration` values here, so there's no unit ambiguity like other libraries where an int could mean seconds or milliseconds — `Duration.ofSeconds(5)` vs `Duration.ofMillis(5)` is explicit either way. Expiration applies identically to every value type (strings, numbers, POJOs, records — nothing special about objects), and is enforced two ways:
- **Lazily**, on every `get`/`getString`/`exists`/etc. call: an expired row is treated as a miss and deleted on the spot, even with the background sweeper disabled (`sweepInterval(null)`).
- **Actively**, by a background sweeper thread that runs every `sweepInterval` (default 60s) and deletes expired rows in batches, so disk isn't held by dead keys even if nothing ever reads them again.

If you inspect the `.db` file directly shortly after a key expires but before the next sweep tick, the row may still be physically present — that's expected; `get()`/`exists()` from Java will already correctly treat it as gone. Lower `sweepInterval` (or call `cache.vacuum()`) if you need the file to shrink sooner.

#### Atomic Counters
```java
long newValue = cache.incr("counter");                 // +1
long newValue = cache.incr("counter", 5);              // +5
long newValue = cache.decr("counter");                 // -1
double newValue = cache.incrDouble("metric", 0.5);     // +=0.5
```

#### Batch Operations
```java
cache.setAll(Map.of("k1", "v1", "k2", "v2"), ttl);
Map<String, String> vals = cache.getAll(Arrays.asList("k1", "k2"));
```

#### Flush & Delete
```java
cache.delete("key1", "key2");   // delete specific keys; returns how many actually existed
cache.flush();                  // delete EVERYTHING in the current namespace -- no key argument
```

`flush()` takes no key/pattern argument by design — it always clears the entire namespace (the default namespace, unless you called `.namespace(...)` on the builder). To clear only a subset, delete by key or by pattern instead:

```java
cache.keys("session:*").forEach(cache::delete);
```

If two `LyteCache` instances point at the **same file but different `.namespace(...)`**, they're isolated — `flush()` on one never touches the other's keys. If `flush()` seems to leave data behind, check you're flushing the same namespace you wrote to.

#### Key Scanning
```java
cache.keys("user:*").forEach(System.out::println); // GLOB pattern: *, ?, [...]
```

#### Distributed Locks
```java
try (CacheLock lock = cache.lock("resource", Duration.ofSeconds(10))) {
    // Critical section: only one process can hold this lock
    // Lock is automatically released on close
}
```

#### Memoization
```java
<T> T cached = cache.memoize("expensive_key", Duration.ofHours(1),
    () -> expensiveComputation());
```

#### Statistics
```java
CacheStats stats = cache.stats();
System.out.println("Keys: " + stats.keyCount());
System.out.println("Hit rate: " + stats.hitRate() + "%");
System.out.println("Size: " + stats.sizeBytes() + " bytes");
System.out.println("Database: " + stats.path());
```

### Configuration

```java
LyteCache cache = LyteCache.builder()
    .path(Path.of("/data/my-cache.db"))        // Optional; uses platform cache dir if omitted
    .namespace("sessions")                      // Isolate multiple caches in one file
    .maxKeys(100_000)                          // Evict when exceeded (default: 1M)
    .maxBytes(256L * 1024 * 1024)              // Evict when exceeded (default: 1 GB)
    .eviction(Eviction.LRU)                    // LRU (default), TTL, RANDOM, NOEVICTION
    .sweepInterval(Duration.ofSeconds(60))     // Background cleanup frequency
    .strict(false)                              // On read error: false=log+miss, true=throw
    .build();
```

### Hashes (atomic field writes)

`hset` and friends update single fields of a stored JSON object **atomically**. The new
document is computed inside SQLite from the row's current value, so concurrent writers to
different fields, or `hincrBy` on the same field, never lose each other's work, across
threads and processes. A get-modify-set in application code would.

```java
cache.hset("user:101", "status", "active");   // true: the field was new
cache.hincrBy("user:101", "logins", 1);       // atomic counter inside the document
String status = cache.hgetString("user:101", "status");
Map<String, Object> all = cache.hgetAll("user:101");
```

- **A hash is just a JSON value.** `set(key, map)` is usable with `hget`, and
  `get(key, Map.class)` returns the whole document.
- **Missing keys are created** with no expiry, unless you use the `ttlIfCreated` overloads,
  which apply only on creation. Field writes never change an existing key's TTL, and
  expired keys are never revived.
- **Field names are literal top-level keys** (`"a.b"` is not a path).
- **Errors:** a non-object value throws `SerializationException` and is left untouched;
  `hincrBy` on a non-integer field throws `IllegalStateException`; overflowing a `long`
  throws `ArithmeticException`, with nothing written.
- **SQLite version:** `cache.hashMode()` / `stats().hashMode()` reports `"json1"`. This
  needs the bundled SQLite 3.53 (sqlite-jdbc 3.53.4.0); SQLite 3.44/3.45 mishandle escaped
  quotes in JSON paths and would be detected and switched to the read-modify-write
  fallback. JSONB is never used.

### Events (local notification)

Processes sharing one cache file **on one machine** can notify each other. The typical
case is invalidating in-memory state, for example between a Spring app and its background
worker:

```java
// worker A, after committing a DB change
cache.delete("balance:42");
cache.notify("invalidate", Map.of("key", "balance:42"));

// workers B..N
try (Subscription sub = cache.watch("invalidate")) {
    for (Event e : sub) localMemo.remove(e.payload(Map.class).get("key"));
}
```

This is **not Redis Pub/Sub**. It is single host only, polled (about 50 ms; up to about
1 s after an idle spell), and durable: events are rows with a monotonic `seq()`, so
`watch(WatchOptions.defaults().fromSeq(cursor), ...)` resumes after a restart. Delivery
is at-least-once, so handlers must be idempotent. Events are kept for
`eventRetention(Duration)` (default 1 h). A subscriber that falls further behind gets a
`GapException`, never a silent skip.

A `Subscription` is `Iterable<Event>` and `AutoCloseable`, with `poll(Duration)`,
`tryNext()`, and a push form, `onEvent(Consumer)` with `onGap(Consumer)`. It is opt-in:
no table and no thread exist until you use it.

`onChange(listener)` is the in-process, immediate alternative.
`builder().publishChanges(ChangeOp.DELETE, ChangeOp.FLUSH)` publishes those mutations to
the reserved `__cache__` channel for other processes. Bulk operations emit one
`ChangeRecord`, not one per key. Full guide:
[docs/notifications.md](../docs/notifications.md).

## Where Is My Data?

By default, LyteCache stores the database file in your platform's cache directory:

- **Linux**: `$XDG_CACHE_HOME/lytecache/<project-id>.db` (or `~/.cache/lytecache/` if `$XDG_CACHE_HOME` is unset)
- **macOS**: `~/Library/Caches/lytecache/<project-id>.db`
- **Windows**: `%LOCALAPPDATA%\lytecache\<project-id>.db`

The `<project-id>` is the first 12 hex characters of the SHA-256 hash of the current working directory's resolved (symlink-free, absolute) path, so different projects get separate cache files automatically -- and this derivation is identical to the Python implementation's, so two LyteCache processes (in either language) started from the same directory share the same file.

You can override the location with:
1. **Builder**: `.path(Path.of("/explicit/path/cache.db"))`
2. **Environment variable**: `LYTECACHE_PATH=/custom/cache.db`

To find your cache file programmatically:
```java
Path where = cache.path(); // Returns the actual file path
Path defaultPath = LyteCache.defaultPath(); // Returns the default location
```

## When to Use LyteCache

### ✅ Good Fit

- Single-node web applications (sessions, user profiles, config)
- Distributed tracing / observability systems (single writer + many readers)
- Developer tools, CLIs, and batch jobs
- Unit test fixtures
- Ad-hoc caching where Redis/Memcached are overkill
- Multi-process scenarios where you need portable, lock-safe coordination

### ❌ Not a Good Fit

- Multi-server shared cache (use Redis or Memcached)
- Very heavy concurrent writes (SQLite is single-writer)
- Pub/sub messaging (use Kafka, RabbitMQ, or Redis)
- Complex queries on cached data (use a database)

## Type Codes & Cross-Language Compatibility

LyteCache follows a cross-language storage spec so that values written by the Python, Go, or Ruby libraries can be read by Java and vice versa. Internally, values are tagged with a type code:

- **0**: Raw bytes
- **1**: UTF-8 string
- **2**: 64-bit signed integer
- **3**: 64-bit IEEE double
- **4**: JSON (UTF-8)
- **5**: Python pickle (reserved; throws `SerializationException` if encountered)
- **6**: Java native serialization (reserved; throws `SerializationException` if encountered)

By design, native serialization formats (pickle, Java serialized objects) are rejected. All persistent values must be portable JSON, ensuring seamless interop with other languages.

## Performance & Concurrency

- **Threading**: Thread-safe via WAL mode and a small connection pool (one writer, one reader per thread).
- **LRU buffering**: Reads don't immediately write; `last_accessed` updates are batched and flushed periodically or on sweep/close.
- **Background sweeper**: Configurable via `sweepInterval(Duration)`. Set to `null` to disable and rely on opportunistic cleanup.
- **Eviction**: O(n) to find victims; configurable policies (LRU, TTL, RANDOM, NOEVICTION).

For typical single-threaded usage, reads are fast (< 1ms), writes are ~10ms (includes sync to disk).

## Dependencies

Runtime:
- `org.xerial:sqlite-jdbc` — SQLite JDBC driver (3.53.4.0, bundling SQLite 3.53: needed for the JSON1 hash path)
- `com.fasterxml.jackson.core:jackson-databind` — JSON serialization
- `com.fasterxml.jackson.datatype:jackson-datatype-jsr310` — `java.time` support

Test:
- JUnit 5 + AssertJ

No Spring, no Guava, no external logging framework — pure Java 17+ with SQLite.

## License

Apache License 2.0

---

**Questions?** See [SPEC.md](SPEC.md) for the detailed storage schema and type encoding.
