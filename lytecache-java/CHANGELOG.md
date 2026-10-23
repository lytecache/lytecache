# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Hash operations on JSON object values: `hset`, `hsetnx`, `hsetMany`, `hget` (natural and
  typed), `hgetString`, `hgetLong`, `hmget`, `hgetAll`, `hdel`, `hexists`, `hkeys`, `hvals`,
  `hlen`, `hincrBy`, `hincrByFloat`, plus `hashMode()` and `CacheStats.hashMode()`. Writes
  are computed inside SQLite, so concurrent field updates are never lost. No schema change.
- Local event notification: `notify`, `notifyMany`, `watch` (returns a `Subscription`:
  `Iterable<Event>`, `poll`, `tryNext`, `onEvent`/`onGap`, `close`), `WatchOptions`,
  `saveCursor`, `loadCursor`, `GapException`, `Event`, `Notification`. Single host only,
  polled, durable. The `events` table is created on first use; `schema_version` stays 1.
- `onChange(...)`: in-process change listeners, one `ChangeRecord` per call; `ChangeOp`.
- Builder options `publishChanges`, `eventRetention`, `eventMaxCount`, `changeKeyCap`.

### Changed

- `org.xerial:sqlite-jdbc` 3.44.0.0 → 3.53.4.0. SQLite 3.44 mishandles escaped quotes in
  JSON path labels, which hash operations rely on.
- `CacheStats` gained a `hashMode` component; the previous 7-argument constructor remains.

## [0.2.0] - 2026-07-07

Initial release. Embedded, Redis-like caching backed by SQLite (via `org.xerial:sqlite-jdbc`),
matching the storage format and semantics of the Python, Node.js, Go, and PHP implementations in
this repository:

- Zero-config `new LyteCache()` / `LyteCache.builder()`, with the same default-path derivation as
  the other implementations.
- Core API: `set`/`get`/`delete`/`exists`/`add`/`replace`/`getSet`/`setAll`/`getAll`, typed getters
  (`getString`/`getLong`/`getDouble`/`getBytes`), and generic `get(key, Class<T>)` /
  `get(key, TypeReference<T>)` for POJOs, records, and generic types a raw `Class` can't express.
- TTL/expiration: `expire`/`persist`/`ttl`/`touch` using `Duration` (no unit ambiguity), lazy
  expiration on every read, plus a background sweeper thread for active cleanup.
- Atomic counters (`incr`/`decr`/`incrDouble`) via a single-statement SQL UPSERT, thread-safe and
  safe across processes sharing one file.
- Eviction policies: `LRU` (default), `TTL`, `RANDOM`, `NOEVICTION`.
- `keys()` (lazy `Stream<String>`, GLOB syntax), `flush()`, `stats()`, `vacuum()`, `close()`
  (`AutoCloseable`).
- `memoize()` read-through helper.
- `lock()`: process-safe distributed lock, `AutoCloseable` via try-with-resources.
- Jackson-based serialization: values are portable JSON, readable by every other language's
  implementation; native Java serialization is never used for the persisted format.
