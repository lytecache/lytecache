# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Hash operations on JSON object values: `hset`, `hsetnx`, `hsetMany`, `hget`, `hmget`,
  `hgetAll`, `hdel`, `hexists`, `hkeys`, `hvals`, `hlen`, `hincrBy`, `hincrByFloat`, plus
  `hashMode()` and `CacheStats::$hashMode`. Writes are computed inside SQLite, so concurrent
  field updates are never lost. If the system SQLite lacks JSON1, they fall back to
  read-modify-write under `BEGIN IMMEDIATE`. No schema change.
- Local event notification: `notify`, `notifyMany`, `watch` (returns a `Subscription`:
  `foreach`, `poll`, `tryNext`, `close`), `saveCursor`, `loadCursor`, `Event`,
  `GapException`. Single host only, polled, durable. The `events` table is created on first
  use; `schema_version` stays 1.
- `onChange($listener, $events)`: in-process change listeners, one `ChangeRecord` per call.
- Constructor options `publishChanges`, `eventRetention`, `eventMaxCount`, `changeKeyCap`,
  `logger`.

## [0.2.1] - 2026-10-23

> **Correctness advisory: upgrade if you use 0.2.0.** After any write failed, later writes
> could be silently lost. This is not a security issue, but it is data loss.

### Fixed

- **Writes silently lost after a failed write (all PHP versions).** When a write failed inside
  its transaction (for example a constraint violation raised by the database), the cached
  prepared statement it used was left un-reset. The next write
  through that statement then did nothing and reported no error: `set()` returned normally, but
  the value was never stored. Every later write through the same statement was lost the same way
  until the `LyteCache` instance was discarded.
  - Affected: `set`, `setMany`, `add`, `incr`/`decr` and other writes that reuse a statement,
    on every supported PHP version. On PHP 8.4 more combinations were affected (for example, a
    failed `setMany` also broke later `set` calls).
  - Not affected: a process whose writes never fail. The bug needs one write to fail first.
  - Symptoms: values missing after a successful `set()`; `incr()` throwing "not numeric" on a key
    that doesn't exist.
  - Fix: every statement is now reset before it runs, and a statement that fails without raising
    an error now throws instead of returning silently. Statement caching is unchanged; the
    measured overhead is within run-to-run noise.

## [0.1.0] - Unreleased

Initial release. Embedded, Redis-like caching backed by SQLite (via `pdo_sqlite`), matching the
storage format and semantics of the Python, Java, Node.js, and Go implementations in this
repository, plus a Laravel cache driver:

- Zero-config `new LyteCache()`, with the same default-path derivation as Python/Java/Node.js/Go.
- Core API: `set`/`get`/`delete`/`has`/`add`/`replace`/`getSet`/`setMany`/`getMany`.
- TTL/expiration: `expire`/`persist`/`ttl`/`touch`, lazy expiration plus opportunistic maintenance
  (`maintain()`, throttled by `sweepInterval` -- PHP has no background threads to run an active
  sweeper).
- Atomic counters (`incr`/`decr`/`incrFloat`) via a single-statement SQL UPSERT, correct across
  concurrent PHP-FPM worker processes sharing one file.
- Eviction policies: `Eviction::LRU` (default), `TTL`, `Random`, `NoEviction`.
- `keys()` generator (GLOB syntax), `flush()`, `stats()`, `vacuum()`, `close()`.
- `remember()` read-through helper, `Bytes` wrapper for binary values, typed rehydration via
  `get($key, class: SomeClass::class)`.
- `lock()`: process-safe distributed lock with `CacheLock::release()`/`block()`.
- Laravel integration: `LytecacheServiceProvider` (auto-discovered), `LytecacheStore` implementing
  `Illuminate\Contracts\Cache\Store` and `LockProvider`, `lytecache:maintain` artisan command,
  zero-config `cache.stores.lytecache` merge.

### Notes

- Session and queue drivers are `v0.2` candidates, not implemented in this release.
- `illuminate/contracts`/`illuminate/support` are `suggest`ed, not required -- the core has zero
  runtime dependencies beyond `ext-pdo`/`ext-pdo_sqlite`.
