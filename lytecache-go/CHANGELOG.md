# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

## [0.2.0] - 2026-07-09

Initial release. Embedded, Redis-like caching backed by SQLite (via the pure-Go `modernc.org/sqlite` driver), matching the storage format and semantics of the Python, Java, Node.js, and PHP implementations in this repository:

- Zero-config `lytecache.New()`, with the same default-path derivation as Python/Java/Node.js.
- `Set`/`Get`/`Delete`/`Exists`/`Add`/`Replace`/`GetSet`/`SetMany`/`GetMany`, with typed `GetBytes`/`GetString`/`GetInt64`/`GetFloat64` convenience wrappers.
- TTL/expiration: `Expire`/`Persist`/`TTLOf`/`Touch`, lazy + active expiration.
- Atomic counters (`Incr`/`Decr`/`IncrFloat`) via a single-statement SQL UPSERT, safe under concurrent goroutines and concurrent OS processes.
- Eviction policies: `LRU` (default), `TTLPolicy`, `Random`, `NoEviction`.
- `Keys` cursor iterator (Go 1.23 `iter.Seq2`), `Flush`, `Stats`, `Vacuum`, `Close` (idempotent).
- `Memoize` read-through helper (package-level generic function).
- `Lock`: process-safe distributed lock.
- Sentinel errors (`ErrCacheFull`, `ErrSerialization`, `ErrSchemaVersion`, `ErrLockTimeout`, `ErrNotNumeric`), all matchable via `errors.Is`.
- Runnable `Example` functions for every major feature, rendered on pkg.go.dev.
- `Cache.Maintain()`: runs one maintenance pass (the same work the background sweeper does) on
  demand, returning how many rows it removed for expiry vs. eviction. For callers who disabled the
  sweeper (`WithSweepInterval(0)`) and want to run a pass on their own schedule.
- `Cache.Inspect(key)`: returns raw on-disk row metadata (`value_type` code, timestamps, size,
  access count) without decoding the value -- for debugging/introspection tools, not ordinary
  application code.

The last two additions exist specifically so [lytecache-cli](https://github.com/lytecache/lytecache-cli) -- a separate repo/module, not part of this one -- can inspect and maintain a database file through the public API alone, with no duplicated cache logic. See that repo for the CLI itself.
