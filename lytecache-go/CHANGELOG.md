# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Hash operations on JSON object values: `HSet`, `HSetNX`, `HSetMany`, `HGet`, `HGetRaw`,
  `HGetString`, `HGetInt64`, `HMGet`, `HGetAll`, `HDel`, `HExists`, `HKeys`, `HVals`, `HLen`,
  `HIncrBy`, `HIncrByFloat`, plus `HashMode()` and `Stats.HashMode`. Writes are computed
  inside SQLite, so concurrent field updates are never lost. No schema change.
- Local event notification: `Notify`, `NotifyMany`, `Watch` (returns `*Subscription`:
  `Events()`, `Next`, `TryNext`, `Err`, `LastSeq`, `Close`), `FromSeq`, `PollInterval`,
  `SaveCursor`, `LoadCursor`, `*GapError`/`ErrGap`. Single host only, polled, durable. The
  `events` table is created on first use; `schema_version` stays 1.
- `OnChange(fn, ops...)`: in-process change listeners, one `ChangeRecord` per call.
- Options `WithPublishChanges`, `WithEventRetention`, `WithEventMaxCount`, `WithChangeKeyCap`.
- `EventStats()` / `RecentEvents()` for admin tooling, and `RawValue.ValueType()`.
  `RecentEvents` takes optional channel patterns.
- More admin-tooling reads and purges for events: `MatchChannel` (Watch's exact matching
  rule), `EventChannelStats`, `EventCountSince`, `Cursors` (every saved cursor, across
  namespaces), `DeleteEvent`, `DeleteEventsBefore` (removes a seq prefix, like retention)
  and `DeleteChannelEvents`. None of them creates the events table, and there is no
  admin method that inserts or rewrites an event.
- Errors `ErrOverflow`, `ErrGap`, `ErrSubscriptionClosed`, `ErrInvalidArgument`.

## [0.3.0] - 2026-08-07

- `Cache.Namespaces()`: enumerates every namespace present in a database file (key count, size on
  disk, expired-but-not-yet-swept count per namespace), regardless of which namespace the `Cache`
  itself is pinned to. A pure read against the read connection pool -- unlike `Get`/`Inspect`, it
  never opportunistically deletes expired rows or updates LRU bookkeeping, so it's safe to call on
  a fixed polling interval.
- `Cache.SchemaVersion()`: exposes the on-disk `schema_version` a file was opened with (previously
  read internally at open time but never surfaced).
- `Cache.Limits()`: exposes the `MaxKeys`/`MaxBytes` capacity limits a `Cache` was constructed with
  (previously private fields with no accessor).
- `Stats.ExpiredPresent`: count of keys in a namespace past their `expires_at` but not yet swept --
  a healthy cache keeps this at or near zero.

These four additions exist specifically so [lytecache-cli](https://github.com/lytecache/lytecache-cli)'s
`ui` web admin command can render a per-namespace, per-file fleet dashboard and Prometheus metrics
through the public API alone, with no raw SQL in the UI layer.

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
  access count) without decoding the value. Meant for debugging and introspection tools, not
  ordinary application code.

The last two additions exist specifically so [lytecache-cli](https://github.com/lytecache/lytecache-cli), a separate repo and module, not part of this one, can inspect and maintain a database file through the public API alone, with no duplicated cache logic. See that repo for the CLI itself.
