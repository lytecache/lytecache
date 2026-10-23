# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Local event notification: `notify`, `notify_many`, `watch` (returns a
  `Subscription`: iterate, `poll`, `try_next`, `close`), `save_cursor`,
  `load_cursor`, and `GapError`. Single host only, polled, durable. Events
  live in an `events` table created on first use; `schema_version` stays 1
  so older libraries still open the file. See docs/notifications.md.
- `on_change(listener, events)`: in-process listeners called after each
  committed mutation, one record per call (bulk operations included).
- Constructor options `publish_changes`, `event_retention`,
  `event_max_count`, `change_key_cap`.

- Hash operations on JSON object values: `hset`, `hset_many`, `hsetnx`,
  `hget`, `hmget`, `hgetall`, `hdel`, `hexists`, `hkeys`, `hvals`, `hlen`,
  `hincrby`, `hincrbyfloat`. Each write is computed inside SQLite from the
  row's current value, so concurrent field updates are never lost. No
  schema change: hashes are ordinary `value_type` 4 rows, and
  `schema_version` stays 1. See SPEC.md, "Hashes".
- `stats()["hash_mode"]`: `"json1"`, or `"fallback"` when the linked SQLite
  lacks JSON1, in which case hash writes use read-modify-write under
  `BEGIN IMMEDIATE`.

## [0.2.0] - Unreleased

Serialization upgrade. No schema changes (schema_version stays 1) and no
breaking changes to existing method signatures other than:

- The default `serializer` changed from `"json"` to `"auto"`. Behavior is
  identical for every value type `"json"` already handled; `"auto"`
  additionally accepts dataclasses and plain objects (still JSON-encoded,
  never pickled).
- `get(key, default=None, cls=None)`: new optional `cls` param to
  reconstruct a dataclass or plain object from a stored JSON value.
  `get_many` is unaffected (still returns plain `dict`/`list`).
- New constructor param `allow_pickle=False`.
- New `serializer="pickle"` mode: falls back to pickling (value_type=5)
  only for values that codes 0-4 can't represent; opt-in only, and reading
  a pickled value under `serializer="auto"` additionally requires
  `allow_pickle=True`. `serializer="json"` never reads or writes pickled
  data, guaranteeing a fully portable file.
- value_type codes 5 (pickle) and 6 (reserved for other languages' native
  formats, e.g. Java) are now recognized; reading an unrecognized or
  unsupported code raises `SerializationError` naming the code instead of
  returning raw bytes.
- The default-path project-id hash was shortened from 16 to 12 hex
  characters to match SPEC.md, which moves the zero-config default cache
  file to a new path for existing users (the old file is left in place,
  unused; delete it manually if you want to reclaim the space).

See SPEC.md for the full updated type-code table and JSON encoding rules.

## [0.1.0] - Unreleased

Initial release.

- `LyteCache` class: the entire public API, zero configuration required.
- Zero-config default database path derived from the platform cache
  directory and the current working directory; `LYTECACHE_PATH` env var
  override; explicit `path=` escape hatch.
- Key/value operations: `set`, `get`, `delete`, `exists`, `add`, `replace`,
  `get_set`, `set_many`, `get_many`.
- Expiration: `expire`, `persist`, `ttl`, `touch`; lazy expiration on every
  read path plus an active background sweeper.
- Atomic counters: `incr`, `decr`, `incr_float`, implemented as single-SQL
  UPSERTs for correctness under multi-process concurrency.
- Introspection & management: `keys`, `flush`, `stats`, `vacuum`, `close`,
  context-manager support.
- Extras: `memoize` decorator, `lock` context manager.
- Eviction policies: `lru`, `ttl`, `random`, `noeviction`.
- JSON-based serialization (no pickle); schema version 1, documented in
  SPEC.md.
