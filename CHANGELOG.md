# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - Unreleased

Initial release.

- `LiteCache` class: the entire public API, zero configuration required.
- Zero-config default database path derived from the platform cache
  directory and the current working directory; `LITECACHE_PATH` env var
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
