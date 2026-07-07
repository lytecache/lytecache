# Changelog

All notable changes to this project are documented in this file.

## [0.2.0] - Unreleased

Initial release. Embedded, Redis-like caching backed by SQLite (via better-sqlite3), matching the
storage format and semantics of the Python and Java implementations in this repository:

- Zero-config `new LyteCache()`, with the same default-path derivation as Python/Java.
- Synchronous API: `set`/`get`/`delete`/`exists`/`add`/`replace`/`getSet`/`setMany`/`getMany`.
- TTL/expiration: `expire`/`persist`/`ttl`/`touch`, lazy + active expiration.
- Atomic counters (`incr`/`decr`/`incrFloat`) via single-statement SQL UPSERT, with `bigint`
  support beyond `Number.MAX_SAFE_INTEGER`.
- Eviction policies: `lru` (default), `ttl`, `random`, `noeviction`.
- `keys()` cursor iterator, `flush()`, `stats()`, `vacuum()`, `close()`.
- `memoize`/`memoizeAsync`/`wrap` read-through helpers.
- `lock()`: process-safe distributed lock with `Symbol.dispose` support.
- Dual ESM + CJS build with type declarations.

### Notes

- `vitest` is pinned to `^3.2.4` (not the latest `^4.x`) with a `vite` override pinned to `^6.3.6`.
  `vitest@4` depends on `vite@8`, which bundles `rolldown` -- `rolldown`'s ESM build imports
  `styleText` from `node:util`, an export that doesn't exist before Node 20.12. Since this package's
  CI matrix (and `engines.node`) supports Node 18, that combination crashes `vitest run` outright on
  Node 18 with `SyntaxError: The requested module 'node:util' does not provide an export named
  'styleText'`. `vitest@3` still resolves to `vite@6`, the last major with real Node 18 support.
  Don't bump `vitest` past `3.x` (or drop the `vite` override) without also dropping Node 18 from
  the support matrix.
- `better-sqlite3` is pinned to `^11.10.0` (not the latest `^12.x`). `better-sqlite3@12.0.0` dropped
  Node 18 support entirely (a documented breaking change, no API changes) -- since it has no
  prebuilt binary for Node 18, `npm install` falls back to compiling from source via `node-gyp`,
  which then fails on CI (and on any machine without a full native build toolchain) since that
  fallback path was never set up to succeed. `better-sqlite3@11.10.0` is the last version with
  Node 18 prebuilt binaries and has an identical API to `12.x`. Don't bump past `11.x` without also
  dropping Node 18 from the support matrix.
