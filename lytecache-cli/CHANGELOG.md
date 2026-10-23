# Changelog

All notable changes to this project are documented in this file.


## [Unreleased]

### Added

- `notify <channel> [payload]`, `watch <channel-pattern>... [--from-seq N] [--count N]`
  (one JSON line per event), and `events [--limit N] [--json]` for lytecache's local event
  notification (single host only, polled, durable).
- `hget`, `hgetall` and `hset` for field-level access to JSON object values.
- Admin UI: an **Events** tab per database, greyed out for files that never used
  notification. It has:
  - a live tail with a channel filter (the same rule as `watch`), adaptive refresh and a
    pause button, and an event viewer that reuses the value viewer;
  - a retention panel with the publish rate and per-channel counts;
  - **subscriber lag** for every saved cursor, flagging cursors that lost events
    (outside retention).
  There is deliberately no route that publishes an event. Under `--allow-delete`, events
  can be purged (one event, older than an age, or a whole channel with typed
  confirmation), and every purge is audit-logged.
- Fleet dashboard: retained-event and publish-rate columns, and an unhealthy flag for a
  database with a subscriber that lost events.
- `/metrics`: `lytecache_events_retained`, `lytecache_events_published_total` (a real
  counter), `lytecache_event_oldest_age_seconds`, `lytecache_subscriber_lag_events`,
  `lytecache_subscriber_lag_seconds` and `lytecache_subscriber_gap`. docs/ui.md adds alert
  rules for lag and for lost events.
- `--mask-channels <glob>` redacts whole channels' event payloads, and `--mask-keys` now
  also redacts event payloads that reference a matching key or field.
- `ui.yaml` `databases:` entries accept `event_retention` (the application's retention,
  shown on the Events tab).

### Fixed

- The UI's database handles no longer run lytecache's background sweeper (an inspection
  tool must not write to files unless asked), and neither the UI nor `lytecache maintain`
  deletes events any more. Both had been applying the library's default 1 h event
  retention, whatever the application had configured. Neither bug was ever released.

### Changed

- `watch` now tails events. The old stats redraw moved to `stats --watch[=seconds]`;
  `watch` with no arguments or a single number still works, with a deprecation note.
- Requires the unreleased lytecache-go hash and event APIs. Development builds use the
  repository's `go.work`; tag a lytecache-go release and bump `go.mod` before releasing
  the CLI.

## [Unreleased]

- `lytecache ui`: a local web administration console (RedisInsight/pgAdmin-style, not a cache
  server -- no wire protocol, nothing an application connects to). Fleet dashboard across every
  configured database, a namespace-aware key browser with glob/sort/pagination, a value viewer
  handling every type code including the non-portable-language message, and cross-database search.
  Delete-only: single/bulk key delete, typed-confirmation namespace flush, a maintenance pass, and
  vacuum -- all gated behind `--allow-delete` (vacuum excepted, since it deletes no rows); there is
  no route, anywhere, that can create or alter a value.
  - Authentication: `ui.yaml` (argon2id-hashed password, session secret, database list), default
    `admin`/`admin` on first run, `lytecache ui passwd`/`reset-password`, session cookies, CSRF,
    per-IP rate limiting. Binding beyond `127.0.0.1` with the default password refuses to start
    outright; `--mask-keys` permanently redacts matching keys; every write is appended to an audit
    log (key names only, never values).
  - `/metrics`: standard Prometheus text exposition via `prometheus/client_golang`, gauges labeled
    by database/namespace, a 5s (configurable) computed-value cache so scraping never hammers the
    files, bearer-token auth mandatory once bound beyond loopback.
  - `lytecache service {install,uninstall,start,stop,restart,status,logs}`: runs `ui` as a managed
    background service via `kardianos/service` (a user LaunchAgent on macOS, a user or `--system`
    systemd unit on Linux, the Service Control Manager on Windows with a Scheduled Task fallback
    when not elevated) -- the exact same server-startup code path as the foreground command, which
    keeps working unchanged. `lytecache ui open` opens it in a browser.
  - See [docs/ui.md](docs/ui.md) for the full guide: multi-database configuration, the SSH-tunnel
    recommendation, exposure guardrails, and per-OS service instructions.

Depends on `lytecache-go` v0.3.0+ (for `Cache.Namespaces`/`SchemaVersion`/`Limits` and
`Stats.ExpiredPresent` -- see that repo's changelog).

## [0.1.0] - 2026-08-05

Initial release. A `redis-cli`-style command-line tool for lytecache database files, built entirely on [`lytecache-go`](https://github.com/lytecache/lytecache-go)'s public API:

- One-shot commands: `get`/`set`/`del`/`exists`/`ttl`/`expire`/`persist`/`touch`/`incr`/`decr`/`keys` (alias `scan`)/`stats` (alias `info`)/`flush`/`maintain`/`vacuum`/`which`/`dump`/`watch`.
- Interactive REPL (bare `lytecache`) with line editing, history, case-insensitive command names, and Ctrl-C/Ctrl-D handling.
- Database resolution via `--db` / `LYTECACHE_PATH` / the library's default path.
- Exit codes (`0`/`1`/`2`/`3`) scripts can depend on; values to stdout, diagnostics to stderr.
- Value type handling: JSON pretty-printing, `--raw` for exact bytes, base64/`--file`/stdin for `--type bytes`, and a graceful `(non-portable value: ...)` message for Python-pickle/Java-serialized values this CLI cannot decode.
- Distribution: `go install`, Homebrew tap, Scoop bucket, winget manifest (attached to releases pending `microsoft/winget-pkgs` submission), `.deb`/`.rpm` packages, and a checksum-verifying `install.sh`.
- A multi-arch (`linux/amd64`/`linux/arm64`) Docker image at `ghcr.io/lytecache/lytecache` (~7 MB, `scratch`-based), plus a `distroless`-based variant, for inspecting a cache file without installing Go.

Depends on `lytecache-go` v0.2.0+ (for `Cache.Inspect` and `Cache.Maintain` — see that repo's changelog).
