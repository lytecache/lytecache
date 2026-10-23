# lytecache

Redis-like caching for Go with zero infrastructure: no server, no CGO, just a file. `lytecache` gives you the familiar Redis API surface (`Set`/`Get`, TTLs, atomic counters, eviction, distributed locks), backed by a local SQLite file instead of a daemon. It uses a pure-Go SQLite driver, so `go build`, `go install`, and cross-compilation stay exactly as simple as they'd be without it.

## Install

```bash
go get github.com/lytecache/lytecache-go
```

## Quickstart

```go
import "github.com/lytecache/lytecache-go"

cache, err := lytecache.New() // no path, no setup -- just works
defer cache.Close()

cache.Set("user:42", map[string]any{"name": "Samson"}, lytecache.TTL(5*time.Minute))

var user map[string]any
found, err := cache.Get("user:42", &user)

n, err := cache.Incr("hits", 1)
```

That's it. The first call to `lytecache.New()` creates the database file (including any missing parent directories) and applies the schema automatically. There's no `Init()`, no migration step, and no server to start.

## API

| Method | Description |
|---|---|
| `Set(key, value, opts...)` | Store a value. `TTL(d)` is the optional expiry. |
| `Get(key, dest)` | Read into `dest` (a pointer); `found=false` on miss or expiry. Never errors on a miss. |
| `GetBytes`/`GetString`/`GetInt64`/`GetFloat64(key)` | Typed convenience wrappers around `Get`. |
| `Delete(keys...)` | Delete keys; returns how many actually existed. |
| `Exists(key)` | Whether a (non-expired) key is present. |
| `Add(key, value, opts...)` | Set only if absent (atomic `SET NX`). |
| `Replace(key, value, opts...)` | Set only if present (atomic `SET XX`). |
| `GetSet(key, value, dest)` | Atomically swap in a new value, decoding the old one into `dest`. |
| `SetMany(entries, opts...)` / `GetMany(keys)` | Bulk set/get in a single transaction; `GetMany` returns `map[string]RawValue` (`.Decode(dest)` per value). |
| `Expire(key, ttl)` / `Persist(key)` | Set or remove a TTL on an existing key. |
| `TTLOf(key)` | Remaining TTL, whether it has one, and whether the key exists. |
| `Touch(key, ttl)` | Refresh a key's TTL (sliding expiration). |
| `Incr(key, amount)` / `Decr(key, amount)` | Atomic integer counters. |
| `IncrFloat(key, amount)` | Atomic float counter. |
| `Keys(pattern)` | Lazily iterate matching keys (`iter.Seq2[string, error]`, GLOB syntax: `*`, `?`, `[...]`). |
| `Flush()` | Clear the current namespace. |
| `Stats()` | Hits, misses, hit rate, key count, size, evictions, path. |
| `Vacuum()` / `Close()` | Reclaim disk space / shut down cleanly (idempotent). |
| `lytecache.Memoize(cache, key, ttl, loader)` | Read-through cache for a computed value. It's a package-level generic function, since Go methods can't have type parameters. |
| `Lock(name, timeout)` | Process-safe distributed lock; release via `lock.Release()`. |
| `HSet(key, field, value, opts...)` / `HSetMany` / `HSetNX` | Atomically set field(s) of a JSON object value. See [Hashes](#hashes). |
| `HGet(key, field, dest)` / `HGetRaw` / `HMGet` / `HGetAll` | Read fields without decoding the rest of the document. |
| `HDel` / `HExists` / `HKeys` / `HVals` / `HLen` | Remove, test or list fields. |
| `HIncrBy(key, field, n)` / `HIncrByFloat` | Atomic counters inside a JSON object. |
| `Notify(channel, payload)` / `NotifyMany` | Publish a local event (single host, polled, durable). See [Events](#events-local-notification). |
| `Watch(patterns, FromSeq(n), PollInterval(d))` | Subscribe; receive from `sub.Events()`, or `sub.Next(ctx)` / `sub.TryNext()`; `sub.Close()`. |
| `SaveCursor(name, seq)` / `LoadCursor(name)` | Persist a subscriber position for durable resume. |
| `OnChange(fn, ops...)` | In-process callback after each committed mutation. |

All errors that indicate a specific condition are sentinel errors you can test with `errors.Is`: `ErrCacheFull`, `ErrSerialization`, `ErrSchemaVersion`, `ErrLockTimeout`, `ErrNotNumeric`, `ErrOverflow`, `ErrGap` (matched by `*GapError`), `ErrSubscriptionClosed`, `ErrInvalidArgument`.

## Hashes

`HSet` and friends update single fields of a stored JSON object **atomically**. The new
document is computed inside SQLite from the row's current value, so concurrent writers
(goroutines or processes) to different fields, or `HIncrBy` on the same field, never lose
each other's work. A get-modify-set in application code would.

```go
created, _ := cache.HSet("user:101", "status", "active") // true: the field was new
cache.HIncrBy("user:101", "logins", 1)                    // atomic counter inside the document
var status string
found, _ := cache.HGet("user:101", "status", &status)
```

- **A hash is just a JSON value.** `Set(key, map/struct)` is usable with `HGet`, and `Get`
  returns the whole document.
- **Missing keys are created** with no expiry, unless you pass `TTL(d)`, which applies only
  on creation. A field write never changes an existing key's TTL. Expired keys are treated
  as missing and never revived.
- **Field names are literal top-level keys** (`"a.b"` is not a path).
- **Errors:** a non-object value returns `ErrSerialization` and is left untouched;
  `HIncrBy` on a non-integer field returns `ErrNotNumeric`; leaving the int64 range
  returns `ErrOverflow`, with nothing written.
- **Lock cost:** a hash write takes the same write lock as `Set`. The win is atomicity and
  smaller payloads, not lock contention.
- **SQLite version:** `modernc.org/sqlite` bundles a recent SQLite with JSON1.
  `Stats().HashMode` reports `"json1"`, or `"fallback"` (read-modify-write under
  `BEGIN IMMEDIATE`) on an SQLite without it. JSONB is never used, so other languages can
  read the values.

## Events (local notification)

Processes sharing one cache file **on one machine** can notify each other. The typical
case is invalidating in-memory state across workers:

```go
// worker A, after committing a DB change
cache.Delete("balance:42")
cache.Notify("invalidate", map[string]any{"key": "balance:42"})

// workers B..N
sub, _ := cache.Watch([]string{"invalidate"})
defer sub.Close()
for ev := range sub.Events() {
    var p struct{ Key string `json:"key"` }
    ev.Decode(&p)
    delete(localMemo, p.Key)
}
```

This is **not Redis Pub/Sub**. It is single host only, polled (about the 50 ms poll
interval; up to about 1 s after an idle spell), and durable: events are rows with a
monotonic `Seq`, so `Watch(..., FromSeq(cursor))` resumes after a restart. Delivery is
at-least-once, so handlers must be idempotent. Events are kept for
`WithEventRetention` (default 1 h). A subscriber that falls further behind sees its
`Events()` channel close with `sub.Err()` returning a `*GapError`, never a silent skip.
It is opt-in: no table and no goroutine exist until you use it.

`OnChange(fn)` is the in-process, immediate alternative. `WithPublishChanges(OpDelete,
OpFlush)` publishes those mutations to the reserved `__cache__` channel for other
processes. Bulk operations emit one `ChangeRecord`, not one per key. Full guide:
[docs/notifications.md](../docs/notifications.md).

## Why a pure-Go SQLite driver?

`lytecache` uses [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a CGO-free, pure-Go port of SQLite, instead of a cgo-based binding. This is a deliberate choice, not an implementation detail:

- **No C toolchain required.** `go build`/`go install`/`go get` work the same everywhere, with no compiler, no `CGO_ENABLED=1`, and no platform-specific build flags.
- **Cross-compilation stays trivial.** Building a Linux binary from macOS (or any other combination) just works, exactly like any other pure-Go dependency.
- **It fits the zero-friction brand.** The whole point of `lytecache` is "no infrastructure," and that shouldn't quietly stop at "no server" while still requiring a working C build environment.

## When to use lytecache

**Good fit:**
- Single-node Go services (or single-machine, multi-process deployments) that want caching, counters, or TTLs with zero infrastructure.
- CLIs, scripts, small services, background jobs, test fixtures.
- A cache that survives process restarts without running a separate daemon.
- Multi-process coordination via the process-safe distributed lock.
- Mixed-language systems where a Go process needs to share a cache file with a Python, Java, or Node.js one.

**Not a good fit:**
- A cache shared live across multiple servers/hosts. SQLite is a local file, not a network service; use Redis/Memcached instead.
- Heavy concurrent write throughput from many processes. SQLite's single-writer model will serialize writes and become a bottleneck.
- Pub/sub, streams, or other Redis data structures beyond key-value and counters. lytecache stays small on purpose.

## Where is my data?

By default, `lytecache.New()` stores its file at:

```
<platform cache dir>/lytecache/<project-id>.db
```

resolved via [`os.UserCacheDir`](https://pkg.go.dev/os#UserCacheDir), which already implements the XDG Base Directory spec on Linux, `~/Library/Caches` on macOS, and `%LocalAppData%` on Windows.

`<project-id>` is the first 12 hex characters of the SHA-256 hash of your current working directory's resolved, absolute path. That derivation is identical across the Python, Java, and Node.js implementations too, so every project gets its own file automatically, and a Go process started from the same directory as a Python, Java, or Node.js one ends up sharing its cache.

```go
lytecache.DefaultPath() // (string, error) -- the resolved default location
cache.Path()            // string -- this instance's actual file
```

Override it explicitly:

```go
cache, err := lytecache.New(lytecache.WithPath("/data/cache.db")) // explicit escape hatch
```

```bash
export LYTECACHE_PATH=/data/cache.db  # takes priority over the default
```

## Configuration reference

```go
cache, err := lytecache.New(
    lytecache.WithPath("/data/cache.db"),          // optional; default: lytecache.DefaultPath()
    lytecache.WithNamespace("sessions"),            // logical partition within the database file
    lytecache.WithMaxKeys(100_000),                 // evict when the namespace exceeds this many keys
    lytecache.WithMaxBytes(256<<20),                // evict when the namespace exceeds this many bytes
    lytecache.WithEviction(lytecache.LRU),          // LRU (default), TTLPolicy, Random, NoEviction
    lytecache.WithSweepInterval(60*time.Second),    // 0 disables the goroutine -> opportunistic mode
    lytecache.WithStrict(false),                    // true: return internal read errors instead of a miss
    lytecache.WithLogger(slog.Default()),           // used for non-strict-mode warnings
)
```

- **Eviction policies**: `LRU` (default, evicts least-recently-used), `TTLPolicy` (soonest-to-expire first), `Random`, and `NoEviction` (returns an error wrapping `ErrCacheFull` instead of evicting). LFU is a documented `v0.2` consideration, not yet implemented.
- **Concurrency**: a `*Cache` is safe for concurrent use by multiple goroutines, like `*http.Client`, and safe for concurrent use by multiple OS processes sharing one file.
- **Not yet included** (see `SPEC.md` and the package doc for the full list of deliberate exclusions): networking, pub/sub, clustering, `context.Context` plumbing on every method. These are `v0.2`-or-later considerations, not oversights.

See [SPEC.md](SPEC.md) for the on-disk schema, type codes, and full cross-language semantics, and the package documentation on [pkg.go.dev](https://pkg.go.dev/github.com/lytecache/lytecache-go) for runnable examples of every major feature.

## CLI

Looking for a command-line tool to inspect and manipulate lytecache database files from a shell (like `redis-cli`, but for a file)? See [lytecache-cli](https://github.com/lytecache/lytecache-cli), a separate repo and module that depends on this library's public API rather than living inside it. That keeps this module a pure library, so consumers who only want `import "github.com/lytecache/lytecache-go"` never pull in CLI-only dependencies like `cobra` and `readline`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
