# lytecache

Redis-like caching for PHP with zero infrastructure -- no server, no extension beyond `pdo_sqlite`, just a file. `lytecache` gives you a familiar Redis-shaped API -- `set`/`get`, TTLs, atomic counters, eviction, distributed locks -- backed by a local SQLite file instead of a daemon. It works standalone in any PHP 8.2+ project, and as a native Laravel cache driver: point `CACHE_STORE` at `lytecache` and the `Cache` facade, `Cache::remember()`, and `Cache::lock()` all just work.

## Requirements

lytecache has two independent requirements. Upgrading one never fixes the other.

### PHP

**PHP 8.2 or newer** (`"php": "^8.2"`), with the `pdo`, `pdo_sqlite`, `json` and `filter`
extensions. Composer enforces all of this when you install. The official PHP Docker
images include them; on Debian or Ubuntu packages, install `php-sqlite3` for
`pdo_sqlite`.

If Composer's check is bypassed, the library refuses to start instead of failing
mid-request (see [Troubleshooting](#troubleshooting)).

### SQLite

`pdo_sqlite` links your system's SQLite library, which Composer can't see. It only
affects hash operations (`hset`, `hget`, …), which detect what the SQLite supports when
the cache opens:

| System SQLite | Hash mode (`hashMode()`) | Examples |
|---|---|---|
| 3.48 or newer | `json1` (native single SQL statements) | Alpine 3.21+ (3.48.0), Homebrew PHP |
| 3.45 – 3.47 | `json1`, except calls naming a field that contains `"`, which use the fallback | Debian 13 (3.46.1), Ubuntu 24.04 (3.45.1) |
| older than 3.45 | `fallback` (read-modify-write) | Debian 12 (3.40.1), RHEL 9 / Rocky 9 (3.34.1), Alpine 3.19 (3.44.2) |

Both modes give identical results, including under concurrency. The fallback is slower,
and its cost grows with the size of the document, because it re-reads the whole JSON
object in PHP. Measured on SQLite 3.46.1:

| Document | `hset` native / fallback | `hget` native / fallback |
|---|---|---|
| 20 fields | 47 µs / 69 µs | 9 µs / 38 µs |
| 500 fields | 141 µs / 4.4 ms | 24 µs / 4.1 ms |

If you use large hashes on an older distribution, the fallback is worth knowing about.
Every other operation (`set`, `get`, counters, locks, events) is unaffected by the SQLite
version.

## Install

```bash
composer require lytecache/lytecache
```

That's it for plain PHP. Laravel users only need to add one environment variable (below) -- the service provider is auto-discovered.

## Quickstart: plain PHP

```php
use Lytecache\LyteCache;

$cache = new LyteCache(); // no path, no setup -- just works

$cache->set('user:42', ['name' => 'Samson'], ttl: 300);
$user = $cache->get('user:42'); // ['name' => 'Samson']

$hits = $cache->incr('hits'); // atomic, safe across PHP-FPM workers
```

The first call to `new LyteCache()` creates the database file (including any missing parent directories) and applies the schema automatically. There's no `init()`, no migration step, and no server to start.

## Quickstart: Laravel

```bash
composer require lytecache/lytecache
```

```ini
# .env
CACHE_STORE=lytecache
```

```php
use Illuminate\Support\Facades\Cache;

$user = Cache::remember('user:42', 300, fn () => User::find(42));

Cache::lock('import:products', 60)->block(30, function () {
    // only one worker runs this at a time, across every PHP-FPM process
});
```

No `config/cache.php` edits are required -- `LytecacheServiceProvider` merges a sensible default config (file at `storage_path('framework/cache/lytecache.db')`) automatically. Publish the config file if you want to change it:

```bash
php artisan vendor:publish --tag=lytecache-config
```

## API

| Method | Description |
|---|---|
| `set($key, $value, ttl: null)` | Store a value. `ttl` is seconds, `null` = no expiry. |
| `get($key, default: null, class: null)` | Read a value; `default` on miss. `class: SomeClass::class` rehydrates a typed object. |
| `delete(...$keys)` | Delete one or more keys; returns how many actually existed. |
| `has($key)` | Whether a (non-expired) key is present. |
| `add($key, $value, ttl: null)` | Set only if absent (atomic `SET NX`); returns whether it was set. |
| `replace($key, $value, ttl: null)` | Set only if present (atomic `SET XX`); returns whether it was set. |
| `getSet($key, $value)` | Atomically swap in a new value, returning the old one (or `null`). |
| `setMany($entries, ttl: null)` / `getMany($keys)` | Bulk set/get in a single transaction. |
| `expire($key, $ttl)` / `persist($key)` | Set or remove a TTL on an existing key. |
| `touch($key, $ttl)` | Refresh a key's TTL (sliding expiration). |
| `ttl($key)` | Remaining TTL in seconds, `-1` if no expiry, `null` if absent. |
| `incr($key, $amount = 1)` / `decr($key, $amount = 1)` | Atomic integer counters. |
| `incrFloat($key, $amount)` | Atomic float counter. |
| `keys($pattern = '*')` | Lazily iterate matching keys (a `Generator`; GLOB syntax: `*`, `?`, `[...]`). |
| `flush()` | Clear the current namespace. |
| `stats()` | Hits, misses, hit rate, key count, size, evictions, path. |
| `vacuum()` / `close()` | Reclaim disk space / shut down cleanly (idempotent). |
| `remember($key, $ttl, $loader)` | Read-through cache for a computed value. |
| `lock($name, $timeout = 30.0)` | Process-safe distributed lock; release via `$lock->release()` or `$lock->block($callback)`. |
| `maintain()` | Run maintenance (expired-row cleanup) immediately; see below. |
| `hset($key, $field, $value, ttl: null)` / `hsetMany` / `hsetnx` | Atomically set field(s) of a JSON object value. See [Hashes](#hashes). |
| `hget($key, $field, $default)` / `hmget` / `hgetAll` | Read fields without decoding the rest of the document. |
| `hdel` / `hexists` / `hkeys` / `hvals` / `hlen` | Remove, test or list fields. |
| `hincrBy($key, $field, $n)` / `hincrByFloat` | Atomic counters inside a JSON object. |
| `notify($channel, $payload)` / `notifyMany` | Publish a local event (single host, polled, durable). See [Events](#events-local-notification). |
| `watch($channels, fromSeq: null, pollInterval: 0.05)` | Subscribe; `foreach`, `poll($timeout)`, `tryNext()`, `close()`. |
| `saveCursor($name, $seq)` / `loadCursor($name)` | Persist a subscriber position for durable resume. |
| `onChange($listener, ['set', 'delete', 'flush'])` | In-process callback after each committed mutation. |

Every method that indicates a specific failure throws a typed exception (all extending `Lytecache\Exceptions\LytecacheException`): `CacheFullException`, `SerializationException`, `SchemaVersionException`, `LockTimeoutException`, `NotNumericException`.

### Hashes

`hset()` and friends update single fields of a stored JSON object **atomically**. The new
document is computed inside SQLite from the row's current value, so PHP-FPM workers
writing different fields, or `hincrBy()` on the same field, never lose each other's work.
A `get()` → modify → `set()` would.

```php
$cache->hset('user:101', 'status', 'active'); // true: the field was new
$cache->hincrBy('user:101', 'logins');        // atomic counter inside the document
$cache->hget('user:101', 'status');           // 'active'
```

- **A hash is just a JSON value.** `set($key, [...])` is usable with `hget()`, and
  `get()` returns the whole document.
- **Missing keys are created** with no expiry, unless you pass `ttl:`, which applies only
  on creation. Field writes never change an existing key's TTL, and expired keys are
  never revived.
- **Field names are literal top-level keys** (`'a.b'` is not a path).
- **Errors:** a non-object value throws `SerializationException` and is left untouched;
  `hincrBy()` on a non-integer field throws `NotNumericException`; overflowing an int
  throws `OverflowException`, with nothing written.
- **System SQLite matters here.** `pdo_sqlite` uses your system's SQLite, so how hashes
  run depends on the distribution, not on PHP. See [SQLite](#sqlite) under Requirements.
  `$cache->hashMode()` and `stats()->hashMode` tell you which mode is in use. JSONB is
  never used.

### Events (local notification)

Processes sharing one cache file **on one machine** can notify each other. The typical
case is invalidating per-worker in-memory state:

```php
// worker A, after committing a DB change
$cache->delete('balance:42');
$cache->notify('invalidate', ['key' => 'balance:42']);

// a CLI worker on the same host
foreach ($cache->watch('invalidate') as $event) {
    unset($localMemo[$event->payload()['key']]);
}
```

This is **not Redis Pub/Sub**. It is single host only, polled, and durable: events are
rows with a monotonic `seq`, so `watch(..., fromSeq: $cursor)` resumes after a restart.
Delivery is at-least-once, so handlers must be idempotent. PHP has no threads, so polling
happens inside the consuming loop (`foreach`, `poll()`): every 50 ms while events flow,
backing off to about 1 s when idle. Events are kept for `eventRetention` seconds
(default 3600). A subscriber that falls further behind gets a `GapException`, never a
silent skip.

`onChange($listener)` runs listeners synchronously right after each mutation commits, in
the same request. `publishChanges: ['delete', 'flush']` publishes those mutations to the
reserved `__cache__` channel for other processes. Bulk operations emit one record, not
one per key. Full guide: [docs/notifications.md](../docs/notifications.md).

### Why opportunistic maintenance?

The Go/Java/Node.js implementations run a background thread that periodically sweeps expired rows. PHP has no equivalent within a single request/worker, so `LyteCache` instead runs bounded maintenance passes opportunistically, roughly every 100 operations. In Laravel, register the artisan command with the scheduler so a quiet cache still gets swept:

```php
// routes/console.php or App\Console\Kernel::schedule()
Schedule::command('lytecache:maintain')->everyMinute();
```

## When to use lytecache

**Good fit:**
- Single-server PHP apps (including multi-process deployments behind PHP-FPM) that want caching, counters, or TTLs with zero infrastructure.
- CLIs, scripts, small services, background jobs, test fixtures.
- A cache that survives process restarts without running a separate daemon.
- Multi-process coordination via the process-safe distributed lock -- including as a drop-in `Cache::lock()` backend in Laravel.
- Mixed-language systems where a PHP process needs to share a cache file with a Python, Java, Node.js, or Go one.

**Not a good fit:**
- A cache shared live across multiple servers/hosts -- SQLite is a local file, not a network service. Use Redis/Memcached.
- Heavy concurrent write throughput from many processes -- SQLite's single-writer model will serialize writes and become a bottleneck.
- Pub/sub, streams, sessions, or queues -- lytecache intentionally stays small (session and queue drivers are `v0.2` candidates, not yet implemented).

## Where is my data?

By default, `new LyteCache()` stores its file at:

```
<platform cache dir>/lytecache/<project-id>.db
```

`<project-id>` is the first 12 hex characters of the SHA-256 hash of your current working directory's resolved, absolute path -- identical to the Python, Java, Node.js, and Go implementations' derivation, so every project gets its own file automatically, and a PHP process and a Python/Java/Node.js/Go process started from the same directory share one cache.

```php
LyteCache::defaultPath(); // string -- the resolved default location
$cache->path();           // string -- this instance's actual file
```

Override it explicitly:

```php
$cache = new LyteCache(path: '/data/cache.db'); // explicit escape hatch
```

```bash
export LYTECACHE_PATH=/data/cache.db  # takes priority over the default
```

In Laravel, the config's `path` (default: `storage_path('framework/cache/lytecache.db')`) always wins -- the platform-cache-dir default above is a plain-PHP-only convenience.

## Configuration reference

```php
$cache = new LyteCache(
    path: '/data/cache.db',          // optional; default: LyteCache::defaultPath()
    namespace: 'sessions',            // logical partition within the database file
    maxKeys: 100_000,                 // evict when the namespace exceeds this many keys
    maxBytes: 256 * 1024 * 1024,      // evict when the namespace exceeds this many bytes
    eviction: Eviction::LRU,          // LRU (default), TTL, Random, NoEviction
    sweepInterval: 60.0,              // minimum seconds between opportunistic maintenance passes
    strict: false,                    // true: throw on internal read errors instead of a miss
);
```

Laravel equivalent, in `config/lytecache.php` (publish with `php artisan vendor:publish --tag=lytecache-config`, or set directly in `config/cache.php`'s `stores.lytecache`):

```php
return [
    'driver' => 'lytecache',
    'path' => storage_path('framework/cache/lytecache.db'),
    'namespace' => 'default',
    'max_keys' => null,
    'max_bytes' => null,
    'eviction' => 'lru', // lru | ttl | random | noeviction
    'sweep_interval' => 60.0,
    'strict' => false,
];
```

- **Eviction policies**: `LRU` (default, evicts least-recently-used), `TTL` (soonest-to-expire first), `Random`, and `NoEviction` (throws `CacheFullException` instead of evicting). LFU is a documented `v0.2` consideration, not yet implemented.
- **Concurrency**: a `LyteCache` instance is safe for concurrent use by multiple OS processes sharing one file, including PHP-FPM workers.
- **Not yet included** (see [SPEC.md](SPEC.md) for the full list of deliberate exclusions): networking, pub/sub, clustering, session/queue drivers. These are `v0.2`-or-later considerations, not oversights.

See [SPEC.md](SPEC.md) for the on-disk schema, type codes, and full cross-language semantics.

## Troubleshooting

### Installed but crashes with a fatal error

On a PHP older than 8.2 you'll now see this at startup, from `vendor/autoload.php`:

```
RuntimeException: lytecache requires PHP 8.2 or newer; this process is running PHP 8.1.34.
Composer should have refused this install, so it was most likely installed with
--ignore-platform-reqs or --ignore-platform-req=php, or the root composer.json sets
config.platform.php.
```

(Older releases instead crashed later, mid-request, with a parse error inside
`vendor/lytecache/lytecache/src`.) Composer refuses this install unless something
bypassed its check. There are three ways that happens; check each:

1. **`--ignore-platform-reqs` or `--ignore-platform-req=php`** in a Dockerfile, CI
   workflow or deploy script. Search for it:
   `grep -rn -- '--ignore-platform-req' Dockerfile* .github/ deploy/ 2>/dev/null`.
   Remove it. If one build genuinely needs it, narrow it to the single extension in
   question (`--ignore-platform-req=ext-something`) and leave the PHP check on.
2. **`config.platform.php` in your app's `composer.json`.** It makes Composer resolve
   for a PHP you aren't running: `composer config platform.php` prints it if set. Set it
   to the PHP you deploy on, or remove it. (Composer's own startup check usually catches
   this case first, with "Composer detected issues in your platform".)
3. **A `composer.lock` resolved on a different PHP**, then installed with the check
   bypassed. Run `composer check-platform-reqs` on the machine that runs the app: it
   lists every requirement the installed packages have that this PHP doesn't meet.

The fix is to run PHP 8.2 or newer, or to stay on a lytecache release that supports your
PHP. Don't silence the error: below 8.2 the library cannot run.

## License

Apache License 2.0. See [LICENSE](LICENSE).
