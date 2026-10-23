# lytecache

> **Upgrade to 0.2.1 if you're on 0.2.0.** In 0.2.0, once any write failed, later writes could be
> silently lost on every PHP version. See the [CHANGELOG](CHANGELOG.md#021---2026-10-23).

Redis-like caching for PHP with zero infrastructure -- no server, no extension beyond `pdo_sqlite`, just a file. `lytecache` gives you a familiar Redis-shaped API -- `set`/`get`, TTLs, atomic counters, eviction, distributed locks -- backed by a local SQLite file instead of a daemon. It works standalone in any PHP 8.2+ project, and as a native Laravel cache driver: point `CACHE_STORE` at `lytecache` and the `Cache` facade, `Cache::remember()`, and `Cache::lock()` all just work.

## Install

```bash
composer require lytecache/lytecache
```

That's it for plain PHP. Laravel users only need to add one environment variable (below) -- the service provider is auto-discovered.

## Quickstart: plain PHP

```php
use Lytecache\LyteCache;

$cache = new LyteCache(); // no path, no setup -- just works

$cache->set('user:42', ['name' => 'Ada'], ttl: 300);
$user = $cache->get('user:42'); // ['name' => 'Ada']

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

Every method that indicates a specific failure throws a typed exception (all extending `Lytecache\Exceptions\LytecacheException`): `CacheFullException`, `SerializationException`, `SchemaVersionException`, `LockTimeoutException`, `NotNumericException`.

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

## License

Apache License 2.0. See [LICENSE](LICENSE).
