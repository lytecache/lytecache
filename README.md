# litecache

Redis-like caching with zero infrastructure. `litecache` gives you the
familiar Redis API surface -- `set`/`get`, TTLs, atomic counters, eviction --
backed by a local SQLite file instead of a server. No daemon to run, no port
to open, no client library to configure. Just `pip install` and go.

## Install

```bash
pip install litecache
```

## Quickstart

```python
from litecache import LiteCache

cache = LiteCache()                    # no path, no setup -- just works
cache.set("user:42", {"name": "Ada"}, ttl=300)
cache.get("user:42")                   # {"name": "Ada"}
cache.incr("hits")                     # 1
cache.get("missing", "default")        # "default"
```

That's it. The first call to `LiteCache()` creates the database file
(including any missing parent directories) and applies the schema
automatically. There is no `init()`, no migration step, and no server to
start.

## Where is my data?

By default, `LiteCache()` stores its file at:

```
<platform cache dir>/litecache/<project-id>.db
```

- **Linux**: `$XDG_CACHE_HOME/litecache/<project-id>.db`, or `~/.cache/litecache/<project-id>.db`
- **macOS**: `~/Library/Caches/litecache/<project-id>.db`
- **Windows**: `%LOCALAPPDATA%\litecache\<project-id>.db`

`<project-id>` is a short hash of your current working directory, so every
project on your machine automatically gets its own cache file -- two
different apps never collide, and nothing is left behind in your repo.

You can inspect or override this:

```python
LiteCache.default_path()     # -> Path, the resolved default location
cache.path                   # -> Path, this instance's actual file
cache.stats()["path"]        # the file is never a mystery
```

To pin the location explicitly (containers, tests, CI), either pass a path
or set an environment variable:

```python
cache = LiteCache("/data/cache.db")   # explicit escape hatch
```

```bash
export LITECACHE_PATH=/data/cache.db  # takes priority over the default
```

## API

| Method | Description |
|---|---|
| `set(key, value, ttl=None)` | Store a value, optionally with a TTL in seconds. |
| `get(key, default=None)` | Read a value; returns `default` on miss or expiry. Never raises on miss. |
| `delete(*keys)` | Delete keys; returns the number actually deleted. |
| `exists(key)` | Whether a (non-expired) key is present. |
| `add(key, value, ttl=None)` | Set only if absent (atomic `SET NX`). |
| `replace(key, value, ttl=None)` | Set only if present (atomic `SET XX`). |
| `get_set(key, value)` | Atomically swap in a new value, returning the old one. |
| `set_many(mapping, ttl=None)` / `get_many(keys)` | Bulk set/get in a single transaction. |
| `expire(key, ttl)` / `persist(key)` | Set or remove a TTL on an existing key. |
| `ttl(key)` | Seconds remaining (`float`), `-1` if no expiry, `None` if missing. |
| `touch(key, ttl)` | Refresh a key's TTL (sliding expiration). |
| `incr(key, amount=1)` / `decr(key, amount=1)` | Atomic integer counters. |
| `incr_float(key, amount)` | Atomic float counter. |
| `keys(pattern="*")` | Lazily iterate matching keys (glob syntax). |
| `flush()` | Clear the current namespace. |
| `stats()` | Hits, misses, hit rate, key count, size, evictions, path. |
| `vacuum()` / `close()` | Reclaim disk space / shut down cleanly. |
| `memoize(ttl=None)` | Decorator that caches a function's return value. |
| `lock(name, timeout=30, blocking=True, poll=0.05)` | Process-safe context-manager lock. |

`LiteCache` also works as a context manager:

```python
with LiteCache() as cache:
    cache.set("k", "v")
```

## When to use litecache

**Use it when:**
- You want caching, counters, or TTLs in a single-process (or single-machine,
  multi-process) application with no infrastructure to stand up.
- Scripts, CLIs, notebooks, small web services, background jobs.
- You want the cache file to survive restarts without running a separate
  daemon.

**Don't use it when:**
- You need a cache shared live across multiple servers/hosts -- SQLite is a
  local file, not a network service. Use Redis/Memcached instead.
- You have heavy concurrent write throughput from many processes -- SQLite's
  single-writer model will serialize writes and become a bottleneck.
- You need pub/sub, streams, or other Redis data structures beyond a
  key-value store with counters. `litecache` intentionally stays small.

## Configuration reference

```python
LiteCache(
    path=None,             # explicit file path; default: LiteCache.default_path()
    namespace="default",   # logical partition within the database file
    max_keys=None,         # evict when the namespace exceeds this many keys
    max_bytes=None,        # evict when the namespace exceeds this many bytes
    eviction="lru",        # "lru" | "ttl" | "random" | "noeviction"
    sweep_interval=60.0,   # seconds between background maintenance passes;
                           # None disables the thread and does maintenance
                           # opportunistically every ~100 operations instead
    serializer="json",     # only "json" is supported in this version
    strict=False,          # True: raise on internal read errors instead of
                           # degrading to a miss
)
```

- **Eviction policies**: `lru` (default, evicts least-recently-used),
  `ttl` (evicts soonest-to-expire first), `random`, and `noeviction` (raises
  `CacheFullError` instead of evicting). `lfu` is a documented TODO.
- **Serialization**: `str`/`int`/`float`/`bytes` are stored natively and
  round-trip exactly; everything else is JSON-encoded. Pickle is never used.
  Non-JSON-serializable values raise `SerializationError` -- serialize them
  yourself first.
- **Concurrency**: safe across threads (one connection per thread) and
  across processes (SQLite WAL mode). `stats()` counters (hits/misses/etc.)
  are per-process, not shared cluster-wide.

See [SPEC.md](SPEC.md) for the on-disk schema and full semantics, and
[CHANGELOG.md](CHANGELOG.md) for release notes.

## License

Apache 2.0. See [LICENSE](LICENSE).
