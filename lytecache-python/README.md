# lytecache

Redis-like caching with zero infrastructure. `lytecache` gives you the
familiar Redis API surface -- `set`/`get`, TTLs, atomic counters, eviction --
backed by a local SQLite file instead of a server. No daemon to run, no port
to open, no client library to configure. Just `pip install` and go.

## Install

```bash
pip install lytecache
```

## Quickstart

```python
from lytecache import LyteCache

cache = LyteCache()                    # no path, no setup -- just works
cache.set("user:42", {"name": "Samson"}, ttl=300)
cache.get("user:42")                   # {"name": "Samson"}
cache.incr("hits")                     # 1
cache.get("missing", "default")        # "default"
```

That's it. The first call to `LyteCache()` creates the database file
(including any missing parent directories) and applies the schema
automatically. There is no `init()`, no migration step, and no server to
start.

## Where is my data?

By default, `LyteCache()` stores its file at:

```
<platform cache dir>/lytecache/<project-id>.db
```

- **Linux**: `$XDG_CACHE_HOME/lytecache/<project-id>.db`, or `~/.cache/lytecache/<project-id>.db`
- **macOS**: `~/Library/Caches/lytecache/<project-id>.db`
- **Windows**: `%LOCALAPPDATA%\lytecache\<project-id>.db`

`<project-id>` is a short hash of your current working directory, so every
project on your machine automatically gets its own cache file -- two
different apps never collide, and nothing is left behind in your repo.

You can inspect or override this:

```python
LyteCache.default_path()     # -> Path, the resolved default location
cache.path                   # -> Path, this instance's actual file
cache.stats()["path"]        # the file is never a mystery
```

To pin the location explicitly (containers, tests, CI), either pass a path
or set an environment variable:

```python
cache = LyteCache("/data/cache.db")   # explicit escape hatch
```

```bash
export LYTECACHE_PATH=/data/cache.db  # takes priority over the default
```

## API

| Method | Description |
|---|---|
| `set(key, value, ttl=None)` | Store a value, optionally with a TTL in seconds. |
| `get(key, default=None, cls=None)` | Read a value; returns `default` on miss or expiry. Never raises on miss. `cls` reconstructs a dataclass/plain object from a stored JSON value. |
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
| `hset(key, field, value, ttl=None)` / `hset_many(key, mapping, ttl=None)` / `hsetnx(...)` | Atomically set field(s) of a JSON object. See [Hashes](#hashes). |
| `hget(key, field, default=None, cls=None)` / `hmget(key, fields)` / `hgetall(key)` | Read field(s) without decoding the rest of the document. |
| `hdel(key, *fields)` / `hexists` / `hkeys` / `hvals` / `hlen` | Remove fields, test or list them. |
| `hincrby(key, field, amount=1)` / `hincrbyfloat(key, field, amount)` | Atomic counters *inside* a JSON object. |
| `notify(channel, payload=None)` / `notify_many(events)` | Publish a local event (single host, polled, durable). See [Events](#events-local-notification). |
| `watch(*channels, from_seq=None, poll_interval=0.05)` | Subscribe to local events; iterate, `poll()`, `try_next()`, `close()`. |
| `save_cursor(name, seq)` / `load_cursor(name)` | Persist a subscriber's position for durable resume. |
| `on_change(listener, events=...)` | In-process callback after each committed mutation. |
| `keys(pattern="*")` | Lazily iterate matching keys (glob syntax). |
| `flush()` | Clear the current namespace. |
| `stats()` | Hits, misses, hit rate, key count, size, evictions, path, hash mode. |
| `vacuum()` / `close()` | Reclaim disk space / shut down cleanly. |
| `memoize(ttl=None)` | Decorator that caches a function's return value. |
| `lock(name, timeout=30, blocking=True, poll=0.05)` | Process-safe context-manager lock. |

`LyteCache` also works as a context manager:

```python
with LyteCache() as cache:
    cache.set("k", "v")
```

## Events (local notification)

Processes sharing one cache file **on one machine** can tell each other that
something happened. The typical case is invalidating in-memory state across
workers:

```python
# worker A, after committing a DB change
cache.delete("balance:42")
cache.notify("invalidate", {"key": "balance:42"})

# workers B..N
for event in cache.watch("invalidate"):
    local_memo.pop(event.payload["key"], None)
```

This is **not Redis Pub/Sub**. It is single host only (it will not work
across machines), polled (latency about the 50 ms poll interval, up to
about 1 s after an idle spell), and durable: events are rows with a
monotonic `seq`, so a restarted subscriber resumes with
`watch(..., from_seq=cursor)`. Delivery is at-least-once, so handlers must
be idempotent. Events are kept for `event_retention` (default 1 hour), and
a subscriber that falls further behind gets a `GapError` rather than a
silent skip. It is opt-in: a cache that never uses it creates no table and
starts no thread.

For reactions within one process, `cache.on_change(listener)` is immediate
and free. `LyteCache(publish_changes={"delete", "flush"})` publishes those
mutations to the `__cache__` channel for other processes. Bulk operations
emit one record, not one per key. Full guide:
[docs/notifications.md](../docs/notifications.md).

## Hashes

Hash operations read and change single fields of a stored JSON object. The
main reason to use them is **atomicity**. The obvious way to update one
field races:

```python
# worker A                              # worker B
doc = cache.get("user:101")             doc = cache.get("user:101")
doc["status"] = "active"                doc["plan"] = "pro"
cache.set("user:101", doc)              cache.set("user:101", doc)   # silently erases A's change
```

`hset` does the update inside SQLite, in one statement computed from the
row's *current* value, so concurrent writers to different fields, or
`hincrby` calls on the same field, never lose each other's work, even
across processes:

```python
cache.hset("user:101", "status", "active")   # True: the field was new
cache.hset("user:101", "status", "paused")   # False: it replaced a value
cache.hincrby("user:101", "logins")          # atomic counter inside the document
cache.hget("user:101", "status")             # "paused"
cache.hgetall("user:101")                    # the whole object, as a dict
```

- **Hashes are just JSON values.** There is no separate type: anything
  stored with `set(key, {...})` works with `hget`, and `get()` on a hash
  returns the whole document.
- **Missing keys are created** (like Redis `HSET`) as a new object with **no
  expiry**, unless you pass `ttl=`. That TTL only applies when the call
  creates the key.
- **Field writes keep the TTL.** `hset` on a key with a TTL leaves the
  countdown alone. Only `expire`/`touch`/`persist` change it.
- **Expired keys stay dead.** Reads treat them as missing, and a write
  starts a fresh object rather than reviving old fields.
- **Field names are taken literally.** `"a.b"`, `"a[0]"` and `'q"x'` are
  top-level keys, not paths into nested data.
- **Type-checked.** Hash operations on a non-object value (a string, a
  number, bytes, a JSON array) raise `SerializationError` and leave the
  value untouched. `hincrby` on a non-integer field raises `TypeError`.
  Going past the 64-bit integer range raises `OverflowError`, and nothing
  is written.
- Removing the last field with `hdel` leaves `{}` in place. Unlike Redis,
  it does not delete the key.

Lock-wise, a hash write is an ordinary write: it takes the same write lock
and `busy_timeout` as `set`, so many single-field writes are not cheaper in
lock terms than whole writes. What you gain is atomicity, plus less data
moved when documents are large. In a measured comparison with
get-modify-set, it was about the same speed on a 2 KB, 50-field document,
and about 1.8× faster on a 50 KB one.

**SQLite version.** Hashes use SQLite's built-in JSON functions (3.38+),
which Python's bundled SQLite has. On an older system SQLite, lytecache
falls back to a read-modify-write under an exclusive write lock: still
atomic, just slower. It logs one warning, and `stats()["hash_mode"]`
reports `"json1"` or `"fallback"`. Values are always stored as plain JSON
text, never SQLite's binary JSONB format, so other lytecache
implementations can read them.

## Serialization

`str`/`int`/`float`/`bytes`/`bool` round-trip exactly. Anything else --
`dict`, `list`, dataclasses, plain objects -- is JSON-encoded, and reads back
as a plain `dict`/`list` by default:

```python
from dataclasses import dataclass

@dataclass
class Address:
    city: str
    zip_code: str

@dataclass
class Person:
    name: str
    age: int
    address: Address

cache.set("p:1", Person("Samson", 30, Address("London", "E1")))

cache.get("p:1")               # {"name": "Samson", "age": 30, "address": {...}} -- plain dict
cache.get("p:1", cls=Person)   # Person(name="Samson", age=30, address=Address(...)) -- typed
```

`tuple` values are JSON-encoded too and come back as `list` -- there's no
tuple type in JSON.

Values that can't be represented as JSON raise `SerializationError` by
default (`serializer="auto"`). If you genuinely need to cache an arbitrary
Python object, opt into pickling explicitly:

```python
cache = LyteCache(serializer="pickle")   # falls back to pickle when JSON can't represent a value
```

**Security note:** unpickling can execute arbitrary code. The default
(`serializer="auto"`) and `serializer="json"` never write or read pickled
data, so this doesn't apply to them. Only `serializer="pickle"` (or
`serializer="auto"` with `allow_pickle=True`) can read pickled values --
treat a cache file that might contain pickled data like application code,
and never open one from an untrusted source.

## When to use lytecache

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
  key-value store with counters. `lytecache` intentionally stays small.

## Configuration reference

Events add these constructor options: `publish_changes` (set of `"set"`,
`"delete"`, `"flush"`, default none), `event_retention` (seconds, default
3600), `event_max_count` (default unlimited) and `change_key_cap` (default
100).


```python
LyteCache(
    path=None,             # explicit file path; default: LyteCache.default_path()
    namespace="default",   # logical partition within the database file
    max_keys=None,         # evict when the namespace exceeds this many keys
    max_bytes=None,        # evict when the namespace exceeds this many bytes
    eviction="lru",        # "lru" | "ttl" | "random" | "noeviction"
    sweep_interval=60.0,   # seconds between background maintenance passes;
                           # None disables the thread and does maintenance
                           # opportunistically every ~100 operations instead
    serializer="auto",     # "auto" | "json" (strict, no pickle) | "pickle"
    strict=False,          # True: raise on internal read errors instead of
                           # degrading to a miss
    allow_pickle=False,    # "auto" mode only: allow reading pickled values
                           # written by a serializer="pickle" cache
)
```

- **Eviction policies**: `lru` (default, evicts least-recently-used),
  `ttl` (evicts soonest-to-expire first), `random`, and `noeviction` (raises
  `CacheFullError` instead of evicting). `lfu` is a documented TODO.
- **Serialization**: see the [Serialization](#serialization) section above.
  `serializer="auto"` (the default) and `serializer="json"` never write or
  read pickled data; `serializer="pickle"` opts into pickling as a fallback
  for values JSON can't represent.
- **Concurrency**: safe across threads (one connection per thread) and
  across processes (SQLite WAL mode). `stats()` counters (hits/misses/etc.)
  are per-process, not shared cluster-wide.

See [SPEC.md](SPEC.md) for the on-disk schema and full semantics, and
[CHANGELOG.md](CHANGELOG.md) for release notes.

## License

Apache 2.0. See [LICENSE](LICENSE).
