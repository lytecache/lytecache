# litecache

**Redis-like caching with zero infrastructure — no server, just a local SQLite file.**

`litecache` gives you the familiar Redis API surface — `set`/`get`, TTLs, atomic counters, eviction, distributed locks — backed by a single portable SQLite file instead of a daemon. No server to run, no port to open, no client to configure. Just add the dependency and go.

This repository is a **monorepo** containing two independent, same-spec implementations:

| Package | Language | Install | Docs |
|---|---|---|---|
| [`litecache-python/`](litecache-python/) | Python 3.9+ | `pip install litecache` | [litecache-python/README.md](litecache-python/README.md) |
| [`litecache-java/`](litecache-java/) | Java 17+ | `io.litecache:litecache` (Gradle/Maven) | [litecache-java/README.md](litecache-java/README.md) |

They share one on-disk [storage spec](#storage-spec--cross-language-compatibility): a cache file written by one is readable — and, for counters, atomically incrementable — by the other. Everything below is a quick tour; each package's own README is the full reference for that language.

## Quickstart

<table>
<tr><th>Python</th><th>Java</th></tr>
<tr valign="top">
<td>

```python
from litecache import LiteCache

cache = LiteCache()            # no path, no setup
cache.set("user:42", {"name": "Ada"}, ttl=300)
cache.get("user:42")           # {"name": "Ada"}
cache.incr("hits")             # 1
```

</td>
<td>

```java
import io.litecache.LiteCache;
import java.time.Duration;

try (LiteCache cache = new LiteCache()) {
    cache.set("user:42", "Ada", Duration.ofMinutes(5));
    cache.getString("user:42");  // "Ada"
    cache.incr("hits");          // 1
}
```

</td>
</tr>
</table>

That's it in both languages: the first call creates the database file (including any missing parent directories) and applies the schema automatically. There is no `init()`, no migration step, and no server to start.

## Where is my data?

Both implementations resolve the **same default file** for the same project, using the same derivation, so a Python process and a Java process started from the same working directory share one cache automatically:

```
<platform cache dir>/litecache/<project-id>.db
```

- **Linux**: `$XDG_CACHE_HOME/litecache/<project-id>.db`, or `~/.cache/litecache/<project-id>.db`
- **macOS**: `~/Library/Caches/litecache/<project-id>.db`
- **Windows**: `%LOCALAPPDATA%\litecache\<project-id>.db`

`<project-id>` is the first 12 hex characters of the SHA-256 hash of your current working directory's resolved, absolute path — identical in both languages, so every project gets its own file automatically and nothing is left behind in your repo.

Override it the same way in either language:
- Pass an explicit path (`LiteCache("/data/cache.db")` in Python, `.path(Path.of("/data/cache.db"))` on the Java builder).
- Set `LITECACHE_PATH=/data/cache.db` in the environment — takes priority over the default in both.

Both expose the resolved path programmatically (`LiteCache.default_path()` / `cache.path` in Python; `LiteCache.defaultPath()` / `cache.path()` in Java) — the file is never a mystery.

## API at a glance

Both APIs cover the same operations; naming follows each language's conventions (`snake_case` vs `camelCase`, `ttl=` vs `Duration`).

| Operation | Python | Java |
|---|---|---|
| Set / get | `cache.set(key, value, ttl=None)` / `cache.get(key, default=None)` | `cache.set(key, value, ttl)` / `cache.get(key, Type.class)` |
| Typed convenience getters | `get(key, default)` returns native type | `getString` / `getLong` / `getDouble` / `getBytes` |
| Delete / exists | `delete(*keys)` / `exists(key)` | `delete(String... keys)` / `exists(key)` |
| Set-if-absent / set-if-present | `add(key, value, ttl)` / `replace(key, value, ttl)` | `add(key, value, ttl)` / `replace(key, value, ttl)` |
| Atomic swap | `get_set(key, value)` | `getSet(key, value)` |
| Bulk set / get | `set_many(mapping, ttl)` / `get_many(keys)` | `setAll(Map, ttl)` / `getAll(Collection)` |
| Expiration | `expire(key, ttl)` / `persist(key)` / `ttl(key)` / `touch(key, ttl)` | `expire(key, ttl)` / `persist(key)` / `ttl(key)` / `touch(key, ttl)` |
| Atomic counters | `incr(key, amount=1)` / `decr(...)` / `incr_float(key, amount)` | `incr(key)` / `decr(key)` / `incrDouble(key, amount)` |
| Key scanning | `keys(pattern="*")` (lazy iterator) | `keys(pattern)` (lazy `Stream<String>`, GLOB syntax) |
| Clear / stats | `flush()` / `stats()` | `flush()` / `stats()` |
| Maintenance | `vacuum()` / `close()` | `vacuum()` / `close()` |
| Read-through cache | `@cache.memoize(ttl=None)` decorator | `cache.memoize(key, ttl, loader)` |
| Distributed lock | `with cache.lock(name, timeout=30): ...` | `try (CacheLock l = cache.lock(name, timeout)) { ... }` |

Two easy-to-miss details that come up often:
- **`flush()` takes no key/pattern argument** — it always deletes everything in the current namespace. To clear a subset, delete by key or pattern instead: `delete(*keys)` / `cache.delete(String... keys)`, or iterate `keys(pattern)` and delete each match.
- **Python's `ttl` is a `float` in seconds** (`ttl=5000` means ~83 minutes, not 5 seconds); **Java's is an explicit `Duration`** (`Duration.ofSeconds(5)` vs `Duration.ofMillis(5)`), so there's no unit ambiguity there. In both, expiration is enforced both lazily (on every read) and actively (a background sweeper thread) — see each package's README for details.

Both are context managers / `AutoCloseable`:

```python
with LiteCache() as cache:
    cache.set("k", "v")
```

```java
try (LiteCache cache = new LiteCache()) {
    cache.set("k", "v");
}
```

See each package's README for full method signatures, configuration options, and serialization rules (values are stored as portable JSON so complex objects round-trip across both languages — see [Storage spec](#storage-spec--cross-language-compatibility)).

## When to use litecache

**Good fit:**
- Single-node apps (or single-machine, multi-process apps) that want caching, counters, or TTLs with zero infrastructure.
- Scripts, CLIs, notebooks, small web services, background jobs, test fixtures.
- A cache that needs to survive process restarts without running a separate daemon.
- Multi-process coordination via the process-safe distributed lock.
- Mixed-language systems where a Python and a Java process need to share one cache file.

**Not a good fit:**
- A cache shared live across multiple servers/hosts — SQLite is a local file, not a network service. Use Redis/Memcached.
- Heavy concurrent write throughput from many processes — SQLite's single-writer model will serialize writes and become a bottleneck.
- Pub/sub, streams, or other Redis data structures beyond key-value + counters — litecache intentionally stays small.
- Complex queries over cached data — use a real database.

## Storage spec & cross-language compatibility

Both implementations read and write the same schema and value encoding, documented as a versioned spec in each package (kept in sync): [litecache-python/SPEC.md](litecache-python/SPEC.md), [litecache-java/SPEC.md](litecache-java/SPEC.md). In short:

- One SQLite file, WAL mode, `PRAGMA busy_timeout=5000` so cross-process/cross-thread contention waits instead of failing.
- Every value is tagged with a `value_type` code: `0` bytes, `1` UTF-8 string, `2` int (UTF-8 decimal text, not binary — this is what lets `incr`/`decr` be a single atomic SQL UPSERT in both languages), `3` float (UTF-8 decimal text), `4` JSON (the format for any object/dict/list/dataclass/POJO/record).
- Codes `5` (Python pickle) and `6` (Java native serialization) are language-specific escape hatches that are **never** used for the portable path — reading one from the other language raises a clear serialization error instead of returning garbage.
- The zero-config default path derivation (`<project-id>` = SHA-256 of the resolved cwd) is byte-for-byte identical between the two, so both languages land on the same file for the same project directory.

## Developing this repo

Each package builds independently:

```bash
# Python
cd litecache-python
pip install -e ".[dev]"   # or: uv sync
pytest

# Java
cd litecache-java
./gradlew build           # compiles, runs tests, generates javadoc
./gradlew publishToMavenLocal
```

See [litecache-python/README.md](litecache-python/README.md) and [litecache-java/README.md](litecache-java/README.md) for full configuration references, and [litecache-python/CHANGELOG.md](litecache-python/CHANGELOG.md) for release notes.

## License

Apache License 2.0. See [LICENSE](LICENSE).
