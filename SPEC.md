# lytecache file format spec (schema version 2)

This document describes the on-disk format of a lytecache database file. It
is a public, versioned spec: any tool or library, in any language, that reads
and writes to this schema and follows these semantics should be able to
interoperate with a lytecache-created file.

## File

A lytecache database is a single SQLite file. It is opened with:

```
PRAGMA busy_timeout=5000;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
```

`busy_timeout` is set before `journal_mode`, so that the WAL switch itself
waits out contention from other connections instead of failing immediately.
WAL mode means the directory alongside the `.db` file will also contain
`-wal` and `-shm` sidecar files during normal operation; these are part of
the database and should not be deleted while the database is in use.

## Tables

```sql
CREATE TABLE IF NOT EXISTS cache (
  key            TEXT    NOT NULL,
  namespace      TEXT    NOT NULL DEFAULT 'default',
  value          BLOB    NOT NULL,
  value_type     INTEGER NOT NULL DEFAULT 0,  -- 0=bytes 1=str 2=int 3=float 4=json
  created_at     INTEGER NOT NULL,            -- unix ms
  expires_at     INTEGER,                     -- unix ms, NULL = no expiry
  last_accessed  INTEGER NOT NULL,            -- unix ms
  access_count   INTEGER NOT NULL DEFAULT 0,
  size_bytes     INTEGER NOT NULL,
  PRIMARY KEY (namespace, key)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cache_lru ON cache(namespace, last_accessed);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
```

`meta` holds a row `('schema_version', '1')`, written the first time
the database is created. It may also hold `('events_version', '1')` (see
"Events") and subscriber cursors named `cursor:<namespace>:<name>`. A reader/writer that encounters a
`schema_version` greater than the highest version it supports MUST refuse to
open the file (lytecache raises `SchemaVersionError`). Readers may safely
open files with an equal or lower schema version.

### Column semantics

- **key / namespace**: together form the primary key. `namespace` is a
  caller-chosen logical partition within one file (lytecache's `namespace=`
  constructor argument); there is no cross-namespace uniqueness constraint.
- **value / value_type**: see "Serialization" below.
- **created_at**: unix ms timestamp of the row's most recent *write*
  (`set`/`add`/`replace`/`get_set`/`set_many` all refresh it — it does not
  track "first written", it tracks "last written"). In-place updates —
  counters and hash field writes — leave it unchanged, except when they
  create the row.
- **expires_at**: unix ms absolute expiry time, or `NULL` for no expiry. A
  row with `expires_at <= now` is expired.
- **last_accessed**: unix ms timestamp of the most recent successful `get`
  (or equivalent read). Used only for LRU eviction ordering. Because it is
  millisecond-resolution, operations that occur within the same millisecond
  are not distinguishable by this column alone; ties are broken by an
  unspecified but stable order (in practice, primary key order, since `cache`
  is a `WITHOUT ROWID` table clustered on `(namespace, key)`).
- **access_count**: incremented on each read; informational only, not used
  by any eviction policy in this version.
- **size_bytes**: `LENGTH(value)` in bytes, kept in sync on every write, used
  to enforce `max_bytes` without a full table scan.

## TTL semantics

- `ttl=None` means no expiry (`expires_at = NULL`).
- `ttl<=0` (including `0` and negative values) produces an `expires_at` at or
  before the current time, so the key reads as immediately expired. It is
  still written to the table; expiration is judged purely by comparing
  `expires_at` to the current time at read time, and by the background
  sweeper.
- **Lazy expiration**: every read path (`get`, `get_many`, `exists`, `ttl`,
  `keys`, ...) treats a row with `expires_at <= now` as if it were absent,
  without deleting it inline.
- **Active expiration**: a background sweeper (or, when
  `sweep_interval=None`, opportunistic maintenance every ~100 operations)
  periodically `DELETE`s expired rows in bounded batches (500 rows/pass).
- `ttl(key)` returns `None` for a missing *or expired* key, `-1` for a key
  with no expiry, and the remaining seconds (float) otherwise.

## Eviction order

Eviction only runs when `max_keys` and/or `max_bytes` is configured, and is
skipped entirely for `eviction="noeviction"` (which instead raises
`CacheFullError` when a *new* key would grow the namespace past its limit;
updates to existing keys never trigger it). When over capacity, rows are
deleted oldest-target-first according to the policy's `ORDER BY`:

| policy | order |
|---|---|
| `lru` (default) | `last_accessed ASC` |
| `ttl` | `(expires_at IS NULL) ASC, expires_at ASC` (soonest-to-expire first; no-expiry rows evicted last) |
| `random` | `RANDOM()` |
| `noeviction` | never evicts; raises `CacheFullError` instead |

`lfu` (least-frequently-used, using `access_count`) is a documented TODO for
a future schema-compatible release.

## Serialization / value_type codes

| code | type | encoding | portable |
|---|---|---|---|
| 0 | `bytes` | stored as-is | yes |
| 1 | `str` | UTF-8 encoded | yes |
| 2 | `int` | decimal ASCII text, e.g. `b"42"`, `b"-3"` | yes |
| 3 | `float` | `repr(float)`-style decimal ASCII text, e.g. `b"3.14"` | yes |
| 4 | `json` | UTF-8 encoded JSON (see below) | yes |
| 5 | `pickle` | Python `pickle.dumps(..., protocol=HIGHEST_PROTOCOL)` | **no** (Python-only) |
| 6 | *(reserved)* | a Java-native serialization format | **no** (Java-only) |

Only codes 0–4 are cross-language portable and are what a from-scratch
implementation in another language (e.g. the companion Java implementation)
should read and write. Codes 5 and 6 are opt-in, language-specific escape
hatches: lytecache never writes code 6, and only writes code 5 when the
cache is explicitly opened with `serializer="pickle"`. A reader that
encounters a code it does not recognize or does not support (including
6, or any value outside 0–6) MUST raise an error naming the code, rather
than returning the raw blob silently.

### Native types (codes 0–3)

`str`/`int`/`float`/`bytes` round-trip exactly. `bool` is stored as JSON
(code 4, `b"true"`/`b"false"`), not as an int, since `bool` is a subtype of
`int` in Python and would otherwise lose its boolean identity on read-back.

Atomic counters (`incr`/`decr`/`incr_float`) operate directly on the decimal
text form of codes 2/3 via a single SQL `UPSERT`, using `CAST(value AS TEXT)`
arithmetic; this is why int/float values are stored as decimal text rather
than SQLite's native binary integer/real encoding. A counter operation
against a row whose `value_type` is not numeric-compatible fails the
UPSERT's `WHERE` guard and the caller receives a Python `TypeError` — no
partial write occurs.

### JSON encoding (code 4)

- Encoded with compact separators and `ensure_ascii=False` (i.e. UTF-8
  bytes, not `\uXXXX` escapes) — equivalent to Python's
  `json.dumps(value, ensure_ascii=False, separators=(",", ":"))`.
- `dict` and `list` values are encoded directly and round-trip as
  `dict`/`list`.
- `tuple` is encoded as a JSON array and round-trips as a `list` — there is
  no tuple type in JSON, so this is a one-way conversion; document this to
  your users if you re-implement the encoder.
- Dataclasses (or the equivalent structured-record type in your language)
  are encoded as a JSON object of their fields, recursively (nested
  dataclasses become nested objects). In Python this is
  `dataclasses.asdict(value)`.
- Plain objects with an instance attribute dict (Python: anything with
  `__dict__`, i.e. not using `__slots__`) are encoded as a JSON object of
  their instance attributes (Python: `vars(value)`).
- A value that is none of the above and cannot be represented as JSON
  raises `SerializationError` under `serializer="auto"` or `"json"`; under
  `serializer="pickle"` it falls back to code 5 instead.
- On read, a JSON value decodes to a plain `dict`/`list`/`str`/`int`/
  `float`/`bool`/`None` by default. A Python-specific extension,
  `get(key, cls=SomeType)`, can additionally reconstruct a specific type
  from the JSON object: if `SomeType` is a dataclass, its fields are
  reconstructed recursively (nested dataclass-typed fields, resolved via
  type hints, are rebuilt from their nested JSON objects); otherwise the
  type is instantiated as `SomeType(**data)`. This reconstruction step is a
  read-time convenience only — the bytes on disk are the same portable
  JSON either way.

### Pickle (code 5) — Python-only, opt-in

Only ever written when the cache is constructed with `serializer="pickle"`,
and only for values that codes 0–4 cannot represent. Reading a code-5 value
back requires either `serializer="pickle"`, or `serializer="auto"` with
`allow_pickle=True` explicitly passed; `serializer="json"` always refuses to
read it. **Security note:** unpickling can execute arbitrary code during
deserialization. `serializer="auto"` (the default) and `serializer="json"`
never write or read pickled data, so this risk does not apply to them.
Treat any cache file that might contain pickled data like application code:
never open one from an untrusted source.

### Foreign codes (6+)

Code 6 is reserved for other languages' own native serialization formats
(e.g. Java). lytecache never writes it. If a from-scratch implementation in
another language wants to store values its own native serializer can
represent but JSON cannot, it should use its own reserved code in this
range and document it here, following the same rule: never silently return
raw bytes for a code your reader doesn't understand -- always raise.

## Schema versions

**Schema version 2** is version 1 plus the optional `events` table described
under "Events". It is purely additive, so the on-disk marker is **not**
bumped: `meta.schema_version` stays `'1'`. Every released lytecache refuses
to open a file whose `schema_version` is newer than its own, and
v1-era libraries must keep opening these files for ordinary cache
operations. That is the compatibility rule:

- A v1 file opens unchanged. It gains the `events` table (and an
  `('events_version', '1')` row in `meta`) only the first time a cache on it
  uses `notify` or `watch`. Nothing else in the file changes.
- A v2 file (one with an `events` table) is fully readable and writable by a
  v1-era library for every ordinary cache operation; such a library ignores
  the `events` table and the extra `meta` rows. Its sweeper does not prune
  events, which only means events wait for the next v2-aware sweep.
- Readers detect events support by the presence of the `events` table, or
  the `events_version` row in `meta`, never by `schema_version`.

## Events (local notification: `notify` / `watch`)

**Single host only. Polled, not pushed. Durable.** Processes that share one
cache file *on one machine* can publish events and watch for them. This is
not Redis Pub/Sub. Subscribers on another machine never receive anything,
and SQLite locking is unreliable on network filesystems, so it cannot be
stretched across hosts. Delivery latency is bounded by the subscriber's poll
interval (default 50 ms), not sub-millisecond. Events are rows with a
monotonic sequence number, so a subscriber that was down resumes from its
last seen sequence instead of missing everything. In that respect this is
closer to Redis Streams than to Pub/Sub, but without consumer groups.

### Table

Created lazily (`CREATE ... IF NOT EXISTS`), the first time a cache uses
`notify` or `watch`, never at open:

```sql
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,  -- monotonic, the cursor
  channel    TEXT    NOT NULL,
  namespace  TEXT    NOT NULL DEFAULT 'default',
  payload    BLOB,
  value_type INTEGER NOT NULL DEFAULT 4,          -- same codes as cache.value_type
  created_at INTEGER NOT NULL                     -- unix ms
);
CREATE INDEX IF NOT EXISTS idx_events_seq_channel ON events(channel, seq);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
INSERT OR IGNORE INTO meta (k, v) VALUES ('events_version', '1');
```

- `payload` / `value_type` follow **exactly** the "Serialization" rules for
  cached values, so any implementation can read any other's events. A
  `NULL` payload reads as null/None. Implementations write a null payload as
  JSON `null` (code 4).
- `namespace` is the publishing cache's namespace.
- `AUTOINCREMENT` means a `seq` is never reused, even after rows are
  deleted. `sqlite_sequence.seq` for `name = 'events'` is the last `seq`
  ever assigned.

### Publishing

- `notify(channel, payload)` is a single `INSERT`, autocommitted, which
  returns the assigned `seq`. `notify_many([(channel, payload), ...])`
  inserts several events in one `BEGIN IMMEDIATE` transaction.
- An event is visible only once committed. Publishing takes the same
  writer lock as `set`, so a high event rate slows cache writes; batch with
  `notify_many`. Callers SHOULD publish *after* their own database
  transaction commits, never from inside one they might roll back.
- Channel names are non-empty, contain no `*`, and do not start with `__`.
  Names starting with `__` are reserved for lytecache (see `__cache__`
  below). Implementations reject violations.

### Watching

`watch(patterns..., from_seq, poll_interval)` returns a subscription.

- **Matching.** A pattern is an exact channel name, or ends in `*` to match
  every channel starting with the text before it (`cache:invalidate:*`).
  `*` alone matches every channel, `__cache__` included. A `*` anywhere else
  is a literal character. That is the entire pattern language. A
  subscription sees only events whose `namespace` equals the watching
  cache's namespace.
- **Starting point.**
  - `from_seq = None` (absent): only events published after `watch`
    returns. The cursor starts at the last assigned seq.
  - `from_seq = N > 0`: events with `seq > N`.
  - `from_seq = 0`: everything still retained. The cursor starts just
    before the oldest retained event, so this never reports a gap.
- **Polling.** One background thread/goroutine/timer per subscription, on
  its own connection, which is read-only (`PRAGMA query_only=1` or the
  driver's read-only mode) and never takes the write lock. Each poll is one
  read transaction, which keeps the gap check and the fetch on the same
  snapshot:

  ```sql
  SELECT seq FROM sqlite_sequence WHERE name = 'events';      -- gap check
  SELECT min(seq) FROM events;                                 -- gap check
  SELECT seq, channel, namespace, payload, value_type, created_at
    FROM events WHERE seq > :cursor AND namespace = :ns AND <channel match>
   ORDER BY seq LIMIT 500;
  ```

  - A full batch of 500 is followed immediately by another poll.
  - **Adaptive backoff.** Poll at `poll_interval` while events flow. After
    each empty poll, double the wait, up to 1 s (or `poll_interval` if
    that is larger). Snap back to `poll_interval` as soon as a poll
    returns events.
  - The first event after an idle period is therefore seen within the
    current backoff (≤ 1 s).
  - An implementation MAY also wake its own subscriptions immediately when
    it publishes in the same process. The table remains the only source of
    truth.
  - SQLite update hooks MUST NOT be used for cross-process delivery: they
    fire only for the connection that made the change.
- **Languages without threads** (PHP) poll inside the consuming call
  instead: iterating the subscription, or calling its poll method, runs
  the same query with the same backoff, and nothing runs between calls.
- **No thread before `watch`.** Publishing alone starts nothing. Closing a
  subscription stops its poller and closes its connection. Closing the
  cache closes all its subscriptions. `close()` is idempotent.

### Delivery semantics

- **At-least-once from the subscriber's cursor.** A subscriber that crashes
  after handling an event but before persisting its cursor sees that event
  again on resume. Handlers MUST be idempotent.
- **Ordering is total per file**, by `seq`, across all channels.
- **Fan-out.** Every subscriber with a matching pattern sees every event.
  There are no consumer groups and no exclusive delivery.
- **Cursors.** The subscriber owns its cursor. The convenience helpers
  `save_cursor(name, seq)` / `load_cursor(name)` store it in
  `meta` under the key `cursor:<namespace>:<name>`, value the decimal seq.
  Each save is a write.

### Retention and gaps

- The maintenance sweeper deletes events by age (`event_retention`,
  default 1 hour) and optionally by count (`event_max_count`, keep the
  newest N), in batches of 500 alongside expired cache rows. An event past
  retention is gone **whether or not anyone consumed it**: a subscriber
  offline longer than the retention window loses those events, so
  retention is the knob for how much downtime is tolerable.
- Retention always deletes a **prefix** of the seq range: everything up to
  the newest event that is past retention (or beyond the count cap), never
  a hole in the middle, even if clocks disagree.
- **Gap rule.** Let `last` = the last assigned seq (0 if none) and
  `oldest` = `min(seq)` (or `last + 1` if the table is empty). A
  subscription whose cursor `c` has `c < oldest − 1` has lost events
  `c+1 .. oldest−1`. One with `c > last` is ahead of the file (the file was
  recreated). Either case MUST be reported explicitly on the first read,
  as `GapError` with the requested seq, the oldest retained seq, and the
  resume point `oldest − 1`, and the subscription ends. It is checked on
  every poll, so a subscriber that falls behind while running is told too.
  Never resume silently at the wrong place. To accept the loss, watch
  again with `from_seq = resume_seq`.

### Change records, `on_change` and `publish_changes`

Two distinct mechanisms report cache mutations:

1. **In-process listeners** (`on_change(listener, events)`): a callback
   registry inside one process, with no SQLite involvement and effectively
   zero latency. Listeners are called only after the mutation's transaction
   commits (a rolled-back `set_many` notifies nobody). They run on a
   dispatch thread (or the language's equivalent) that holds no cache lock,
   so a slow listener never stalls writers, and a listener may write to the
   cache. A listener that throws is logged and skipped. The write already
   committed, and the remaining listeners still run. Records produced by a
   listener's own writes carry depth + 1. Beyond depth 8 they are dropped
   with a warning, which stops infinite recursion. (PHP, which has no
   threads, calls listeners synchronously right after the mutating call's
   COMMIT, outside any transaction.) No dispatch thread exists
   until the first listener registers, and all listeners are dropped on
   `close()`. After `unsubscribe()` returns, that listener is never called
   again.
2. **Cross-process auto-publish** (`publish_changes={...}`, off by
   default): each matching mutation also writes one event to the reserved
   channel `__cache__`. This is the polled, durable `notify`/`watch`
   mechanism. It is **not** zero-latency, and it costs one event row
   written under the write lock per mutation. It is written right after
   the mutation commits, so a crash between the two can drop that event.

Operation names, shared by both:

| op | emitted by | when |
|---|---|---|
| `set` | every write that changes a value: `set`, `set_many`, `add`, `replace`, `get_set`, counters, hash writes | the write changed something (`add`/`replace`/`hsetnx` that did nothing emit nothing) |
| `delete` | `delete(keys...)` | at least one key was removed |
| `flush` | `flush()` | at least one row was removed |

TTL expiry, eviction, and lytecache's own lock keys are never reported.

**One record per call, never one per key.** A `set_many` of 1,000 keys is
one record. The record, which is also the `__cache__` event's JSON payload:

```json
{"op": "set", "namespace": "default", "keys": ["a", "b"], "count": 2, "truncated": false}
```

- `keys` lists the keys the call named, deduplicated and in call order. If
  there are more than `change_key_cap` (default 100), `keys` is `null` and
  `truncated` is `true`.
- `count` is the number of keys written, or for `delete`/`flush` the
  number of rows removed.
- `flush` always has `keys: null, truncated: true`.
- Subscribers should treat a truncated record as "a bulk change happened,
  refetch what you care about": invalidate by namespace or prefix, not key
  by key.

## Hashes (field operations on JSON objects)

`hset`/`hget`/`hincrby` and friends read and modify individual top-level
fields of a stored JSON object. **This needs no schema change and does not
bump `schema_version`:** a hash is an ordinary `value_type` 4 row whose JSON
root is an object, not a separate type or storage format. Anything written
with `set(key, {...})` is usable with `hget`, `get()` on a hash returns the
whole document, and a file containing hashes is readable by any
implementation that understands code 4.

### Atomicity: why these exist

Read-modify-write in application code loses updates: two workers read the
same document, each changes a different field, and the second `set`
silently overwrites the first. Hash writes avoid this by computing the new
document **inside SQLite from the row's current value**, e.g.

```sql
UPDATE cache SET value = CAST(json_set(CAST(value AS TEXT), :path, json(:v)) AS BLOB), ...
 WHERE namespace = :ns AND key = :key AND (expires_at IS NULL OR expires_at > :now) ...
```

SQLite serializes the statement, so no update is lost. An implementation
MUST NOT implement a hash write as "read the document into the application,
change it, write it back" — except in the fallback mode below, where that
sequence is held under one `BEGIN IMMEDIATE` write lock. Where an exact
return value needs more than one statement (whether `hset` created the
field, how many fields `hdel` removed), use guarded writes inside one
`BEGIN IMMEDIATE` transaction (e.g. "set the field WHERE it is absent", then
"set it WHERE the key is a live object") rather than a read that a later
write depends on.

### Paths

A field name is always **bound as a parameter**, never interpolated into SQL
text, and turned into a JSON path in SQL as

```sql
'$."' || replace(replace(:field, '\', '\\'), '"', '\"') || '"'
```

(backslashes, then double quotes, escaped). This addresses exactly one
top-level key for any field name, including ones containing `.`, `[`, `]`,
`"`, `\`, spaces, non-ASCII characters, or the empty string. Nested paths
are not supported: `hset(k, "a.b", 1)` sets a top-level key named `a.b`.

### Rules

- **Type guard.** Hash operations apply only to `value_type` 4 rows whose
  root is a JSON object. Any other code (0, 1, 2, 3, 5, 6), malformed JSON,
  or a JSON root that is an array or scalar raises a serialization error
  naming the actual type. The row is never modified.
- **Expiry.** Every hash statement carries the read path's liveness
  predicate, `expires_at IS NULL OR expires_at > :now`. A logically-expired
  key behaves exactly like a missing key: reads return empty or null, and a
  write starts a fresh object (the old fields and the old expiry are not
  carried over). A write never brings an expired row's contents back.
- **TTL preservation.** A field write never changes `expires_at`; only
  `expire`/`touch`/`persist` (or a whole-value write such as `set`) do.
- **Missing keys.** A write to a missing key creates `{}` plus the field(s),
  with **no expiry** unless the caller passes an optional TTL; that TTL is
  ignored when the key already exists. `hincrby` on a missing key or field
  starts from 0. Reads on a missing key return empty or null and never
  create a row.
- **Emptying a hash.** Unlike Redis, removing the last field leaves `{}` in
  place rather than deleting the key.
- **`size_bytes`** is recomputed from the new value in the same statement:
  `length(CAST(<new json> AS BLOB))`, which is bytes, not characters.
- **LRU metadata.** A hash write counts as one access: it sets
  `last_accessed` and increments `access_count` by exactly 1 per call, even
  when the call takes several statements (an `hdel` of three fields). A
  hash read follows the normal read rule (buffered metadata, no
  synchronous write).
- **Counters.** `hincrby` requires the existing field to be a JSON integer
  (string, boolean, null, object, array and float are rejected) and keeps
  it an integer: `5` becomes `6`, never `6.0`. A result outside the signed
  64-bit range is rejected and nothing is written. `hincrbyfloat` accepts an
  integer or float field and stores a float; non-finite results are
  rejected.
- **Float text.** SQLite before about 3.5x renders a REAL into JSON (and
  `CAST(x AS TEXT)`) with 15 significant digits, so `0.1 + 0.2` would be
  stored as `0.3`. Implementations therefore write a float field through
  `json(<text>)`, where `<text>` is the shortest of
  `printf('%!.15g' / '%!.16g' / '%!.17g', x)` that `CAST(... AS REAL)`
  reads back as exactly `x`. That is the same shortest round-trip form as
  Python's `repr()`, and it works on every SQLite version.
- **Encoding.** Rows written by hash operations stay UTF-8 JSON **text**
  (`value_type` 4), compact, with non-ASCII characters stored as UTF-8, the
  same as other code-4 values. Readers MUST accept any valid JSON
  formatting (whitespace, `\uXXXX` escapes), since rows may come from other
  encoders.

### JSON1 requirement and fallback

Hash writes use SQLite's built-in JSON functions (`json_set`, `json_remove`,
`json_extract`, `json_type`, `json_valid`, `json_each`, `json_object`) and
the `->` operator, all built in from SQLite 3.38. Bundled-SQLite drivers
(Python's own build, `sqlite-jdbc`, `modernc.org/sqlite`, `better-sqlite3`)
are recent enough. PHP's `pdo_sqlite` links the system SQLite, which may be
older on some distributions.

Implementations detect support once, when the first connection opens, by
running `SELECT json_valid('{}'), '{"a\"b":[1]}' -> '$."a\"b"'` (expecting
`1, '[1]'`) and catching any error. This checks the JSON functions, the `->`
operator and escaped path labels together. The last check matters: SQLite
3.44 and 3.45 resolve a `\"` inside a quoted path label incorrectly.
`json_extract` returns NULL, and 3.44's `json_set` silently writes
nothing. Releases from 3.49 on handle it correctly (verified on 3.49, 3.53). When the check fails, an
implementation falls back to read-modify-write inside `BEGIN IMMEDIATE`.
That is correct but slower and holds the write lock longer. It logs one
warning naming the SQLite version and reports the mode as
`stats()["hash_mode"]` (`"json1"` or `"fallback"`). The environment variable
`LYTECACHE_FORCE_JSON_FALLBACK=1` forces the fallback, for testing.

**JSONB is not used.** SQLite 3.45+ can store JSON in a binary format that
is faster to query, but other lytecache implementations cannot read it, so
it would break the cross-language file contract. Values are cast to `TEXT`
before every JSON function call, so a 3.45+ SQLite never interprets the
`BLOB` column as JSONB, and results are cast back to `BLOB`.

### Not supported

Per-field TTLs (Redis 7.4 `HEXPIRE`) would need a schema change and are a
future consideration. Nested-path operations and a separate hash storage
format are out of scope.

## Concurrency

- WAL mode allows one writer and many concurrent readers across processes.
- All read-then-write sequences that must be atomic (`add`, `replace`,
  `get_set`, counters, `set_many`, hash writes, lock acquire/release) are implemented
  either as a single SQL statement, or as an explicit `BEGIN IMMEDIATE`
  transaction that holds SQLite's write lock for the whole sequence so no
  other connection can interleave a conflicting write.
- `notify` and auto-published change events take the same write lock as
  `set`. Subscriptions only ever read.
- Hash writes take the same write lock as any other write and are subject
  to the same `busy_timeout`. A high rate of single-field writes is not
  cheaper in lock terms than whole-value writes; the gains are atomicity and
  smaller payloads.
- Per-process in-memory statistics (`hits`, `misses`, `evictions`,
  `expired_removed`) are exactly that — per-process. They are not
  synchronized across other processes sharing the same file.
