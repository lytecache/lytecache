# Local event notification: `notify` / `watch`

## Read this first: the boundaries

- **Single host only.** If your subscribers run on different machines, use
  Redis or a real message broker. This will not work, and it is not
  intended to. Events live in the cache's SQLite file. SQLite locking is
  unreliable on network filesystems (NFS, SMB, Kubernetes
  `ReadWriteMany`), so a cache file shared between hosts is not supported
  at all.
- **Polled, not pushed.** Subscribers poll the file. Delivery latency is
  roughly the poll interval (default 50 ms), and up to about 1 s for the
  first event after a quiet period (see [Latency](#latency)). If you need
  sub-millisecond delivery, this is the wrong tool.
- **Durable, at-least-once.** Every event is a committed row with a
  monotonic sequence number. A subscriber that was down resumes from its
  last seen sequence instead of missing everything. The flip side: a
  subscriber that crashes between handling an event and saving its cursor
  sees that event again. **Your handlers must be idempotent.**
- **Fan-out only.** Every subscriber watching a channel sees every event on
  it. There are no consumer groups, no exclusive delivery and no
  acknowledgements.
- **Opt-in.** A cache that never calls `notify` or `watch` creates no
  `events` table, starts no polling thread, and has no measurable overhead
  on `get`/`set`.
- **It is not Redis Pub/Sub.** Some things are better than Pub/Sub
  (durable, resumable), and some are worse (local, polled, higher latency).

## The headline use case: cross-worker cache invalidation

Several workers on one host share a cache file: PHP-FPM or Gunicorn
workers, or a Spring app plus its background worker. Each also keeps
derived state in memory, such as a memo dict or a computed config. When one
worker changes the underlying data, the others' in-memory copies are stale.

```python
# worker A, after committing a DB change
cache.delete("balance:42")
cache.notify("invalidate", {"key": "balance:42"})

# workers B..N
for event in cache.watch("invalidate"):
    local_memo.pop(event.payload["key"], None)
```

Run the `watch` loop on a background thread in each worker. Publish
**after** your own database transaction commits. If you publish from inside
a transaction that later rolls back, the others will invalidate for a
change that never happened. That is harmless here, but wrong in general.

The same recipe in the other languages:

```java
// Java -- Subscription is AutoCloseable and Iterable<Event> (also poll(), onEvent())
cache.delete("balance:42");
cache.notify("invalidate", Map.of("key", "balance:42"));

try (Subscription sub = cache.watch("invalidate")) {
    for (Event e : sub) localMemo.remove(e.payload(Map.class).get("key"));
}
```

```ts
// Node -- an async iterator (and an EventEmitter-style .on("event"))
cache.delete("balance:42");
cache.notify("invalidate", { key: "balance:42" });

using sub = cache.watch("invalidate");
for await (const event of sub) localMemo.delete(event.payload.key);
```

```go
// Go -- events arrive on a channel
cache.Delete("balance:42")
cache.Notify("invalidate", map[string]any{"key": "balance:42"})

sub, _ := cache.Watch([]string{"invalidate"})
defer sub.Close()
for ev := range sub.Events() {
    var p struct{ Key string `json:"key"` }
    ev.Decode(&p)
    delete(localMemo, p.Key)
}
```

```php
// PHP -- no threads: polling happens inside the foreach (run it in a CLI worker)
$cache->delete('balance:42');
$cache->notify('invalidate', ['key' => 'balance:42']);

foreach ($cache->watch('invalidate') as $event) {
    unset($localMemo[$event->payload()['key']]);
}
```

### Built-in: automatic invalidation events

Rather than calling `notify` yourself after every `delete`, you can have
lytecache publish mutations to the reserved `__cache__` channel. This is
off by default and chosen per operation:

```python
cache = LyteCache(publish_changes={"delete", "flush"})

for event in cache.watch("__cache__"):
    change = event.payload  # {"op", "namespace", "keys", "count", "truncated"}
    if change["truncated"]:
        local_memo.clear()  # a bulk change: refetch by namespace or prefix
    else:
        for key in change["keys"]:
            local_memo.pop(key, None)
```

This adds one event row written under the write lock **per mutating call**
(not per key; see below), so enable only the operations you need. Events on
`delete` and `flush` are usually enough for invalidation. Publishing every
`set` doubles the writes for single-key sets.

## Two different mechanisms for reacting to changes

Don't confuse these. They have different costs and different latency.

| | `on_change(listener)` | `watch("__cache__")` with `publish_changes` |
|---|---|---|
| Reaches | this process only | every process on this host |
| Latency | effectively zero | the poll interval (50 ms+) |
| Cost | none when unused; a queue put when used | one event row written per mutation, under the write lock |
| Survives restarts | no | yes, within retention |
| Use for | invalidating an in-process memo, local metrics, derived in-memory state | coordinating separate worker processes |

```python
unsubscribe = cache.on_change(lambda rec: memo.pop(rec.keys[0], None)
                              if rec.keys else memo.clear(),
                              events={"set", "delete", "flush"})
```

Listeners are called only after the mutation's transaction commits, on a
dispatch thread that holds no cache lock. A slow listener never stalls
writers, a throwing listener is logged and skipped, and a listener may
write to the cache. A listener whose own writes keep re-triggering
listeners is stopped after 8 levels, with a warning.

### Bulk operations emit one record, not N

A `set_many` of 1,000 keys produces **one** change record, for both
mechanisms. Per-key records would multiply the writes of the very batching
`set_many` exists to save. The record carries the operation, namespace,
count, and the key list up to `change_key_cap` (default 100). Past the cap,
`keys` is `null` and `truncated` is `true`. `flush` is always truncated.
For a truncated record, invalidate by namespace or key prefix rather than
key by key.

Measured cost of each mechanism (Python, `set_many` of 10,000 keys, median
of 7 runs on an Apple-silicon laptop):

| | time |
|---|---|
| no listeners | 37.4 ms |
| one `on_change` listener | 37.9 ms |
| `publish_changes={"set"}` | 42.5 ms |

## API

```python
seq = cache.notify(channel, payload=None)            # -> int, the event's seq
seqs = cache.notify_many([(channel, payload), ...])  # one transaction

sub = cache.watch(*patterns, from_seq=None, poll_interval=0.05)
for event in sub: ...            # blocking iteration
sub.poll(timeout)                # next event or None
sub.try_next()                   # non-blocking
sub.close()                      # mandatory, idempotent; or `with cache.watch(...) as sub:`

event.seq, event.channel, event.namespace, event.payload, event.created_at  # created_at: unix ms

cache.save_cursor("worker-b", event.seq)
cache.watch("jobs", from_seq=cache.load_cursor("worker-b"))
```

- **Channels.** Exact names, or a trailing `*` for a prefix:
  `"cache:invalidate:*"`. `"*"` alone matches everything. Nothing else is
  special. Published channel names may not contain `*` or start with `__`.
- **Payloads** use exactly the same encoding as cached values, so an event
  published from Python is readable from Java, Node, Go and PHP.
- **Namespaces.** A subscription sees events published in its own cache's
  namespace.
- **`from_seq`.** `None` means only events published from now on. `N`
  resumes after seq `N`. `0` replays everything still retained.
- **Ordering** is total per file, by `seq`, across all channels.

### Durable resume

Persist the seq of the last event you *finished* handling, and start from
it next time:

```python
with cache.watch("jobs", from_seq=cache.load_cursor("worker-b")) as sub:
    for event in sub:
        handle(event)                            # must be idempotent
        cache.save_cursor("worker-b", event.seq)
```

Each `save_cursor` is a write. On a busy stream, save every N events and
accept that up to N may be redelivered after a crash.

## Retention and gaps

Events are deleted by the maintenance sweeper:

- by age: `event_retention`, default 3600 seconds;
- optionally by count: `event_max_count`, which keeps the newest N.

An event past retention is gone **whether or not anyone consumed it**. A
subscriber offline for longer than the retention window loses the events
published in between. **Retention is the knob for how much downtime you can
tolerate.**

Losing events is never silent. A subscriber whose cursor is older than the
oldest retained event gets a `GapError` on its first read, instead of being
quietly resumed at the wrong place. So does one that falls that far behind
while running. The error carries `requested_seq`, `oldest_seq` and
`resume_seq`. To accept the loss and carry on, typically after a full
resync of whatever state the events were keeping fresh, watch again from
`err.resume_seq`. `from_seq=0` never raises a gap: it explicitly means
"whatever is still there".

## Latency

- **While events flow:** each subscriber polls every `poll_interval`
  (default 50 ms), so delivery takes between 0 and 50 ms, plus the time to
  handle anything queued ahead.
- **When idle, adaptive backoff:** after each empty poll the wait doubles,
  50 → 100 → 200 → … → 1 s, and snaps back to 50 ms on the first new
  event. This stops an idle cache from doing 20 reads a second forever.
  The cost is that the *first* event after a quiet spell can take up to
  the current backoff, at most about 1 s.
- **Same process:** a `notify` from the same cache instance wakes its own
  subscriptions immediately. This is an optimization only, and other
  processes still poll.

Measured on an Apple-silicon laptop (Python, default 50 ms poll interval,
publisher in a separate process, events 70–150 ms apart):

| | p50 | p95 | max |
|---|---|---|---|
| cross-process, notify → received | 49.9 ms | 105.7 ms | 200.5 ms |
| same process, notify → received | 0.59 ms | 15.9 ms | |
| idle subscriber | 1.0 poll/s, 0.044 % of one CPU core | | |

## Write contention

`notify` takes the same SQLite writer lock as `set`. A high-rate event
stream therefore slows cache writes, as any other writes would. When you
publish many events at once, use `notify_many`, which writes them all in
one transaction. Subscriptions never take the write lock: each one polls on
its own read-only connection.

## Compared with Redis

| | lytecache `notify`/`watch` | Redis Pub/Sub | Redis Streams |
|---|---|---|---|
| Reach | one host | network | network |
| Delivery | polled (≈ poll interval) | pushed (sub-ms) | pushed or polled |
| Durable | yes, within retention | no, fire-and-forget | yes |
| Resume after downtime | yes, from a stored seq | no, missed messages are gone | yes |
| Gap detection | explicit `GapError` | n/a | trimmed IDs |
| Consumer groups / exclusive delivery | no | no | yes |
| Infrastructure | none (the cache file) | a Redis server | a Redis server |

The one place this design beats Redis Pub/Sub is that events are durable
and resumable. Redis Streams has that too, along with network reach and
consumer groups, which this lacks. If you need any of those, use Redis.

## From the shell

```bash
lytecache notify invalidate '{"key":"balance:42"}'   # publish (payload parsed as JSON if valid)
lytecache watch 'invalidate*' --from-seq 120         # tail live, one JSON line per event
lytecache events --limit 50                          # recent retained events: seq, channel, age, size
```

The web UI's **Events** tab shows a live tail and retention stats for each
database. It is read-only and cannot publish events, consistent with the
UI's delete-only mutation policy.

## On disk

Events live in an `events` table created the first time `notify` or
`watch` is used. The file keeps `schema_version = 1`, so libraries
released before events existed still open it for ordinary cache use. See
[SPEC.md](../SPEC.md#events-local-notification-notify--watch) for the exact
table, polling query, delivery semantics and gap rule that every
implementation follows.
