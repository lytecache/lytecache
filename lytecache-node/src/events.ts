/**
 * Local event notification (notify / watch) and in-process change listeners.
 *
 * Two separate mechanisms; see SPEC.md, "Events":
 *
 * - {@link Subscription}: the cross-process `watch` side. SINGLE HOST ONLY, POLLED (latency = poll
 *   interval), DURABLE (events are rows in the `events` table with a monotonic `seq`, kept for the
 *   retention window). This is not Redis Pub/Sub. Each subscription owns one timer and one
 *   read-only connection; it never takes the write lock.
 * - {@link ChangeDispatcher}: in-process `onChange` listeners. No SQLite involvement: change records
 *   are queued after a mutation commits and delivered from a `setImmediate` callback, never inside
 *   the write.
 */

import Database from "better-sqlite3";
import type { Database as DatabaseType } from "better-sqlite3";
import { EventEmitter } from "node:events";

import "./dispose-polyfill.js";
import { GapError } from "./errors.js";
import { deserialize } from "./serialize.js";

export const EVENTS_DDL = `
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  channel    TEXT    NOT NULL,
  namespace  TEXT    NOT NULL DEFAULT 'default',
  payload    BLOB,
  value_type INTEGER NOT NULL DEFAULT 4,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_seq_channel ON events(channel, seq);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
INSERT OR IGNORE INTO meta (k, v) VALUES ('events_version', '1');
`;

export const CACHE_CHANNEL = "__cache__";
export const EVENTS_BATCH = 500;
const MAX_IDLE_MS = 1000;
const MAX_QUEUE = 10_000;
export const MAX_DISPATCH_DEPTH = 8;

export type ChangeOp = "set" | "delete" | "flush";
export const CHANGE_OPS: readonly ChangeOp[] = ["set", "delete", "flush"];

/** One published event. `payload` is decoded with the same rules as cached values. */
export interface CacheEvent<T = unknown> {
  seq: number;
  channel: string;
  namespace: string;
  /** Unix milliseconds. */
  createdAt: number;
  payload: T;
}

/**
 * One committed mutation. A bulk operation produces ONE record: `keys` lists the keys the call
 * targeted, or is `null` with `truncated: true` when there were more than `changeKeyCap` (default
 * 100); `flush` always has `keys: null`. Its JSON form is the payload of `__cache__` events.
 */
export interface ChangeRecord {
  op: ChangeOp;
  namespace: string;
  keys: string[] | null;
  count: number;
  truncated: boolean;
}

export function validateChannel(channel: unknown): string {
  if (typeof channel !== "string" || channel === "") {
    throw new TypeError("channel must be a non-empty string");
  }
  if (channel.includes("*")) {
    throw new TypeError(
      `channel ${JSON.stringify(channel)} may not contain '*' (reserved for watch patterns)`,
    );
  }
  if (channel.startsWith("__")) {
    throw new TypeError(
      `channel names starting with '__' are reserved: ${JSON.stringify(channel)}`,
    );
  }
  return channel;
}

/**
 * The matching rule: an exact name, or a trailing `*` prefix glob; `*` alone matches everything;
 * `*` anywhere else is literal.
 */
export function channelFilter(patterns: readonly string[]): { sql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];
  for (const p of patterns) {
    if (typeof p !== "string" || p === "")
      throw new TypeError("watch patterns must be non-empty strings");
    if (p.endsWith("*")) {
      const prefix = p.slice(0, -1);
      if (prefix === "") return { sql: "1", params: [] };
      clauses.push("substr(channel, 1, ?) = ?");
      params.push([...prefix].length, prefix);
    } else {
      clauses.push("channel = ?");
      params.push(p);
    }
  }
  return { sql: `(${clauses.join(" OR ")})`, params };
}

interface EventRow {
  seq: number;
  channel: string;
  namespace: string;
  payload: Buffer | null;
  value_type: number;
  created_at: number;
}

export function lastAssignedSeq(db: DatabaseType): number {
  const row = db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'events'").get() as
    { seq: number } | undefined;
  return row?.seq ?? 0;
}

/**
 * SPEC.md's gap rule: a cursor older than the oldest retained event minus one has lost events;
 * one ahead of the last seq ever assigned means the file was recreated.
 */
export function gapCheck(db: DatabaseType, cursor: number): GapError | null {
  const last = lastAssignedSeq(db);
  if (cursor > last) return new GapError(cursor, last + 1, true);
  const row = db.prepare("SELECT min(seq) AS m FROM events").get() as { m: number | null };
  const oldest = row.m ?? last + 1;
  if (cursor < oldest - 1) return new GapError(cursor, oldest, false);
  return null;
}

export interface SubscriptionInit {
  path: string;
  namespace: string;
  patterns: readonly string[];
  startSeq: number;
  gap: GapError | null;
  pollIntervalMs: number;
  onClose: (sub: Subscription) => void;
  logger: (message: string) => void;
}

/**
 * A live, polled view of the `events` table for some channels, returned by `LyteCache.watch`.
 *
 * SINGLE HOST ONLY, POLLED, DURABLE. Consume it either by pulling -- `for await (const e of sub)`,
 * `await sub.next(timeoutMs)`, `sub.tryNext()` -- or by pushing, with `sub.on("event", fn)`. While
 * any `"event"` listener is attached, events are emitted rather than queued; otherwise they queue
 * (up to 10,000, then polling pauses until you read). Delivery is at-least-once from your stored
 * cursor, in `seq` order -- handlers must be idempotent. Lost events surface as a {@link GapError}:
 * rejected from the pull API, or emitted as `"error"` in push mode. Always `close()` it (or `using`):
 * an open subscription keeps its polling timer, and so the process, alive.
 */
export class Subscription extends EventEmitter implements AsyncIterable<CacheEvent>, Disposable {
  readonly patterns: readonly string[];
  private readonly path: string;
  private readonly namespace: string;
  private readonly filter: { sql: string; params: unknown[] };
  private readonly pollIntervalMs: number;
  private readonly onCloseCb: (sub: Subscription) => void;
  private readonly logger: (message: string) => void;
  private db: DatabaseType | null = null;
  private cursor: number;
  private delivered: number;
  private interval: number;
  private timer: NodeJS.Timeout | null = null;
  private readonly queue: CacheEvent[] = [];
  private waiters: { resolve: () => void }[] = [];
  private gap: GapError | null;
  private closed = false;

  constructor(init: SubscriptionInit) {
    super();
    this.path = init.path;
    this.namespace = init.namespace;
    this.patterns = [...init.patterns];
    this.filter = channelFilter(init.patterns);
    this.cursor = init.startSeq;
    this.delivered = init.startSeq;
    this.pollIntervalMs = init.pollIntervalMs;
    this.interval = init.pollIntervalMs;
    this.onCloseCb = init.onClose;
    this.logger = init.logger;
    this.gap = init.gap;
    if (this.gap === null) {
      this.db = new Database(this.path, { readonly: true, fileMustExist: true });
      this.db.pragma("busy_timeout = 5000");
      this.schedule(0);
    }
  }

  /** seq of the last event handed to you (the value to persist as a cursor). */
  get lastSeq(): number {
    return this.delivered;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  /** @internal Wakes this subscription now (an in-process notify); the table stays the truth. */
  wake(): void {
    if (this.closed || this.gap !== null) return;
    this.interval = this.pollIntervalMs;
    this.schedule(0);
  }

  private schedule(ms: number): void {
    if (this.closed) return;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = setTimeout(() => this.poll(), ms);
  }

  private pushMode(): boolean {
    return this.listenerCount("event") > 0;
  }

  private poll(): void {
    this.timer = null;
    if (this.closed || this.db === null) return;
    if (!this.pushMode() && this.queue.length >= MAX_QUEUE) {
      this.schedule(this.pollIntervalMs); // backpressure: wait for the consumer
      return;
    }
    let rows: EventRow[] = [];
    try {
      // One read transaction, so the gap check and the fetch see the same snapshot: the sweeper
      // cannot delete events between the two.
      const db = this.db;
      const read = db.transaction((): EventRow[] => {
        const gap = gapCheck(db, this.cursor);
        if (gap !== null) {
          this.gap = gap;
          return [];
        }
        return db
          .prepare(
            `SELECT seq, channel, namespace, payload, value_type, created_at FROM events
             WHERE seq > ? AND namespace = ? AND ${this.filter.sql} ORDER BY seq LIMIT ?`,
          )
          .all(this.cursor, this.namespace, ...this.filter.params, EVENTS_BATCH) as EventRow[];
      });
      rows = read.deferred();
    } catch (err) {
      this.logger(`watch poll failed, retrying: ${String(err)}`);
    }
    if (this.gap !== null) {
      this.reportGap();
      return;
    }
    for (const row of rows) {
      this.cursor = row.seq;
      this.deliver(this.toEvent(row));
    }
    if (rows.length > 0) {
      this.interval = this.pollIntervalMs;
      if (rows.length === EVENTS_BATCH) {
        this.schedule(0); // more are waiting: read on without sleeping
        return;
      }
    } else {
      this.interval = Math.min(this.interval * 2, Math.max(MAX_IDLE_MS, this.pollIntervalMs));
    }
    this.schedule(this.interval);
  }

  private toEvent(row: EventRow): CacheEvent {
    return {
      seq: row.seq,
      channel: row.channel,
      namespace: row.namespace,
      createdAt: row.created_at,
      get payload(): unknown {
        return row.payload === null ? null : deserialize(row.payload, row.value_type);
      },
    };
  }

  private deliver(event: CacheEvent): void {
    if (this.pushMode()) {
      this.delivered = event.seq;
      try {
        this.emit("event", event);
      } catch (err) {
        this.logger(`watch "event" listener threw: ${String(err)}`);
      }
      return;
    }
    this.queue.push(event);
    this.wakeWaiters();
  }

  private reportGap(): void {
    this.releaseDb();
    if (this.pushMode() || this.listenerCount("error") > 0) {
      const gap = this.gap!;
      queueMicrotask(() => this.emit("error", gap));
    }
    this.wakeWaiters();
  }

  private wakeWaiters(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const w of waiters) w.resolve();
  }

  /** The next already-fetched event, without waiting; `undefined` if there is none yet. */
  tryNext(): CacheEvent | undefined {
    const event = this.queue.shift();
    if (event !== undefined) {
      this.delivered = event.seq;
      return event;
    }
    if (this.gap !== null) {
      const gap = this.gap;
      this.close();
      throw gap;
    }
    return undefined;
  }

  /**
   * Waits up to `timeoutMs` (forever if omitted) for the next event. Resolves `undefined` on
   * timeout or once closed; rejects with {@link GapError} if events were lost to retention.
   */
  async next(timeoutMs?: number): Promise<CacheEvent | undefined> {
    const deadline = timeoutMs === undefined ? Infinity : Date.now() + timeoutMs;
    for (;;) {
      const event = this.tryNext();
      if (event !== undefined) return event;
      if (this.closed) return undefined;
      const remaining = deadline - Date.now();
      if (remaining <= 0) return undefined;
      await new Promise<void>((resolve) => {
        const waiter = { resolve };
        this.waiters.push(waiter);
        if (remaining !== Infinity) {
          setTimeout(() => {
            this.waiters = this.waiters.filter((w) => w !== waiter);
            resolve();
          }, remaining);
        }
      });
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<CacheEvent> {
    for (;;) {
      const event = await this.next();
      if (event === undefined) return;
      yield event;
    }
  }

  private releaseDb(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.db !== null) {
      this.db.close();
      this.db = null;
    }
  }

  /** Stops polling and releases the timer and connection. Idempotent. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.releaseDb();
    this.wakeWaiters();
    this.onCloseCb(this);
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

type Listener = (record: ChangeRecord) => unknown;

/**
 * In-process `onChange` delivery. Records are queued after a mutation commits and delivered from a
 * `setImmediate` callback -- never inside the write, never holding anything a listener's own write
 * would need. Nothing is scheduled until a listener exists.
 */
export class ChangeDispatcher {
  private readonly listeners = new Map<number, { fn: Listener; ops: Set<ChangeOp> }>();
  private nextId = 0;
  private queue: { record: ChangeRecord; depth: number }[] = [];
  private scheduled: NodeJS.Immediate | null = null;
  private currentDepth = 0;
  private closed = false;

  constructor(private readonly logger: (message: string) => void) {}

  add(fn: Listener, ops: Iterable<string>): () => void {
    const set = new Set<ChangeOp>();
    for (const op of ops) {
      if (!(CHANGE_OPS as readonly string[]).includes(op)) {
        throw new TypeError(
          `onChange events must be a non-empty subset of ${CHANGE_OPS.join(", ")}`,
        );
      }
      set.add(op as ChangeOp);
    }
    if (set.size === 0) throw new TypeError("onChange events must not be empty");
    const id = this.nextId++;
    this.listeners.set(id, { fn, ops: set });
    return () => {
      this.listeners.delete(id);
    };
  }

  wants(op: ChangeOp): boolean {
    for (const l of this.listeners.values()) if (l.ops.has(op)) return true;
    return false;
  }

  /**
   * Queues a record. A record produced while a listener is running (by that listener's own,
   * synchronous write) is one level deeper; past {@link MAX_DISPATCH_DEPTH} it is dropped. Writes
   * a listener makes after an `await` start again at depth 1 -- the guard only sees synchronous
   * re-entry.
   */
  submit(record: ChangeRecord): void {
    if (this.closed) return;
    const depth = this.currentDepth + 1;
    if (depth > MAX_DISPATCH_DEPTH) {
      this.logger(
        `dropping ${record.op} change record: onChange listeners re-entered the cache ` +
          `${MAX_DISPATCH_DEPTH} levels deep`,
      );
      return;
    }
    this.queue.push({ record, depth });
    if (this.scheduled === null) this.scheduled = setImmediate(() => this.drain());
  }

  private drain(): void {
    this.scheduled = null;
    const batch = this.queue;
    this.queue = [];
    for (const { record, depth } of batch) {
      for (const [id, l] of [...this.listeners]) {
        if (!l.ops.has(record.op) || !this.listeners.has(id)) continue;
        this.currentDepth = depth;
        try {
          const result = l.fn(record);
          if (result instanceof Promise) {
            result.catch((err: unknown) =>
              this.logger(`onChange listener rejected: ${String(err)}`),
            );
          }
        } catch (err) {
          this.logger(`onChange listener threw: ${String(err)}`);
        } finally {
          this.currentDepth = 0;
        }
      }
    }
  }

  close(): void {
    this.closed = true;
    this.listeners.clear();
    this.queue = [];
    if (this.scheduled !== null) {
      clearImmediate(this.scheduled);
      this.scheduled = null;
    }
  }
}
