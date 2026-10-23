import Database from "better-sqlite3";
import type { Database as DatabaseType } from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

import "./dispose-polyfill.js";
import type { GapError } from "./errors.js";
import { CacheFullError, LyteCacheError, SchemaVersionError, SerializationError } from "./errors.js";
import {
  CACHE_CHANNEL,
  CHANGE_OPS,
  ChangeDispatcher,
  EVENTS_DDL,
  Subscription,
  channelFilter,
  gapCheck,
  lastAssignedSeq,
  validateChannel,
  type ChangeOp,
  type ChangeRecord,
} from "./events.js";
import { JsonObject, parseJsonObject } from "./json-object.js";
import { CacheLock, type LockOptions } from "./lock.js";
import { defaultPath as resolveDefaultPath } from "./paths.js";
import { DDL, PRAGMAS, SCHEMA_VERSION, TypeCode } from "./schema.js";
import {
  deserialize,
  serialize,
  type DeserializeOptions,
} from "./serialize.js";
import { sleepSyncMs } from "./sleep-sync.js";

export type Eviction = "lru" | "ttl" | "random" | "noeviction";

export interface LyteCacheOptions {
  /** Explicit database file path. Optional escape hatch; omit to use the zero-config default. */
  path?: string;
  /** Logical partition within the database file. Default `"default"`. */
  namespace?: string;
  /** Evict when the namespace exceeds this many keys. */
  maxKeys?: number;
  /** Evict when the namespace exceeds this many bytes. */
  maxBytes?: number;
  /** Eviction policy when `maxKeys`/`maxBytes` is exceeded. Default `"lru"`. */
  eviction?: Eviction;
  /** Seconds between background maintenance passes. `null` disables the timer and sweeps
   * opportunistically every ~100 operations instead. Default 60. */
  sweepInterval?: number | null;
  /** When true, internal read errors throw instead of degrading to a miss. Default false. */
  strict?: boolean;
  /** Called with a warning message when a non-strict read degrades to a miss. Default `console.warn`. */
  logger?: (message: string) => void;
  /**
   * Mutations that also publish a {@link ChangeRecord} to the reserved `__cache__` channel, for
   * watchers in other processes on this host. Off by default. Each costs one event row written under
   * the write lock (per call, not per key); delivery is polled. For in-process reactions use
   * {@link LyteCache.onChange} instead.
   */
  publishChanges?: Iterable<ChangeOp>;
  /** Seconds events are kept before the sweeper deletes them, read or not. Default 3600. */
  eventRetention?: number;
  /** Additionally keep at most this many newest events. Default: no count cap. */
  eventMaxCount?: number;
  /** Keys a {@link ChangeRecord} lists before it is truncated. Default 100. */
  changeKeyCap?: number;
}

export interface WatchOptions {
  /** Resume after this seq (0 = everything retained). Omit for only events published from now on. */
  fromSeq?: number;
  /** Seconds between polls while events flow (default 0.05); backs off to ~1 s while idle. */
  pollInterval?: number;
}

export interface SetOptions {
  /** Time-to-live, in seconds. Omit (or `undefined`) for no expiry. */
  ttl?: number;
}

export type GetOptions = DeserializeOptions;

export interface CacheStatsSnapshot {
  hits: number;
  misses: number;
  hitRate: number;
  keyCount: number;
  sizeBytes: number;
  evictions: number;
  expiredRemoved: number;
  path: string;
  /** "json1" or "fallback" -- see {@link LyteCache.hashMode}. */
  hashMode: "json1" | "fallback";
}

const BATCH_DELETE_LIMIT = 500;
const LRU_FLUSH_THRESHOLD = 200;
const OPPORTUNISTIC_EVERY = 100;
const FORCE_JSON_FALLBACK_ENV = "LYTECACHE_FORCE_JSON_FALLBACK";
const HSET_MANY_PER_STATEMENT = 500; // fields per statement (bound-variable limit)
const JSON_SET_PAIRS = 30; // path/value pairs per json_set call (function-argument limit)
const INT64_MAX = 9223372036854775807n;
const INT64_MIN = -9223372036854775808n;
const TYPE_NAMES: Record<number, string> = {
  0: "bytes",
  1: "str",
  2: "int",
  3: "float",
  4: "json",
  5: "pickle",
  6: "java-native",
};

// SQL fragments for the JSON1 hash path. Field names are always bound (@f0, ...), never
// interpolated: the path is built in SQL as $."<field>" with backslashes and quotes escaped. The
// BLOB value is CAST to TEXT before every JSON function so SQLite 3.45+ never reads it as JSONB, and
// results are CAST back to BLOB.
const DOC = "CAST(cache.value AS TEXT)";
const LIVE = "(cache.expires_at IS NULL OR cache.expires_at > @now)";
const EXPIRED = "(cache.expires_at IS NOT NULL AND cache.expires_at <= @now)";
const ROOT_TYPE = `(CASE WHEN json_valid(${DOC}) THEN json_type(${DOC}) END)`;
const IS_OBJ = `(cache.value_type = 4 AND CASE WHEN json_valid(${DOC}) THEN json_type(${DOC}) END = 'object')`;

function path(param: string): string {
  return String.raw`('$."' || replace(replace(@${param}, '\', '\\'), '"', '\"') || '"')`;
}

/**
 * JSON text for a REAL with the fewest digits that round-trip exactly. SQLite before ~3.5x (the
 * bundled 3.49 included) renders a REAL into JSON -- and CAST(x AS TEXT) -- with 15 significant
 * digits, which would store 0.1 + 0.2 as 0.3. This picks the shortest of 15/16/17 digits that reads
 * back to the same double, on every SQLite version.
 */
function floatText(expr: string): string {
  const p = (n: number): string => `printf('%!.${n}g', ${expr})`;
  return `(CASE WHEN CAST(${p(15)} AS REAL) = ${expr} THEN ${p(15)} WHEN CAST(${p(16)} AS REAL) = ${expr} THEN ${p(16)} ELSE ${p(17)} END)`;
}

function fieldType(param: string): string {
  return `(CASE WHEN ${IS_OBJ} THEN json_type(${DOC}, ${path(param)}) END)`;
}

/**
 * An UPSERT that creates `freshDoc` for a missing/expired key, or rewrites a live JSON object to
 * `liveDoc` when `where` holds. expires_at is preserved on a live row and size_bytes is recomputed
 * from the new value in the same statement.
 */
function upsertHashSql(liveDoc: string, freshDoc: string, where: string): string {
  const newDoc = `CAST(CASE WHEN ${EXPIRED} THEN ${freshDoc} ELSE ${liveDoc} END AS BLOB)`;
  return `
    INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
    VALUES (@ns, @key, CAST(${freshDoc} AS BLOB), 4, @now, @exp, @now, 0, length(CAST(${freshDoc} AS BLOB)))
    ON CONFLICT(namespace, key) DO UPDATE SET
      value = ${newDoc},
      value_type = 4,
      created_at = CASE WHEN ${EXPIRED} THEN @now ELSE cache.created_at END,
      expires_at = CASE WHEN ${EXPIRED} THEN @exp ELSE cache.expires_at END,
      last_accessed = @now,
      access_count = CASE WHEN ${EXPIRED} THEN 0 ELSE cache.access_count + 1 END,
      size_bytes = length(${newDoc})
    WHERE ${EXPIRED} OR ${where}`;
}

function requireField(field: unknown): void {
  if (typeof field !== "string") throw new TypeError(`hash field names must be strings, not ${typeof field}`);
}

/**
 * Encodes one hash field value as compact JSON text. Buffers are rejected (JSON would store an
 * object of byte numbers), as are undefined, NaN/Infinity, and BigInts nested inside objects.
 */
function encodeHashValue(value: unknown): string {
  if (value === undefined) throw new SerializationError("cannot store undefined in a hash field -- did you mean null?");
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    throw new SerializationError("hash fields hold JSON values; Buffers are not supported");
  }
  if (typeof value === "bigint") {
    if (value > INT64_MAX || value < INT64_MIN) {
      throw new SerializationError(`bigint ${value} is outside the signed 64-bit integer range`);
    }
    return value.toString();
  }
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new SerializationError("cannot store NaN or Infinity in a hash field");
  }
  if (typeof value === "number") return JSON.stringify(value); // a JSON number, any magnitude
  const { bytes, typeCode } = serialize(value);
  if (typeCode === TypeCode.JSON) return bytes.toString("utf8");
  return JSON.stringify(typeof value === "string" ? value : Number(bytes.toString("utf8")));
}

/** Renders a float the way SQLite's json_set does: always with a fraction or exponent. */
function formatJsonFloat(f: number): string {
  const s = String(f);
  return /[.eE]/.test(s) ? s : `${s}.0`;
}
interface RawRow {
  value: Buffer;
  value_type: number;
  expires_at: number | null;
}

function nowMs(): number {
  return Date.now();
}

/**
 * An embedded, Redis-like cache backed by a local SQLite file. The class is the entire public
 * API -- there are no module-level functions. Zero configuration is required:
 *
 * ```ts
 * import { LyteCache } from "lytecache";
 *
 * const cache = new LyteCache();
 * cache.set("user:42", { name: "Ada" }, { ttl: 300 });
 * cache.get("user:42"); // { name: "Ada" }
 * ```
 *
 * Every method is synchronous: better-sqlite3's native binding blocks the event loop for the
 * (microsecond-scale) duration of the query, which is what makes this API clean -- no `await`
 * noise, no connection pool, no partially-applied writes to reason about. See the README's "Why
 * synchronous?" section.
 */
export class LyteCache implements Disposable {
  private readonly db: DatabaseType;
  private readonly dbPath: string;
  private readonly namespace: string;
  private readonly maxKeys: number | null;
  private readonly maxBytes: number | null;
  private readonly eviction: Eviction;
  private readonly sweepIntervalSeconds: number | null;
  private readonly strict: boolean;
  private readonly logger: (message: string) => void;

  private hits = 0;
  private misses = 0;
  private evictionsCount = 0;
  private expiredRemovedCount = 0;
  private opCount = 0;
  private closed = false;

  private readonly lruBuffer = new Map<string, { lastAccessed: number; accessCount: number }>();
  private sweeperTimer: NodeJS.Timeout | null = null;

  // JSON1 support for hash operations, detected once at open.
  private readonly json1: boolean;
  // Events and change listeners are opt-in: none of this costs anything (no table, no timer) until
  // notify/watch/onChange is first used.
  private readonly publishOps: Set<ChangeOp>;
  private readonly eventRetentionMs: number;
  private readonly eventMaxCount: number | null;
  private readonly changeKeyCap: number;
  private eventsReady = false;
  private readonly subscriptions = new Set<Subscription>();
  private dispatcher: ChangeDispatcher | null = null;

  constructor(options: LyteCacheOptions = {}) {
    this.dbPath = options.path ?? resolveDefaultPath();
    this.namespace = options.namespace ?? "default";
    this.maxKeys = options.maxKeys ?? null;
    this.maxBytes = options.maxBytes ?? null;
    this.eviction = options.eviction ?? "lru";
    this.sweepIntervalSeconds = options.sweepInterval === undefined ? 60 : options.sweepInterval;
    this.strict = options.strict ?? false;
    this.logger = options.logger ?? ((message: string) => console.warn(`lytecache: ${message}`));
    this.publishOps = new Set(options.publishChanges ?? []);
    for (const op of this.publishOps) {
      if (!(CHANGE_OPS as readonly string[]).includes(op)) {
        throw new TypeError(`publishChanges must be a subset of ${CHANGE_OPS.join(", ")}, got ${String(op)}`);
      }
    }
    const retention = options.eventRetention ?? 3600;
    if (!(retention > 0)) throw new TypeError("eventRetention must be a positive number of seconds");
    this.eventRetentionMs = Math.round(retention * 1000);
    this.eventMaxCount = options.eventMaxCount ?? null;
    if (this.eventMaxCount !== null && !(this.eventMaxCount >= 1)) {
      throw new TypeError("eventMaxCount must be a positive integer");
    }
    this.changeKeyCap = options.changeKeyCap ?? 100;

    const parent = dirname(this.dbPath);
    mkdirSync(parent, { recursive: true });

    this.db = new Database(this.dbPath);
    this.initWithRetry();
    this.json1 = this.detectJson1();

    if (this.sweepIntervalSeconds !== null) {
      this.sweeperTimer = setInterval(() => this.sweepOnce(), this.sweepIntervalSeconds * 1000);
      this.sweeperTimer.unref();
    }
  }

  // -- construction / schema -------------------------------------------------

  /** Returns the resolved zero-config default database path. */
  static defaultPath(): string {
    return resolveDefaultPath();
  }

  /** The actual database file backing this instance. */
  get path(): string {
    return this.dbPath;
  }

  /**
   * Applies the PRAGMAs and schema, retrying on SQLITE_BUSY: when several processes create the
   * same fresh WAL-mode file for the first time simultaneously, the very first `journal_mode=WAL`
   * switch (which needs a brief exclusive lock) can collide before `busy_timeout` has had a
   * chance to matter, independent of how high `busy_timeout` itself is set. This is a well-known
   * SQLite cold-start race, not something a single PRAGMA setting resolves.
   */
  private initWithRetry(): void {
    const maxAttempts = 25;
    for (let attempt = 1; ; attempt++) {
      try {
        for (const pragma of PRAGMAS) {
          this.db.pragma(pragma.replace(/^PRAGMA\s+/i, ""));
        }
        this.initSchema();
        return;
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code === "SQLITE_BUSY" && attempt < maxAttempts) {
          sleepSyncMs(20);
          continue;
        }
        throw err;
      }
    }
  }

  private initSchema(): void {
    this.db.exec(DDL);
    const row = this.db.prepare("SELECT v FROM meta WHERE k = 'schema_version'").get() as
      | { v: string }
      | undefined;
    if (row === undefined) {
      // INSERT OR IGNORE: another process opening this same fresh file for the first time may
      // win this exact race between the SELECT above and this INSERT, in which case this becomes
      // a harmless no-op instead of a PRIMARY KEY constraint violation.
      this.db.prepare("INSERT OR IGNORE INTO meta (k, v) VALUES ('schema_version', '1')").run();
    } else {
      const version = Number.parseInt(row.v, 10);
      if (version > SCHEMA_VERSION) {
        throw new SchemaVersionError(
          `database schema version ${version} is newer than the version ${SCHEMA_VERSION} supported ` +
            "by this lytecache install; upgrade lytecache to open this file",
        );
      }
    }
  }

  private requireOpen(): void {
    if (this.closed) {
      throw new LyteCacheError("this LyteCache instance is closed");
    }
  }

  private hasCapacityLimits(): boolean {
    return this.maxKeys !== null || this.maxBytes !== null;
  }

  // -- key/value --------------------------------------------------------------

  /** Stores a value, optionally with a TTL in seconds. */
  set(key: string, value: unknown, options: SetOptions = {}): void {
    this.requireOpen();
    const { bytes, typeCode } = serialize(value);
    const now = nowMs();
    const expiresAt = options.ttl === undefined ? null : now + Math.round(options.ttl * 1000);

    if (this.eviction === "noeviction" && this.hasCapacityLimits()) {
      this.checkCapacityBeforeWrite(key);
    }

    this.db
      .prepare(
        `INSERT INTO cache
            (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
         VALUES (@ns, @key, @value, @vtype, @now, @expiresAt, @now, 0, @size)
         ON CONFLICT(namespace, key) DO UPDATE SET
            value = excluded.value,
            value_type = excluded.value_type,
            created_at = excluded.created_at,
            expires_at = excluded.expires_at,
            last_accessed = excluded.last_accessed,
            access_count = 0,
            size_bytes = excluded.size_bytes`,
      )
      .run({
        ns: this.namespace,
        key,
        value: bytes,
        vtype: typeCode,
        now,
        expiresAt,
        size: bytes.length,
      });

    this.changed("set", [key], 1);
    this.maybeEvict();
    this.maybeOpportunisticMaintenance();
  }

  /**
   * Reads a value, or `defaultValue` (default `undefined`) on a miss or expiry. Never throws for
   * a missing key.
   */
  get<T = unknown>(key: string, defaultValue?: T, options?: GetOptions): T | undefined {
    this.requireOpen();
    const now = nowMs();
    let row: RawRow | undefined;
    try {
      row = this.db
        .prepare("SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?")
        .get(this.namespace, key) as RawRow | undefined;
    } catch (err) {
      if (this.strict) {
        throw new LyteCacheError(`get(${JSON.stringify(key)}) failed: ${String(err)}`, {
          cause: err,
        });
      }
      this.logger(`get(${JSON.stringify(key)}) failed, treating as a miss: ${String(err)}`);
      this.misses += 1;
      return defaultValue;
    }

    if (row === undefined) {
      this.misses += 1;
      this.maybeOpportunisticMaintenance();
      return defaultValue;
    }
    if (row.expires_at !== null && row.expires_at <= now) {
      this.misses += 1;
      this.deleteExpiredRow(key, now);
      this.maybeOpportunisticMaintenance();
      return defaultValue;
    }

    this.hits += 1;
    this.bufferLru(key, now);
    this.maybeOpportunisticMaintenance();

    try {
      return deserialize(row.value, row.value_type, options) as T;
    } catch (err) {
      if (this.strict) throw err;
      this.logger(`failed to deserialize key ${JSON.stringify(key)}: ${String(err)}`);
      return defaultValue;
    }
  }

  /** Deletes keys; returns the number actually deleted. */
  delete(...keys: string[]): number {
    this.requireOpen();
    if (keys.length === 0) return 0;
    const placeholders = keys.map(() => "?").join(",");
    const result = this.db
      .prepare(`DELETE FROM cache WHERE namespace = ? AND key IN (${placeholders})`)
      .run(this.namespace, ...keys);
    for (const key of keys) this.lruBuffer.delete(key);
    if (result.changes > 0) this.changed("delete", [...new Set(keys)], result.changes);
    return result.changes;
  }

  /** Whether a (non-expired) key is present. */
  exists(key: string): boolean {
    this.requireOpen();
    const now = nowMs();
    const row = this.db
      .prepare(
        "SELECT 1 FROM cache WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)",
      )
      .get(this.namespace, key, now);
    return row !== undefined;
  }

  /** Sets a value only if absent (atomic `SET NX`). Returns whether it was set. */
  add(key: string, value: unknown, options: SetOptions = {}): boolean {
    return this.addInternal(key, value, options, true);
  }

  private addInternal(key: string, value: unknown, options: SetOptions, emit: boolean): boolean {
    this.requireOpen();
    const { bytes, typeCode } = serialize(value);
    const now = nowMs();
    const expiresAt = options.ttl === undefined ? null : now + Math.round(options.ttl * 1000);

    if (this.eviction === "noeviction" && this.hasCapacityLimits()) {
      this.checkCapacityBeforeWrite(key);
    }

    // Single atomic UPSERT: the DO UPDATE branch only fires (and only "wins") when the existing
    // row is already expired, so a fresh key and an expired key both insert/overwrite in one
    // statement -- no separate check-then-write race between processes.
    const result = this.db
      .prepare(
        `INSERT INTO cache
            (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
         VALUES (@ns, @key, @value, @vtype, @now, @expiresAt, @now, 0, @size)
         ON CONFLICT(namespace, key) DO UPDATE SET
            value = excluded.value,
            value_type = excluded.value_type,
            created_at = excluded.created_at,
            expires_at = excluded.expires_at,
            last_accessed = excluded.last_accessed,
            access_count = 0,
            size_bytes = excluded.size_bytes
         WHERE cache.expires_at IS NOT NULL AND cache.expires_at <= @now`,
      )
      .run({
        ns: this.namespace,
        key,
        value: bytes,
        vtype: typeCode,
        now,
        expiresAt,
        size: bytes.length,
      });

    const won = result.changes === 1;
    if (won) {
      if (emit) this.changed("set", [key], 1);
      this.maybeEvict();
    }
    return won;
  }

  /** Sets a value only if present (atomic `SET XX`). Returns whether it was replaced. */
  replace(key: string, value: unknown, options: SetOptions = {}): boolean {
    this.requireOpen();
    const { bytes, typeCode } = serialize(value);
    const now = nowMs();
    const expiresAt = options.ttl === undefined ? null : now + Math.round(options.ttl * 1000);

    // A single UPDATE with the existence/expiry check in the WHERE clause is already atomic; no
    // separate check-then-write is needed.
    const result = this.db
      .prepare(
        `UPDATE cache SET value = @value, value_type = @vtype, created_at = @now, expires_at = @expiresAt,
            last_accessed = @now, access_count = 0, size_bytes = @size
         WHERE namespace = @ns AND key = @key AND (expires_at IS NULL OR expires_at > @now)`,
      )
      .run({
        ns: this.namespace,
        key,
        value: bytes,
        vtype: typeCode,
        now,
        expiresAt,
        size: bytes.length,
      });
    if (result.changes > 0) this.changed("set", [key], 1);
    return result.changes > 0;
  }

  /** Atomically reads and replaces a value, returning the old one (or `undefined` if absent/expired). */
  getSet(key: string, value: unknown): unknown {
    this.requireOpen();
    const { bytes, typeCode } = serialize(value);
    const now = nowMs();

    const txn = this.db.transaction(() => {
      const row = this.db
        .prepare("SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?")
        .get(this.namespace, key) as RawRow | undefined;
      const oldValue =
        row !== undefined && (row.expires_at === null || row.expires_at > now)
          ? deserialize(row.value, row.value_type)
          : undefined;

      this.db
        .prepare(
          `INSERT INTO cache
              (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
           VALUES (@ns, @key, @value, @vtype, @now, NULL, @now, 0, @size)
           ON CONFLICT(namespace, key) DO UPDATE SET
              value = excluded.value,
              value_type = excluded.value_type,
              created_at = excluded.created_at,
              expires_at = NULL,
              last_accessed = excluded.last_accessed,
              access_count = 0,
              size_bytes = excluded.size_bytes`,
        )
        .run({ ns: this.namespace, key, value: bytes, vtype: typeCode, now, size: bytes.length });

      return oldValue;
    });
    const old = txn.immediate();
    this.changed("set", [key], 1);
    return old;
  }

  /** Sets every entry, in a single transaction. */
  setMany(entries: Record<string, unknown>, options: SetOptions = {}): void {
    this.requireOpen();
    const keys = Object.keys(entries);
    if (keys.length === 0) return;
    const now = nowMs();
    const expiresAt = options.ttl === undefined ? null : now + Math.round(options.ttl * 1000);

    const stmt = this.db.prepare(
      `INSERT INTO cache
          (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
       VALUES (@ns, @key, @value, @vtype, @now, @expiresAt, @now, 0, @size)
       ON CONFLICT(namespace, key) DO UPDATE SET
          value = excluded.value,
          value_type = excluded.value_type,
          created_at = excluded.created_at,
          expires_at = excluded.expires_at,
          last_accessed = excluded.last_accessed,
          access_count = 0,
          size_bytes = excluded.size_bytes`,
    );
    const txn = this.db.transaction((rows: string[]) => {
      for (const key of rows) {
        const { bytes, typeCode } = serialize(entries[key]);
        stmt.run({ ns: this.namespace, key, value: bytes, vtype: typeCode, now, expiresAt, size: bytes.length });
      }
    });
    txn.immediate(keys);
    // ONE change record for the whole batch, never one per key.
    this.changed("set", keys, keys.length);
    this.maybeEvict();
  }

  /** Gets multiple values as a `Map`; missing/expired keys are simply absent from the result. */
  getMany(keys: string[]): Map<string, unknown> {
    this.requireOpen();
    const result = new Map<string, unknown>();
    if (keys.length === 0) return result;
    const now = nowMs();
    const stmt = this.db.prepare(
      "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?",
    );
    let hits = 0;
    for (const key of keys) {
      const row = stmt.get(this.namespace, key) as RawRow | undefined;
      if (row === undefined) continue;
      if (row.expires_at !== null && row.expires_at <= now) continue;
      result.set(key, deserialize(row.value, row.value_type));
      this.bufferLru(key, now);
      hits += 1;
    }
    this.hits += hits;
    this.misses += keys.length - hits;
    return result;
  }

  // -- expiration -------------------------------------------------------------

  /** Sets (or overwrites) a key's TTL, in seconds. Returns whether the key exists (and isn't already expired). */
  expire(key: string, ttlSeconds: number): boolean {
    this.requireOpen();
    const now = nowMs();
    const expiresAt = now + Math.round(ttlSeconds * 1000);
    const result = this.db
      .prepare(
        "UPDATE cache SET expires_at = ? WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)",
      )
      .run(expiresAt, this.namespace, key, now);
    return result.changes > 0;
  }

  /** Removes a key's TTL, if any. Returns whether the key existed with a TTL to remove. */
  persist(key: string): boolean {
    this.requireOpen();
    const now = nowMs();
    const result = this.db
      .prepare(
        "UPDATE cache SET expires_at = NULL WHERE namespace = ? AND key = ? AND expires_at IS NOT NULL AND expires_at > ?",
      )
      .run(this.namespace, key, now);
    return result.changes > 0;
  }

  /** Seconds remaining, `-1` if no expiry, `undefined` if the key doesn't exist (or already expired). */
  ttl(key: string): number | undefined {
    this.requireOpen();
    const now = nowMs();
    const row = this.db
      .prepare("SELECT expires_at FROM cache WHERE namespace = ? AND key = ?")
      .get(this.namespace, key) as { expires_at: number | null } | undefined;
    if (row === undefined) return undefined;
    if (row.expires_at === null) return -1;
    const remainingMs = row.expires_at - now;
    if (remainingMs <= 0) return undefined;
    return remainingMs / 1000;
  }

  /** Refreshes a key's TTL (sliding expiration). Equivalent to {@link expire}. */
  touch(key: string, ttlSeconds: number): boolean {
    return this.expire(key, ttlSeconds);
  }

  // -- atomic counters ----------------------------------------------------------

  /** Atomically adds `amount` (default 1) to an integer counter, creating it at 0 if absent. */
  incr(key: string, amount: number | bigint = 1): number | bigint {
    this.requireOpen();
    const text = this.atomicIncr(key, amount, TypeCode.INT, [TypeCode.INT]);
    return decodeIntText(text);
  }

  /** Atomically subtracts `amount` (default 1) from an integer counter. */
  decr(key: string, amount: number | bigint = 1): number | bigint {
    return this.incr(key, -amount);
  }

  /** Atomically adds `amount` to a float counter, creating it at 0 if absent. */
  incrFloat(key: string, amount: number): number {
    this.requireOpen();
    if (!Number.isFinite(amount)) {
      throw new TypeError("incrFloat amount must be a finite number");
    }
    const text = this.atomicIncr(key, amount, TypeCode.FLOAT, [TypeCode.INT, TypeCode.FLOAT]);
    return Number(text);
  }

  /**
   * Performs the atomic single-statement UPSERT that adds `amount` to the existing numeric value
   * (or 0 if absent/expired), mirroring the Python and Java implementations' SQL exactly so a
   * single SQLite statement -- not a JS-side read-modify-write -- is the unit of atomicity shared
   * across processes.
   */
  private atomicIncr(
    key: string,
    amount: number | bigint,
    resultType: number,
    allowedTypes: number[],
  ): string {
    const now = nowMs();
    const initialBytes = Buffer.from(amount.toString(), "utf8");
    const inClause = `(${allowedTypes.join(",")})`;
    // better-sqlite3 binds a plain JS `number` as SQLite REAL, never INTEGER -- so for an integer
    // result, `amount` must be bound as a BigInt (which binds as INTEGER); otherwise SQLite's
    // `CAST(value AS TEXT) + amount` promotes to REAL (e.g. producing "2.0" instead of "2"),
    // corrupting the decimal-text wire format this type code is supposed to hold.
    const boundAmount = resultType === TypeCode.INT ? BigInt(amount) : Number(amount);

    const sql = `
      INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
      VALUES (@ns, @key, @blob, @rtype, @now, NULL, @now, 0, @size)
      ON CONFLICT(namespace, key) DO UPDATE SET
        value = CAST(CAST(
            (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN 0
                  ELSE CAST(cache.value AS TEXT) END) + @amount
            AS TEXT) AS BLOB),
        value_type = @rtype,
        expires_at = CASE
            WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN NULL
            ELSE cache.expires_at
        END,
        last_accessed = @now,
        size_bytes = LENGTH(CAST(
            (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN 0
                  ELSE CAST(cache.value AS TEXT) END) + @amount
            AS TEXT))
      WHERE (cache.expires_at IS NOT NULL AND cache.expires_at <= @now)
         OR cache.value_type IN ${inClause}
    `;

    const txn = this.db.transaction(() => {
      const result = this.db.prepare(sql).run({
        ns: this.namespace,
        key,
        blob: initialBytes,
        rtype: resultType,
        now,
        size: initialBytes.length,
        amount: boundAmount,
      });
      if (result.changes === 0) {
        throw new TypeError(
          `value for key ${JSON.stringify(key)} is not ${resultType === TypeCode.FLOAT ? "a number" : "an integer"}`,
        );
      }
      const row = this.db
        .prepare("SELECT value FROM cache WHERE namespace = ? AND key = ?")
        .get(this.namespace, key) as { value: Buffer } | undefined;
      if (row === undefined) {
        throw new LyteCacheError("unexpected: key not found after incr upsert");
      }
      return row.value.toString("utf8");
    });
    const text = txn.immediate();
    this.changed("set", [key], 1);
    return text;
  }

  // -- hashes: field-level access to JSON object values -------------------------
  //
  // A hash is not a separate type: it is a view over an ordinary value_type-4 row whose JSON root
  // is an object. Anything written with set(key, {...}) is usable with hget, and get() on a hash
  // returns the whole document. The reason these exist is atomicity: on the JSON1 path every write
  // is computed inside SQLite from the row's *current* value (json_set/json_remove), so two
  // processes updating different fields never lose each other's write. Where an exact return value
  // needs more than one statement (hset's "created", hdel's count), guarded writes run inside one
  // IMMEDIATE transaction; no write depends on data read into JS. The read-modify-write fallback
  // (SQLite without JSON1, or LYTECACHE_FORCE_JSON_FALLBACK=1) holds the write lock throughout.

  private detectJson1(): boolean {
    if (process.env[FORCE_JSON_FALLBACK_ENV]) {
      this.logger(`${FORCE_JSON_FALLBACK_ENV} is set; hash operations use the read-modify-write fallback`);
      return false;
    }
    try {
      const row = this.db
        .prepare(String.raw`SELECT json_valid('{}') AS ok, '{"a\"b":[1]}' -> '$."a\"b"' AS arr`)
        .get() as { ok: number; arr: string };
      if (row.ok === 1 && row.arr === "[1]") return true;
    } catch {
      // fall through
    }
    const version = (this.db.prepare("SELECT sqlite_version() AS v").get() as { v: string }).v;
    this.logger(
      `SQLite ${version} lacks the JSON1 support hash operations need (SQLite >= 3.38); falling back ` +
        "to a slower read-modify-write under an IMMEDIATE transaction",
    );
    return false;
  }

  private hashParams(key: string, now: number, ttl: number | undefined): Record<string, unknown> {
    return {
      ns: this.namespace,
      key,
      now,
      exp: ttl === undefined ? null : now + Math.round(ttl * 1000),
    };
  }

  private wrongType(key: string, valueType: number, rootType: string | null): SerializationError {
    if (valueType !== TypeCode.JSON) {
      return new SerializationError(
        `key ${JSON.stringify(key)} holds a value of type ${TYPE_NAMES[valueType] ?? "unknown"} ` +
          `(value_type=${valueType}), not a JSON object; hash operations apply only to JSON objects (value_type=4)`,
      );
    }
    if (rootType === null) {
      return new SerializationError(`key ${JSON.stringify(key)} holds malformed JSON (value_type=4)`);
    }
    return new SerializationError(
      `key ${JSON.stringify(key)} holds a JSON ${rootType}, not a JSON object; hash operations apply only to JSON objects`,
    );
  }

  private checkHashType(key: string, now: number): void {
    const row = this.db
      .prepare(`SELECT cache.value_type AS vt, ${ROOT_TYPE} AS root FROM cache WHERE namespace = @ns AND key = @key AND ${LIVE}`)
      .get({ ns: this.namespace, key, now }) as { vt: number; root: string | null } | undefined;
    if (row !== undefined && !(row.vt === TypeCode.JSON && row.root === "object")) {
      throw this.wrongType(key, row.vt, row.root);
    }
  }

  private loadHash(key: string, now: number): JsonObject | undefined {
    const row = this.db
      .prepare(`SELECT value, value_type FROM cache WHERE namespace = @ns AND key = @key AND ${LIVE}`)
      .get({ ns: this.namespace, key, now }) as { value: Buffer; value_type: number } | undefined;
    if (row === undefined) return undefined;
    if (row.value_type !== TypeCode.JSON) throw this.wrongType(key, row.value_type, null);
    let parsed: ReturnType<typeof parseJsonObject>;
    try {
      parsed = parseJsonObject(row.value.toString("utf8"));
    } catch {
      throw this.wrongType(key, TypeCode.JSON, null);
    }
    if (parsed.object === undefined) throw this.wrongType(key, TypeCode.JSON, parsed.root);
    return parsed.object;
  }

  /** Fallback path: read, mutate in JS, write back -- all under BEGIN IMMEDIATE. */
  private rmwHash<R>(
    key: string,
    ttl: number | undefined,
    create: boolean,
    mutate: (doc: JsonObject) => [changed: boolean, result: R],
  ): R {
    const now = nowMs();
    let wrote = false;
    const result = this.db
      .transaction((): R => {
        let doc = this.loadHash(key, now);
        const exists = doc !== undefined;
        if (doc === undefined) {
          if (!create) return mutate(new JsonObject())[1];
          if (this.eviction === "noeviction" && this.hasCapacityLimits()) this.checkCapacityBeforeWrite(key);
          doc = new JsonObject();
        }
        const [changed, r] = mutate(doc);
        if (!changed) return r;
        const blob = Buffer.from(doc.toJSON(), "utf8");
        if (exists) {
          this.db
            .prepare(
              `UPDATE cache SET value = @v, size_bytes = @size, last_accessed = @now, access_count = access_count + 1
               WHERE namespace = @ns AND key = @key`,
            )
            .run({ v: blob, size: blob.length, now, ns: this.namespace, key });
        } else {
          this.db
            .prepare(
              `INSERT OR REPLACE INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
               VALUES (@ns, @key, @v, 4, @now, @exp, @now, 0, @size)`,
            )
            .run({ ...this.hashParams(key, now, ttl), v: blob, size: blob.length });
        }
        wrote = true;
        return r;
      })
      .immediate();
    if (wrote) {
      this.changed("set", [key], 1);
      this.maybeEvict();
    }
    return result;
  }

  /**
   * Atomically sets one field of the JSON object at `key`. Returns `true` if the field was new,
   * `false` if it replaced a value. A missing (or expired) key is created as a new object with **no
   * expiry** unless `options.ttl` is given; `ttl` is ignored for an existing key, whose expiry a
   * field write never changes. Throws {@link SerializationError} if `key` holds anything but a JSON
   * object.
   */
  hset(key: string, field: string, value: unknown, options: SetOptions = {}): boolean {
    this.requireOpen();
    requireField(field);
    const encoded = encodeHashValue(value);
    if (!this.json1) {
      return this.rmwHash(key, options.ttl, true, (doc) => {
        const created = !doc.has(field);
        doc.set(field, encoded);
        return [true, created];
      });
    }
    const now = nowMs();
    const params = { ...this.hashParams(key, now, options.ttl), f0: field, v0: encoded };
    const setExpr = `json_set(${DOC}, ${path("f0")}, json(@v0))`;
    const created = this.db
      .transaction((): boolean => {
        if (this.eviction === "noeviction" && this.hasCapacityLimits()) this.checkCapacityBeforeWrite(key);
        const first = this.db
          .prepare(upsertHashSql(setExpr, "json_object(@f0, json(@v0))", `(${fieldType("f0")} IS NULL AND ${IS_OBJ})`))
          .run(params);
        if (first.changes === 1) return true;
        const second = this.db
          .prepare(
            `UPDATE cache SET value = CAST(${setExpr} AS BLOB), size_bytes = length(CAST(${setExpr} AS BLOB)),
               last_accessed = @now, access_count = access_count + 1
             WHERE namespace = @ns AND key = @key AND ${LIVE} AND ${IS_OBJ}`,
          )
          .run(params);
        if (second.changes === 0) this.checkHashType(key, now);
        return false;
      })
      .immediate();
    this.changed("set", [key], 1);
    this.maybeEvict();
    this.maybeOpportunisticMaintenance();
    return created;
  }

  /** Sets a field only if it is absent, atomically; returns whether it was set. */
  hsetnx(key: string, field: string, value: unknown, options: SetOptions = {}): boolean {
    this.requireOpen();
    requireField(field);
    const encoded = encodeHashValue(value);
    if (!this.json1) {
      return this.rmwHash(key, options.ttl, true, (doc) => {
        if (doc.has(field)) return [false, false];
        doc.set(field, encoded);
        return [true, true];
      });
    }
    const now = nowMs();
    const params = { ...this.hashParams(key, now, options.ttl), f0: field, v0: encoded };
    const won = this.db
      .transaction((): boolean => {
        if (this.eviction === "noeviction" && this.hasCapacityLimits()) this.checkCapacityBeforeWrite(key);
        const res = this.db
          .prepare(
            upsertHashSql(
              `json_set(${DOC}, ${path("f0")}, json(@v0))`,
              "json_object(@f0, json(@v0))",
              `(${fieldType("f0")} IS NULL AND ${IS_OBJ})`,
            ),
          )
          .run(params);
        if (res.changes === 1) return true;
        this.checkHashType(key, now);
        return false;
      })
      .immediate();
    if (won) {
      this.changed("set", [key], 1);
      this.maybeEvict();
    }
    return won;
  }

  /** Sets several fields at once, atomically (Redis' multi-field HSET). */
  hsetMany(key: string, fields: Record<string, unknown>, options: SetOptions = {}): void {
    this.requireOpen();
    const entries = Object.entries(fields).map(([f, v]): [string, string] => {
      requireField(f);
      return [f, encodeHashValue(v)];
    });
    if (entries.length === 0) return;
    if (!this.json1) {
      this.rmwHash(key, options.ttl, true, (doc) => {
        for (const [f, v] of entries) doc.set(f, v);
        return [true, undefined];
      });
      return;
    }
    const now = nowMs();
    this.db
      .transaction(() => {
        if (this.eviction === "noeviction" && this.hasCapacityLimits()) this.checkCapacityBeforeWrite(key);
        // SQLite caps function arguments and bound variables, so a large mapping is applied as
        // several statements -- each json_set nested over groups of pairs -- in this transaction.
        for (let start = 0; start < entries.length; start += HSET_MANY_PER_STATEMENT) {
          const chunk = entries.slice(start, start + HSET_MANY_PER_STATEMENT);
          const params: Record<string, unknown> = this.hashParams(key, now, options.ttl);
          let liveDoc = DOC;
          let freshDoc = "'{}'";
          for (let g = 0; g < chunk.length; g += JSON_SET_PAIRS) {
            const pairs: string[] = [];
            for (let i = g; i < Math.min(g + JSON_SET_PAIRS, chunk.length); i++) {
              params[`f${i}`] = chunk[i]![0];
              params[`v${i}`] = chunk[i]![1];
              pairs.push(`${path(`f${i}`)}, json(@v${i})`);
            }
            liveDoc = `json_set(${liveDoc}, ${pairs.join(", ")})`;
            freshDoc = `json_set(${freshDoc}, ${pairs.join(", ")})`;
          }
          const res = this.db.prepare(upsertHashSql(liveDoc, freshDoc, IS_OBJ)).run(params);
          if (res.changes === 0) this.checkHashType(key, now);
        }
      })
      .immediate();
    this.changed("set", [key], 1);
    this.maybeEvict();
  }

  /**
   * Returns one field of the JSON object at `key`, or `defaultValue` on a missing key, missing
   * field or expired key -- never throws on a miss. A stored JSON `null` is returned as `null`.
   */
  hget<T = unknown>(key: string, field: string, defaultValue?: T): T | undefined {
    const [value] = this.hmgetRaw(key, [field]);
    return value === undefined ? defaultValue : (JSON.parse(value) as T);
  }

  /** Several fields, in order, with `undefined` for each one that is missing. */
  hmget(key: string, fields: string[]): unknown[] {
    return this.hmgetRaw(key, fields).map((v) => (v === undefined ? undefined : JSON.parse(v)));
  }

  private hmgetRaw(key: string, fields: string[]): (string | undefined)[] {
    this.requireOpen();
    for (const f of fields) requireField(f);
    if (fields.length === 0) return [];
    const now = nowMs();
    if (!this.json1) {
      const doc = this.loadHash(key, now);
      this.recordHashRead(key, now, doc !== undefined);
      return fields.map((f) => doc?.get(f));
    }
    const params: Record<string, unknown> = { ns: this.namespace, key, now };
    const cols = fields.map((f, i) => {
      params[`f${i}`] = f;
      return `CASE WHEN ${IS_OBJ} THEN ${DOC} -> ${path(`f${i}`)} END AS c${i}`;
    });
    const row = this.db
      .prepare(
        `SELECT cache.value_type AS vt, ${ROOT_TYPE} AS root, ${cols.join(", ")}
         FROM cache WHERE namespace = @ns AND key = @key AND ${LIVE}`,
      )
      .get(params) as Record<string, unknown> | undefined;
    if (row === undefined) {
      this.recordHashRead(key, now, false);
      return fields.map(() => undefined);
    }
    if (!(row.vt === TypeCode.JSON && row.root === "object")) {
      throw this.wrongType(key, row.vt as number, row.root as string | null);
    }
    this.recordHashRead(key, now, true);
    return fields.map((_, i) => (row[`c${i}`] ?? undefined) as string | undefined);
  }

  /** The whole object at `key` ({} if missing). */
  hgetAll(key: string): Record<string, unknown> {
    this.requireOpen();
    const now = nowMs();
    const doc = this.loadHash(key, now);
    this.recordHashRead(key, now, doc !== undefined);
    const out: Record<string, unknown> = {};
    for (const [k, v] of doc?.fields ?? []) out[k] = JSON.parse(v);
    return out;
  }

  /**
   * Removes fields; returns how many were present and removed. Unlike Redis, removing the last
   * field leaves an empty object `{}` rather than deleting the key. A missing key returns 0.
   */
  hdel(key: string, ...fields: string[]): number {
    this.requireOpen();
    const unique = [...new Set(fields)];
    for (const f of unique) requireField(f);
    if (unique.length === 0) return 0;
    if (!this.json1) {
      return this.rmwHash(key, undefined, false, (doc) => {
        const n = unique.filter((f) => doc.delete(f)).length;
        return [n > 0, n];
      });
    }
    const now = nowMs();
    const removeExpr = `json_remove(${DOC}, ${path("f0")})`;
    const stmt = this.db.prepare(
      `UPDATE cache SET value = CAST(${removeExpr} AS BLOB), size_bytes = length(CAST(${removeExpr} AS BLOB)),
         last_accessed = @now, access_count = access_count + @bump
       WHERE namespace = @ns AND key = @key AND ${LIVE} AND ${fieldType("f0")} IS NOT NULL`,
    );
    const removed = this.db
      .transaction((): number => {
        let n = 0;
        for (const f of unique) {
          // One hdel call is one access, however many statements it takes.
          n += stmt.run({ ns: this.namespace, key, now, f0: f, bump: n === 0 ? 1n : 0n }).changes;
        }
        if (n === 0) this.checkHashType(key, now);
        return n;
      })
      .immediate();
    if (removed > 0) this.changed("set", [key], 1);
    return removed;
  }

  /** Whether `key` is a live JSON object containing `field`. */
  hexists(key: string, field: string): boolean {
    return this.hmgetRaw(key, [field])[0] !== undefined;
  }

  /** Field names in document order ([] if missing). */
  hkeys(key: string): string[] {
    return this.hashEntries(key, false).map(([k]) => k);
  }

  /** Field values in document order ([] if missing). */
  hvals(key: string): unknown[] {
    return this.hashEntries(key, true).map(([, v]) => JSON.parse(v));
  }

  /** Number of fields (0 if missing). */
  hlen(key: string): number {
    this.requireOpen();
    const now = nowMs();
    if (!this.json1) {
      const doc = this.loadHash(key, now);
      if (doc !== undefined) this.bufferLru(key, now);
      return doc?.fields.length ?? 0;
    }
    const row = this.db
      .prepare(
        `SELECT cache.value_type AS vt, ${ROOT_TYPE} AS root,
           CASE WHEN ${IS_OBJ} THEN (SELECT count(*) FROM json_each(${DOC})) END AS n
         FROM cache WHERE namespace = @ns AND key = @key AND ${LIVE}`,
      )
      .get({ ns: this.namespace, key, now }) as { vt: number; root: string | null; n: number } | undefined;
    if (row === undefined) return 0;
    if (!(row.vt === TypeCode.JSON && row.root === "object")) throw this.wrongType(key, row.vt, row.root);
    this.bufferLru(key, now);
    return row.n;
  }

  private hashEntries(key: string, withValues: boolean): [string, string][] {
    this.requireOpen();
    const now = nowMs();
    if (!this.json1) {
      const doc = this.loadHash(key, now);
      if (doc !== undefined) this.bufferLru(key, now);
      return doc?.fields ?? [];
    }
    const rows = this.db
      .prepare(
        `SELECT cache.value_type AS vt, ${ROOT_TYPE} AS root, j.key AS k, ${withValues ? `CASE WHEN j.key IS NOT NULL THEN ${DOC} -> j.fullkey END` : "NULL"} AS v
         FROM cache LEFT JOIN json_each(CASE WHEN ${IS_OBJ} THEN ${DOC} ELSE '{}' END) AS j
         WHERE cache.namespace = @ns AND cache.key = @key AND ${LIVE}`,
      )
      .all({ ns: this.namespace, key, now }) as { vt: number; root: string | null; k: string | null; v: string | null }[];
    if (rows.length === 0) return [];
    const first = rows[0]!;
    if (!(first.vt === TypeCode.JSON && first.root === "object")) throw this.wrongType(key, first.vt, first.root);
    this.bufferLru(key, now);
    return rows.filter((r) => r.k !== null).map((r) => [r.k!, r.v ?? "null"]);
  }

  /**
   * Atomically adds `amount` to an integer field and returns the new value (a `bigint` beyond
   * `Number.MAX_SAFE_INTEGER`). A missing field starts at 0; a missing key is created (no expiry
   * unless `options.ttl`). Throws `TypeError` if the field holds anything but a JSON integer, and
   * `RangeError` -- writing nothing -- if the result would leave the signed 64-bit range.
   */
  hincrby(key: string, field: string, amount: number | bigint = 1, options: SetOptions = {}): number | bigint {
    this.requireOpen();
    requireField(field);
    if (typeof amount === "number" && !Number.isInteger(amount)) {
      throw new TypeError("hincrby amount must be an integer");
    }
    const big = BigInt(amount);
    if (big > INT64_MAX || big < INT64_MIN) throw new RangeError(`hincrby amount ${big} is outside the int64 range`);
    return decodeIntText(this.hincr(key, field, big, options.ttl, true));
  }

  /** Atomically adds `amount` to a numeric field and returns the new value, stored as a float. */
  hincrbyfloat(key: string, field: string, amount: number, options: SetOptions = {}): number {
    this.requireOpen();
    requireField(field);
    if (typeof amount !== "number" || !Number.isFinite(amount)) {
      throw new TypeError("hincrbyfloat amount must be a finite number");
    }
    return Number(this.hincr(key, field, amount, options.ttl, false));
  }

  private hincr(key: string, field: string, amount: number | bigint, ttl: number | undefined, integer: boolean): string {
    const kind = integer ? "an integer" : "a number";
    if (!this.json1) {
      return this.rmwHash(key, ttl, true, (doc) => {
        const raw = doc.get(field) ?? "0";
        if (integer) {
          if (!/^-?\d+$/.test(raw.trim())) {
            throw new TypeError(`hash field ${JSON.stringify(field)} of key ${JSON.stringify(key)} is not ${kind}`);
          }
          const next = BigInt(raw.trim()) + (amount as bigint);
          if (next > INT64_MAX || next < INT64_MIN) {
            throw new RangeError(`incrementing field ${JSON.stringify(field)} of key ${JSON.stringify(key)} would overflow int64`);
          }
          doc.set(field, next.toString());
          return [true, next.toString()];
        }
        const trimmed = raw.trim();
        if (!/^-?\d/.test(trimmed)) {
          throw new TypeError(`hash field ${JSON.stringify(field)} of key ${JSON.stringify(key)} is not ${kind}`);
        }
        const next = Number(trimmed) + (amount as number);
        if (!Number.isFinite(next)) {
          throw new RangeError(`incrementing field ${JSON.stringify(field)} of key ${JSON.stringify(key)} would overflow`);
        }
        const text = formatJsonFloat(next);
        doc.set(field, text);
        return [true, text];
      });
    }
    const now = nowMs();
    const params = { ...this.hashParams(key, now, ttl), f0: field, amount };
    const stored = `json_extract(${DOC}, ${path("f0")})`;
    const newVal = integer ? `(COALESCE(${stored}, 0) + @amount)` : `(CAST(COALESCE(${stored}, 0) AS REAL) + @amount)`;
    const guard = integer
      ? `COALESCE(${fieldType("f0")}, 'integer') = 'integer' AND typeof(${newVal}) = 'integer'`
      : `COALESCE(${fieldType("f0")}, 'real') IN ('integer', 'real') AND ${newVal} BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308`;
    const sql =
      upsertHashSql(`json_set(${DOC}, ${path("f0")}, ${integer ? newVal : `json(${floatText(newVal)})`})`, "json_object(@f0, @amount)", `(CASE WHEN ${IS_OBJ} THEN (${guard}) ELSE 0 END)`) +
      (integer
        ? ` RETURNING CAST(json_extract(CAST(value AS TEXT), ${path("f0")}) AS TEXT) AS r`
        : ` RETURNING json_extract(CAST(value AS TEXT), ${path("f0")}) AS r`);
    const result = this.db
      .transaction((): string => {
        if (this.eviction === "noeviction" && this.hasCapacityLimits()) this.checkCapacityBeforeWrite(key);
        // Integers come back as exact decimal text (int64 can exceed 2^53); floats as the REAL
        // itself, since CAST(real AS TEXT) keeps only 15 significant digits.
        const row = this.db.prepare(sql).get(params) as { r: string | number } | undefined;
        if (row !== undefined) return String(row.r);
        this.checkHashType(key, now);
        const ft = (
          this.db.prepare(`SELECT ${fieldType("f0")} AS t FROM cache WHERE namespace = @ns AND key = @key`).get(params) as {
            t: string | null;
          }
        ).t;
        if ((integer && ft === "integer") || (!integer && (ft === "integer" || ft === "real"))) {
          throw new RangeError(`incrementing field ${JSON.stringify(field)} of key ${JSON.stringify(key)} would overflow`);
        }
        throw new TypeError(`hash field ${JSON.stringify(field)} of key ${JSON.stringify(key)} holds a JSON ${ft}, not ${kind}`);
      })
      .immediate();
    this.changed("set", [key], 1);
    this.maybeEvict();
    return result;
  }

  private recordHashRead(key: string, now: number, found: boolean): void {
    if (found) {
      this.hits += 1;
      this.bufferLru(key, now);
    } else {
      this.misses += 1;
    }
    this.maybeOpportunisticMaintenance();
  }

  /** "json1" (single SQL statements) or "fallback" (read-modify-write under BEGIN IMMEDIATE). */
  get hashMode(): "json1" | "fallback" {
    return this.json1 ? "json1" : "fallback";
  }

  // -- events: local notification (notify / watch) ---------------------------------
  //
  // SINGLE HOST ONLY. POLLED, NOT PUSHED. DURABLE. Not Redis Pub/Sub: events are rows in the
  // `events` table with a monotonic seq; watchers on the same machine poll it (every 50 ms by
  // default, backing off to ~1 s when idle). See docs/notifications.md.

  private ensureEvents(): void {
    if (this.eventsReady) return;
    this.db.exec(EVENTS_DDL);
    this.eventsReady = true;
  }

  private eventsExist(): boolean {
    if (!this.eventsReady) {
      this.eventsReady =
        this.db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'").get() !== undefined;
    }
    return this.eventsReady;
  }

  /**
   * Publishes an event for watchers **on this host** and returns its `seq`. Single host only,
   * delivered by polling (not pushed), durable until retention deletes it (`eventRetention`,
   * default 1 hour). `payload` is encoded exactly like a cached value. Each call is one write under
   * the same lock as `set`, so a high event rate slows cache writes; batch with {@link notifyMany}.
   * Call it after your own database transaction commits, never inside one you might roll back.
   */
  notify(channel: string, payload: unknown = null): number {
    this.requireOpen();
    validateChannel(channel);
    return this.insertEvents([[channel, payload]])[0]!;
  }

  /** Publishes several `[channel, payload]` events in one transaction; returns their seqs in order. */
  notifyMany(events: [channel: string, payload: unknown][]): number[] {
    this.requireOpen();
    for (const [channel] of events) validateChannel(channel);
    return events.length === 0 ? [] : this.insertEvents(events);
  }

  private insertEvents(events: [string, unknown][]): number[] {
    const now = nowMs();
    const rows = events.map(([channel, payload]) => ({ channel, ...serialize(payload) }));
    this.ensureEvents();
    const stmt = this.db.prepare(
      "INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES (?, ?, ?, ?, ?)",
    );
    const insertAll = (): number[] =>
      rows.map((r) => Number(stmt.run(r.channel, this.namespace, r.bytes, r.typeCode, now).lastInsertRowid));
    const seqs = rows.length === 1 ? insertAll() : this.db.transaction(insertAll).immediate();
    // In-process optimization only: wake this cache's watchers now instead of at their next poll.
    for (const sub of this.subscriptions) sub.wake();
    return seqs;
  }

  /**
   * Subscribes to events published **on this host** to matching channels: exact names, or a
   * trailing `*` for a prefix (`"cache:invalidate:*"`); `"*"` matches every channel. Only events
   * from this cache's namespace are seen.
   *
   * Single host only; polled (latency about `pollInterval`, up to ~1 s after an idle spell);
   * durable and resumable via `fromSeq` (`undefined`: only new events; `N`: after seq N; `0`:
   * everything retained). Delivery is at-least-once from your cursor, in total seq order, to every
   * subscriber (fan-out; no consumer groups) -- handlers must be idempotent. Lost events surface as
   * a {@link GapError}. Always `close()` the subscription (or `using sub = cache.watch(...)`).
   */
  watch(channels: string | string[], options: WatchOptions = {}): Subscription {
    this.requireOpen();
    const patterns = typeof channels === "string" ? [channels] : channels;
    if (patterns.length === 0) throw new TypeError("watch() needs at least one channel or pattern");
    channelFilter(patterns); // validates
    const { fromSeq } = options;
    if (fromSeq !== undefined && (!Number.isInteger(fromSeq) || fromSeq < 0)) {
      throw new TypeError("fromSeq must be an integer >= 0");
    }
    const pollInterval = options.pollInterval ?? 0.05;
    if (!(pollInterval > 0)) throw new TypeError("pollInterval must be positive");
    this.ensureEvents();
    let start = 0;
    let gap: GapError | null = null;
    this.db
      .transaction(() => {
        if (fromSeq === undefined) {
          start = lastAssignedSeq(this.db);
        } else if (fromSeq === 0) {
          const oldest = (this.db.prepare("SELECT min(seq) AS m FROM events").get() as { m: number | null }).m;
          start = oldest !== null ? oldest - 1 : lastAssignedSeq(this.db);
        } else {
          start = fromSeq;
          gap = gapCheck(this.db, start);
        }
      })
      .deferred();
    const sub = new Subscription({
      path: this.dbPath,
      namespace: this.namespace,
      patterns,
      startSeq: start,
      gap,
      pollIntervalMs: pollInterval * 1000,
      onClose: (s) => this.subscriptions.delete(s),
      logger: this.logger,
    });
    this.subscriptions.add(sub);
    return sub;
  }

  /**
   * Persists a named subscriber cursor (in the `meta` table). Save `event.seq` after you have
   * finished handling an event, then resume with `watch(..., { fromSeq: cache.loadCursor(name) })`.
   * Each save is a write: on a busy stream save every N events, accepting up to N redeliveries.
   */
  saveCursor(name: string, seq: number): void {
    this.requireOpen();
    this.db
      .prepare("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v")
      .run(`cursor:${this.namespace}:${name}`, String(seq));
  }

  /** The cursor last saved under `name`, or `undefined`. */
  loadCursor(name: string): number | undefined {
    this.requireOpen();
    const row = this.db.prepare("SELECT v FROM meta WHERE k = ?").get(`cursor:${this.namespace}:${name}`) as
      | { v: string }
      | undefined;
    return row === undefined ? undefined : Number(row.v);
  }

  /**
   * Retention in bounded batches. Always deletes a *prefix* of the seq range (everything up to the
   * newest event past retention or beyond the count cap), never a hole, which keeps gap detection
   * sound even if clocks disagree.
   */
  private sweepEvents(now: number): void {
    if (!this.eventsExist()) return;
    const bounds: number[] = [];
    const byAge = (this.db.prepare("SELECT max(seq) AS m FROM events WHERE created_at <= ?").get(now - this.eventRetentionMs) as {
      m: number | null;
    }).m;
    if (byAge !== null) bounds.push(byAge);
    if (this.eventMaxCount !== null) {
      const row = this.db.prepare("SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?").get(this.eventMaxCount) as
        | { seq: number }
        | undefined;
      if (row !== undefined) bounds.push(row.seq);
    }
    if (bounds.length === 0) return;
    const bound = Math.max(...bounds);
    const stmt = this.db.prepare("DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)");
    while (stmt.run(bound, BATCH_DELETE_LIMIT).changes >= BATCH_DELETE_LIMIT) {
      // keep deleting in bounded batches
    }
  }

  // -- change listeners (in-process) and auto-publish -------------------------------

  /**
   * Calls `listener(record)` after mutations in **this process** commit. In-process only, no SQLite
   * involvement, effectively zero latency. A record arrives only after its transaction commits (a
   * rolled-back `setMany` notifies nobody), from a `setImmediate` callback -- never inside the
   * write -- so a slow listener never blocks the write and a listener may write to the cache. A
   * throwing (or rejecting) listener is logged and skipped. Synchronous re-entry is limited to 8
   * levels. Bulk operations produce ONE record. Returns an unsubscribe function; once it returns,
   * the listener is never called again.
   *
   * `"set"` covers every write that changes a value (set, setMany, add, replace, getSet, counters,
   * hash writes); `"delete"` is `delete`; `"flush"` is `flush`. TTL expiry and eviction are not
   * reported.
   */
  onChange(
    listener: (record: ChangeRecord) => unknown,
    options: { events?: Iterable<ChangeOp> } = {},
  ): () => void {
    this.requireOpen();
    this.dispatcher ??= new ChangeDispatcher(this.logger);
    return this.dispatcher.add(listener, options.events ?? CHANGE_OPS);
  }

  private changed(op: ChangeOp, keys: string[] | null, count: number): void {
    const toListeners = this.dispatcher !== null && this.dispatcher.wants(op);
    const publish = this.publishOps.has(op);
    if (!toListeners && !publish) return; // the common case: nothing registered
    const truncated = keys === null || keys.length > this.changeKeyCap;
    const record: ChangeRecord = {
      op,
      namespace: this.namespace,
      keys: truncated ? null : [...keys],
      count,
      truncated,
    };
    if (toListeners) this.dispatcher!.submit(record);
    if (publish) {
      try {
        this.insertEvents([[CACHE_CHANNEL, record]]);
      } catch (err) {
        if (this.strict) throw err;
        this.logger(`failed to publish ${op} change event: ${String(err)}`);
      }
    }
  }

  // -- introspection & management -----------------------------------------------

  /** Lazily iterates keys matching a `GLOB` pattern (`*`, `?`, `[...]`), cursor-batched, never
   * loading the whole namespace at once. */
  *keys(pattern = "*"): IterableIterator<string> {
    this.requireOpen();
    const batchSize = 500;
    let lastKey = "";
    const now = nowMs();
    for (;;) {
      const rows = this.db
        .prepare(
          `SELECT key FROM cache
           WHERE namespace = ? AND key GLOB ? AND key > ?
             AND (expires_at IS NULL OR expires_at > ?)
           ORDER BY key LIMIT ?`,
        )
        .all(this.namespace, pattern, lastKey, now, batchSize) as { key: string }[];
      if (rows.length === 0) return;
      for (const row of rows) yield row.key;
      lastKey = rows[rows.length - 1]!.key;
      if (rows.length < batchSize) return;
    }
  }

  /** Clears every entry in the current namespace. */
  flush(): void {
    this.requireOpen();
    const result = this.db.prepare("DELETE FROM cache WHERE namespace = ?").run(this.namespace);
    this.lruBuffer.clear();
    if (result.changes > 0) this.changed("flush", null, result.changes);
  }

  /** A snapshot of cache statistics. */
  stats(): CacheStatsSnapshot {
    this.requireOpen();
    const row = this.db
      .prepare("SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?")
      .get(this.namespace) as { cnt: number; sz: number };
    const total = this.hits + this.misses;
    return {
      hits: this.hits,
      misses: this.misses,
      hitRate: total === 0 ? 0 : this.hits / total,
      keyCount: row.cnt,
      sizeBytes: row.sz,
      evictions: this.evictionsCount,
      expiredRemoved: this.expiredRemovedCount,
      path: this.dbPath,
      hashMode: this.hashMode,
    };
  }

  /** Reclaims disk space (flushes buffered LRU metadata first). */
  vacuum(): void {
    this.requireOpen();
    this.flushLruBuffer();
    this.db.exec("VACUUM");
  }

  /** Shuts down cleanly: flushes buffered LRU metadata, stops the sweeper, and closes the database. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const sub of [...this.subscriptions]) sub.close();
    this.dispatcher?.close();
    this.dispatcher = null;
    if (this.sweeperTimer !== null) {
      clearInterval(this.sweeperTimer);
      this.sweeperTimer = null;
    }
    this.flushLruBuffer();
    this.db.close();
  }

  /** Enables `using cache = new LyteCache()` (TC39 explicit resource management). */
  [Symbol.dispose](): void {
    this.close();
  }

  // -- extras: memoize + lock -----------------------------------------------

  /** Read-through cache: returns the cached value if present, else computes it via `loader`, stores, and returns it. */
  memoize<T>(key: string, ttlSeconds: number | undefined, loader: () => T): T {
    const sentinel = Symbol("miss");
    const cached = this.get<T | symbol>(key, sentinel);
    if (cached !== sentinel) return cached as T;
    const computed = loader();
    this.set(key, computed, ttlSeconds === undefined ? {} : { ttl: ttlSeconds });
    return computed;
  }

  /** Async version of {@link memoize}, for a `loader` that returns a `Promise`. */
  async memoizeAsync<T>(
    key: string,
    ttlSeconds: number | undefined,
    loader: () => Promise<T>,
  ): Promise<T> {
    const sentinel = Symbol("miss");
    const cached = this.get<T | symbol>(key, sentinel);
    if (cached !== sentinel) return cached as T;
    const computed = await loader();
    this.set(key, computed, ttlSeconds === undefined ? {} : { ttl: ttlSeconds });
    return computed;
  }

  /** Wraps a function so repeated calls with the same arguments are memoized (key derived from the function's name and `JSON.stringify(args)`). */
  wrap<Args extends unknown[], R>(
    fn: (...args: Args) => R,
    options: { ttl?: number; keyPrefix?: string } = {},
  ): (...args: Args) => R {
    const prefix = options.keyPrefix ?? fn.name ?? "anonymous";
    return (...args: Args): R => {
      const key = `wrap:${prefix}:${JSON.stringify(args)}`;
      return this.memoize(key, options.ttl, () => fn(...args));
    };
  }

  /** Acquires a process-safe distributed lock. Use with `using` or call `.release()`/`.close()` when done. */
  lock(name: string, options?: LockOptions): CacheLock {
    this.requireOpen();
    return new CacheLock(this, name, options);
  }

  /** @internal Used by {@link CacheLock}; not part of the primary API. */
  tryAcquireLock(lockKey: string, token: string, ttlSeconds: number): boolean {
    return this.addInternal(lockKey, token, { ttl: ttlSeconds }, false);
  }

  /**
   * @internal Used by {@link CacheLock}; not part of the primary API. Atomically deletes the
   * lock only if it's still held by `token`, so a lock that already expired and was re-acquired
   * by someone else is never deleted out from under them.
   */
  releaseLock(lockKey: string, token: string): boolean {
    const txn = this.db.transaction(() => {
      const row = this.db
        .prepare("SELECT value FROM cache WHERE namespace = ? AND key = ?")
        .get(this.namespace, lockKey) as { value: Buffer } | undefined;
      if (row === undefined || row.value.toString("utf8") !== token) return false;
      const result = this.db
        .prepare("DELETE FROM cache WHERE namespace = ? AND key = ?")
        .run(this.namespace, lockKey);
      return result.changes > 0;
    });
    return txn.immediate();
  }

  // -- internal: LRU buffering, sweeping, eviction --------------------------

  private bufferLru(key: string, now: number): void {
    const entry = this.lruBuffer.get(key);
    if (entry === undefined) {
      this.lruBuffer.set(key, { lastAccessed: now, accessCount: 1 });
    } else {
      entry.lastAccessed = now;
      entry.accessCount += 1;
    }
    if (this.lruBuffer.size >= LRU_FLUSH_THRESHOLD) {
      this.flushLruBuffer();
    }
  }

  private flushLruBuffer(): void {
    if (this.lruBuffer.size === 0) return;
    const pending = [...this.lruBuffer.entries()];
    this.lruBuffer.clear();
    try {
      const stmt = this.db.prepare(
        "UPDATE cache SET last_accessed = ?, access_count = access_count + ? WHERE namespace = ? AND key = ?",
      );
      const txn = this.db.transaction((entries: [string, { lastAccessed: number; accessCount: number }][]) => {
        for (const [key, meta] of entries) {
          stmt.run(meta.lastAccessed, meta.accessCount, this.namespace, key);
        }
      });
      txn.immediate(pending);
    } catch (err) {
      if (this.strict) throw err;
      this.logger(`failed to flush LRU bookkeeping buffer: ${String(err)}`);
    }
  }

  private maybeOpportunisticMaintenance(): void {
    if (this.sweepIntervalSeconds !== null) return;
    this.opCount += 1;
    if (this.opCount >= OPPORTUNISTIC_EVERY) {
      this.opCount = 0;
      this.sweepOnce();
    }
  }

  private sweepOnce(): void {
    try {
      this.flushLruBuffer();
      const now = nowMs();
      // Bounded batch delete via subquery: plain SQLite doesn't support DELETE ... LIMIT, so the
      // batch is bounded by selecting the matching keys first.
      const result = this.db
        .prepare(
          `DELETE FROM cache WHERE namespace = ? AND key IN (
             SELECT key FROM cache WHERE namespace = ? AND expires_at IS NOT NULL AND expires_at <= ?
             LIMIT ?
           )`,
        )
        .run(this.namespace, this.namespace, now, BATCH_DELETE_LIMIT);
      this.expiredRemovedCount += result.changes;
      this.maybeEvict();
      this.sweepEvents(now);
    } catch (err) {
      if (this.strict) throw err;
      this.logger(`sweep failed: ${String(err)}`);
    }
  }

  private deleteExpiredRow(key: string, now: number): void {
    try {
      this.db
        .prepare(
          "DELETE FROM cache WHERE namespace = ? AND key = ? AND expires_at IS NOT NULL AND expires_at <= ?",
        )
        .run(this.namespace, key, now);
    } catch (err) {
      this.logger(`failed to delete expired key ${JSON.stringify(key)}: ${String(err)}`);
    }
  }

  /**
   * Rejects a write outright if it would grow the dataset past `maxKeys`/`maxBytes` under
   * `"noeviction"` -- there's nothing to evict, so the write itself must fail rather than being
   * accepted and evicted after the fact. Updating an existing, non-expired key never grows the
   * dataset, so it's always allowed.
   */
  private checkCapacityBeforeWrite(key: string): void {
    const now = nowMs();
    const exists = this.db
      .prepare(
        "SELECT 1 FROM cache WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)",
      )
      .get(this.namespace, key, now);
    if (exists !== undefined) return;
    const row = this.db
      .prepare("SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?")
      .get(this.namespace) as { cnt: number; sz: number };
    if (this.maxKeys !== null && row.cnt >= this.maxKeys) {
      throw new CacheFullError(`cache is full (keys=${row.cnt}, max=${this.maxKeys})`);
    }
    if (this.maxBytes !== null && row.sz >= this.maxBytes) {
      throw new CacheFullError(`cache is full (bytes=${row.sz}, max=${this.maxBytes})`);
    }
  }

  private maybeEvict(): void {
    if (this.eviction === "noeviction") return;
    if (this.maxKeys === null && this.maxBytes === null) return;
    // Flush buffered last_accessed updates first so LRU eviction order reflects the access that
    // just happened, not a stale on-disk timestamp from before the read was buffered.
    if (this.eviction === "lru") {
      this.flushLruBuffer();
    }
    const orderBy =
      this.eviction === "lru"
        ? "last_accessed ASC"
        : this.eviction === "ttl"
          ? "(expires_at IS NULL) ASC, expires_at ASC"
          : "RANDOM()";
    for (;;) {
      const row = this.db
        .prepare("SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?")
        .get(this.namespace) as { cnt: number; sz: number };
      const overKeys = this.maxKeys !== null ? row.cnt - this.maxKeys : 0;
      const overBytes = this.maxBytes !== null && row.sz > this.maxBytes;
      if (overKeys <= 0 && !overBytes) return;
      const batch = Math.max(overKeys, 1);
      const result = this.db
        .prepare(
          `DELETE FROM cache WHERE namespace = ? AND key IN (
             SELECT key FROM cache WHERE namespace = ? ORDER BY ${orderBy} LIMIT ?
           )`,
        )
        .run(this.namespace, this.namespace, batch);
      if (result.changes === 0) return;
      this.evictionsCount += result.changes;
    }
  }
}

function decodeIntText(text: string): number | bigint {
  const big = BigInt(text);
  const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
  const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);
  if (big > MAX_SAFE || big < MIN_SAFE) return big;
  return Number(big);
}
