/**
 * Base class for every error this library throws. Extending the built-in
 * {@link Error} (rather than a plain object) preserves stack traces and
 * `instanceof` checks across the compiled ESM/CJS boundary.
 */
export class LyteCacheError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "LyteCacheError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown by {@link Eviction} `"noeviction"` when a write would grow the dataset past its limit. */
export class CacheFullError extends LyteCacheError {
  constructor(message: string) {
    super(message);
    this.name = "CacheFullError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when a value can't be serialized (functions, symbols, circular
 * references, `Map`/`Set`) or can't be deserialized (a stored type code this
 * language doesn't support, e.g. Python pickle or Java's native format).
 */
export class SerializationError extends LyteCacheError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SerializationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when opening a database file whose `schema_version` is newer than this library supports. */
export class SchemaVersionError extends LyteCacheError {
  constructor(message: string) {
    super(message);
    this.name = "SchemaVersionError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown by {@link LyteCache.lock} when a lock can't be acquired within its timeout. */
export class LockTimeoutError extends LyteCacheError {
  constructor(message: string) {
    super(message);
    this.name = "LockTimeoutError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown (or emitted as `"error"`) by a {@link Subscription} when events it should have seen are
 * gone: retention deleted them before it read them, or its cursor is ahead of the file (the file
 * was recreated). Never silently skipped. To accept the loss, watch again with
 * `{ fromSeq: err.resumeSeq }`.
 */
export class GapError extends LyteCacheError {
  readonly requestedSeq: number;
  readonly oldestSeq: number;
  readonly resumeSeq: number;
  readonly ahead: boolean;

  constructor(requestedSeq: number, oldestSeq: number, ahead: boolean) {
    super(
      ahead
        ? `cursor ${requestedSeq} is ahead of the last event ever published (${oldestSeq - 1}); ` +
            "the database file was probably recreated"
        : `events after seq ${requestedSeq} are no longer retained (oldest retained is ${oldestSeq}); ` +
            `events ${requestedSeq + 1}..${oldestSeq - 1} were lost`,
    );
    this.name = "GapError";
    this.requestedSeq = requestedSeq;
    this.oldestSeq = oldestSeq;
    this.resumeSeq = oldestSeq - 1;
    this.ahead = ahead;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
