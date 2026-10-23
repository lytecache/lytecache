<?php

declare(strict_types=1);

namespace Lytecache;

use Lytecache\Exceptions\CacheFullException;
use Lytecache\Exceptions\GapException;
use Lytecache\Exceptions\LockTimeoutException;
use Lytecache\Exceptions\NotNumericException;
use Lytecache\Exceptions\SchemaVersionException;
use Lytecache\Exceptions\SerializationException;
use Lytecache\Support\JsonObject;
use Lytecache\Support\Paths;
use Lytecache\Support\Schema;
use Lytecache\Support\Serializer;
use Lytecache\Support\Statement;

/**
 * An embedded, Redis-like cache backed by a SQLite file.
 *
 * The zero-configuration form is the flagship way to use this class:
 *
 *     $cache = new LyteCache();
 *     $cache->set('user:42', ['name' => 'Ada'], ttl: 300);
 *     $cache->get('user:42'); // ['name' => 'Ada']
 *
 * With no arguments, the database file (and any missing parent
 * directories) is created on first use, at a default, per-project
 * location -- see {@see self::defaultPath()}.
 *
 * A LyteCache instance is safe to share across many operations within one
 * PHP process/request, and safe for many PHP-FPM worker processes to use
 * concurrently against the same file: every read-modify-write operation is
 * a single SQL statement or an explicit transaction.
 */
final class LyteCache
{
    private const UPSERT_SQL = <<<'SQL'
        INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
        VALUES (:key, :namespace, :value, :type, :now, :expires, :now2, 0, :size)
        ON CONFLICT(namespace, key) DO UPDATE SET
          value = excluded.value,
          value_type = excluded.value_type,
          created_at = excluded.created_at,
          expires_at = excluded.expires_at,
          last_accessed = excluded.last_accessed,
          access_count = 0,
          size_bytes = excluded.size_bytes
        SQL;

    /**
     * Namespaces distributed-lock keys away from ordinary user keys.
     * Public so the Laravel Lock adapter (see src/Laravel/LytecacheLock.php)
     * can build the same underlying key without duplicating this literal.
     */
    public const LOCK_KEY_PREFIX = '__lock__:';

    private const LOCK_POLL_SECONDS = 0.05;

    /** How many operations pass between opportunistic maintenance attempts. */
    private const MAINTENANCE_EVERY_OPS = 100;

    /** Bounds each expired-row delete pass and each LRU-flush transaction. */
    private const MAINTENANCE_BATCH = 500;

    /** Defensive cap on the one-row-at-a-time eviction loop. */
    private const MAX_EVICTION_PASSES = 100_000;

    private readonly string $path;

    private \PDO $pdo;

    /** @var array<string, \PDOStatement> */
    private array $stmtCache = [];

    /** @var array<string, array{lastAccessed: int, accessCount: int}> */
    private array $lruBuffer = [];

    private int $hits = 0;

    private int $misses = 0;

    private int $evictions = 0;

    private int $expiredRemoved = 0;

    private int $opsSinceMaintenance = 0;

    private float $lastMaintenanceAt;

    private bool $closed = false;

    /** JSON1 support for hash operations, detected once at open. */
    private readonly bool $json1;

    /**
     * Whether this SQLite also resolves path labels with an escaped double quote
     * ($."a\"b"). SQLite 3.45-3.47 handle every other field name natively but not
     * that one, so hash calls naming such a field take the fallback there.
     */
    private readonly bool $json1QuotedLabels;

    // Events and change listeners are opt-in: none of this costs anything
    // (no table, no work) until notify/watch/onChange is first used.
    private bool $eventsReady = false;

    /** @var list<Subscription> */
    private array $subscriptions = [];

    /** @var array<int, array{0: \Closure, 1: list<string>}> */
    private array $listeners = [];

    private int $nextListenerId = 0;

    private int $dispatchDepth = 0;

    /** @var \Closure(string): void */
    private readonly \Closure $logger;

    /**
     * @param  string|null  $path  Explicit database file path; default: {@see self::defaultPath()}.
     * @param  string  $namespace  Logical partition within the database file.
     * @param  int|null  $maxKeys  Evict (per $eviction) once the namespace exceeds this many keys.
     * @param  int|null  $maxBytes  Evict (per $eviction) once the namespace exceeds this many bytes.
     * @param  Eviction  $eviction  Eviction policy.
     * @param  float|null  $sweepInterval  Minimum seconds between opportunistic maintenance passes;
     *                                     null removes that minimum, so maintenance runs as often as the internal operation
     *                                     counter allows. PHP has no background threads, so there is no sweeper to disable outright
     *                                     -- see maintain() and the README's "Why opportunistic maintenance?" section.
     * @param  bool  $strict  When false (default), a read that hits an internal deserialization
     *                        error degrades to a miss rather than throwing. When true, it throws
     *                        {@see SerializationException}. Writes always throw, in both modes.
     * @param  list<string>  $publishChanges  Mutations ("set", "delete", "flush") that also publish a
     *                                        {@see ChangeRecord} to the reserved "__cache__" channel, for
     *                                        watchers in other processes on this host. Off by default; each costs one event row
     *                                        written under the write lock (per call, not per key).
     * @param  float  $eventRetention  Seconds events are kept before maintenance deletes them, read or not.
     * @param  int|null  $eventMaxCount  Additionally keep at most this many newest events.
     * @param  int  $changeKeyCap  Keys a {@see ChangeRecord} lists before it is truncated.
     * @param  (\Closure(string): void)|null  $logger  Receives warnings (default: error_log()).
     */
    public function __construct(
        ?string $path = null,
        private readonly string $namespace = 'default',
        private readonly ?int $maxKeys = null,
        private readonly ?int $maxBytes = null,
        private readonly Eviction $eviction = Eviction::LRU,
        private readonly ?float $sweepInterval = 60.0,
        private readonly bool $strict = false,
        private readonly array $publishChanges = [],
        private readonly float $eventRetention = 3600.0,
        private readonly ?int $eventMaxCount = null,
        private readonly int $changeKeyCap = 100,
        ?\Closure $logger = null,
    ) {
        foreach ($publishChanges as $op) {
            if (! in_array($op, self::CHANGE_OPS, true)) {
                throw new \InvalidArgumentException("lytecache: publishChanges must be a subset of set, delete, flush; got \"{$op}\"");
            }
        }
        if ($eventRetention <= 0) {
            throw new \InvalidArgumentException('lytecache: eventRetention must be positive');
        }
        $this->logger = $logger ?? static function (string $message): void {
            error_log($message);
        };
        $this->path = $path === null ? self::defaultPath() : Paths::expandHome($path);

        $dir = dirname($this->path);
        if ($dir !== '' && $dir !== '.' && ! is_dir($dir) && ! mkdir($dir, 0755, true) && ! is_dir($dir)) {
            throw new \RuntimeException("lytecache: could not create database directory {$dir}");
        }

        $this->pdo = $this->openWithRetry($this->path);
        $this->lastMaintenanceAt = microtime(true);
        [$this->json1, $this->json1QuotedLabels] = $this->detectJson1();
    }

    /**
     * Returns the default database file location for the current working
     * directory: "<platform cache dir>/lytecache/<project-id>.db".
     *
     * <project-id> is the first 12 hex characters of the SHA-256 hash of
     * the resolved, absolute current working directory -- the same
     * derivation used by the Python, Java, Node.js, and Go
     * implementations of lytecache, so a process in any of those
     * languages started from the same directory resolves to the same
     * file.
     *
     * If the LYTECACHE_PATH environment variable is set, it is returned
     * instead (after "~" expansion), taking priority over the derived
     * default.
     */
    public static function defaultPath(): string
    {
        return Paths::defaultPath();
    }

    /** This instance's actual database file path. */
    public function path(): string
    {
        return $this->path;
    }

    // ---------------------------------------------------------------
    // Construction / connection management
    // ---------------------------------------------------------------

    private function openWithRetry(string $path): \PDO
    {
        $maxAttempts = 25;
        $lastException = null;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $pdo = new \PDO('sqlite:'.$path, options: [
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                    // Resets each statement before it runs, so a cached statement whose
                    // last execution failed can't silently drop the next write. See
                    // Support\Statement.
                    \PDO::ATTR_STATEMENT_CLASS => [Statement::class],
                ]);

                foreach (Schema::PRAGMAS as $pragma) {
                    $pdo->exec($pragma);
                }

                $pdo->exec(Schema::DDL);
                $this->checkSchemaVersion($pdo);

                return $pdo;
            } catch (\PDOException $e) {
                $lastException = $e;

                if (! self::isSqliteBusy($e)) {
                    throw $e;
                }

                usleep(20_000);
            }
        }

        throw new \RuntimeException(
            "lytecache: initializing schema after {$maxAttempts} attempts: ".$lastException->getMessage(),
            previous: $lastException
        );
    }

    /**
     * Multiple processes creating the same brand-new WAL-mode file at the
     * exact same moment can hit SQLITE_BUSY on the initial journal_mode
     * switch, before busy_timeout has had a chance to matter (a
     * well-known SQLite cold-start race, not specific to this driver) --
     * openWithRetry() retries specifically on this error before giving up.
     */
    private static function isSqliteBusy(\PDOException $e): bool
    {
        $info = $e->errorInfo ?? null;
        if (is_array($info) && ($info[1] ?? null) === 5) {
            return true;
        }

        return str_contains($e->getMessage(), 'database is locked');
    }

    private function checkSchemaVersion(\PDO $pdo): void
    {
        $result = $pdo->query("SELECT v FROM meta WHERE k = 'schema_version'");
        $row = false;
        if ($result !== false) {
            $row = $result->fetch(\PDO::FETCH_ASSOC);
            $result->closeCursor();
        }

        if ($row === false) {
            $pdo->exec('INSERT OR IGNORE INTO meta (k, v) VALUES (\'schema_version\', \''.Schema::VERSION.'\')');

            return;
        }

        $version = (int) $row['v'];
        if ($version > Schema::VERSION) {
            throw new SchemaVersionException(
                "lytecache: file has schema_version={$version}, this version of lytecache supports up to ".Schema::VERSION
            );
        }
    }

    /**
     * Cached prepared statements. Reuse is safe even after a failed execution,
     * because the connection's statement class (Support\Statement) resets every
     * statement before running it.
     */
    private function prepare(string $sql): \PDOStatement
    {
        return $this->stmtCache[$sql] ??= $this->pdo->prepare($sql);
    }

    /**
     * Executes $sql with $params, binding $blobValue onto $blobParam as
     * PDO::PARAM_LOB so it is always stored/read back as raw, binary-safe
     * bytes -- never re-interpreted or coerced by SQLite's dynamic typing.
     *
     * Wrapped in an explicit BEGIN IMMEDIATE/COMMIT rather than left as a
     * bare autocommit statement: an UPSERT is, under the hood, a
     * conflict-check *read* that may need to upgrade to a *write*
     * mid-statement, and that upgrade is exactly the SQLite scenario
     * where busy_timeout-based retries alone can spin far longer than
     * expected under real concurrent-writer load (observed empirically
     * against the multi-process test). BEGIN IMMEDIATE acquires the write
     * lock upfront instead, which avoids the upgrade entirely.
     *
     * @param  array<string, int|string|null>  $params
     */
    private function executeWithBlob(string $sql, array $params, string $blobParam, string $blobValue): \PDOStatement
    {
        $stmt = $this->prepare($sql);
        $stmt->bindValue($blobParam, $blobValue, \PDO::PARAM_LOB);

        foreach ($params as $key => $value) {
            $stmt->bindValue($key, $value);
        }

        $this->inWriteTransaction(function () use ($stmt) {
            $stmt->execute();
        });

        return $stmt;
    }

    /**
     * Runs $body inside a single BEGIN IMMEDIATE/COMMIT transaction, with
     * jittered retry on SQLITE_BUSY around the whole attempt. $body may
     * issue several statements (e.g. a read followed by a write, as in
     * {@see getSet()}) -- callers must never nest a second call to this
     * method (or another BEGIN) inside $body, since SQLite does not support
     * nested transactions.
     */
    private function inWriteTransaction(callable $body): void
    {
        $this->executeWithRetry(function () use ($body) {
            $this->pdo->exec('BEGIN IMMEDIATE');

            try {
                $body();
                $this->pdo->exec('COMMIT');
            } catch (\Throwable $e) {
                // Roll back on *any* failure, including a failed COMMIT --
                // otherwise a busy COMMIT leaves the transaction dangling
                // (neither committed nor rolled back), and the retry above
                // would attempt a nested BEGIN IMMEDIATE on top of it,
                // corrupting the retry (this was a real bug caught by the
                // multi-process test intermittently under-counting).
                $this->pdo->exec('ROLLBACK');

                throw $e;
            }
        });
    }

    /**
     * Retries $fn on SQLITE_BUSY. PRAGMA busy_timeout (5000ms) already does
     * the bulk of the waiting for us at the C level, blocking inside each
     * BEGIN IMMEDIATE/COMMIT until the competing writer releases the lock
     * or the timeout elapses -- so a busy error surfacing here means a
     * writer held the lock for the *entire* 5s window, which only happens
     * under truly pathological contention. This loop is a thin safety net
     * on top of that: a handful of extra attempts with a short jittered
     * sleep, mainly to desynchronize many PHP-FPM workers that all got
     * unblocked at the same instant and would otherwise immediately
     * collide again on the next BEGIN IMMEDIATE.
     */
    private function executeWithRetry(callable $fn): mixed
    {
        $maxAttempts = 20;
        $maxDelayMicros = 50_000;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                return $fn();
            } catch (\PDOException $e) {
                if ($attempt === $maxAttempts || ! self::isSqliteBusy($e)) {
                    throw $e;
                }

                usleep(random_int(1_000, $maxDelayMicros));
            }
        }

        throw new \RuntimeException('lytecache: unreachable'); // @codeCoverageIgnore
    }

    private static function nowMillis(): int
    {
        return (int) round(microtime(true) * 1000);
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new \RuntimeException('lytecache: this LyteCache instance has been closed');
        }
    }

    // ---------------------------------------------------------------
    // Key-value
    // ---------------------------------------------------------------

    /** Stores $value under $key, replacing any existing value. $ttl is in seconds. */
    public function set(string $key, mixed $value, ?float $ttl = null): void
    {
        $this->assertOpen();
        [$data, $type] = Serializer::encode($value);

        if ($this->eviction === Eviction::NoEviction && ($this->maxKeys !== null || $this->maxBytes !== null)) {
            $this->checkCapacity($key);
        }

        $now = self::nowMillis();
        $expires = $ttl === null ? null : $now + (int) round($ttl * 1000);

        $this->executeWithBlob(self::UPSERT_SQL, [
            ':key' => $key,
            ':namespace' => $this->namespace,
            ':type' => $type,
            ':now' => $now,
            ':now2' => $now,
            ':expires' => $expires,
            ':size' => strlen($data),
        ], ':value', $data);

        $this->changed('set', [$key], 1);
        $this->maybeEvict();
        $this->maybeMaintain();
    }

    /**
     * Reads $key. Returns $default (never throws) on a miss or expiry.
     *
     * $class requests typed rehydration of a JSON-coded value: the
     * decoded array is mapped onto $class via its constructor (matching
     * parameter names to array keys) or, failing that, by assigning
     * public properties directly. See SPEC.md for the full rules.
     *
     * @param  class-string|null  $class
     */
    public function get(string $key, mixed $default = null, ?string $class = null): mixed
    {
        $this->assertOpen();
        $row = $this->selectRaw($key);
        $this->maybeMaintain();

        if ($row === null) {
            $this->misses++;

            return $default;
        }

        $now = self::nowMillis();
        if ($row['expires_at'] !== null && (int) $row['expires_at'] <= $now) {
            $this->misses++;
            $this->deleteRaw($key);

            return $default;
        }

        $this->hits++;
        $this->bufferLru($key, $now);

        try {
            return Serializer::decode($row['value'], (int) $row['value_type'], $class);
        } catch (SerializationException $e) {
            if ($this->strict) {
                throw $e;
            }

            return $default;
        }
    }

    /**
     * @return array{value: string, value_type: int, expires_at: int|null}|null
     */
    private function selectRaw(string $key): ?array
    {
        $stmt = $this->prepare('SELECT value, value_type, expires_at FROM cache WHERE namespace = :ns AND key = :key');
        $stmt->execute([':ns' => $this->namespace, ':key' => $key]);
        /** @var array{value: string, value_type: int, expires_at: int|null}|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        return $row === false ? null : $row;
    }

    private function deleteRaw(string $key): void
    {
        $stmt = $this->prepare('DELETE FROM cache WHERE namespace = :ns AND key = :key');
        $this->executeWithRetry(fn () => $stmt->execute([':ns' => $this->namespace, ':key' => $key]));
    }

    /** Deletes the given keys, returning how many actually existed. */
    public function delete(string ...$keys): int
    {
        $this->assertOpen();
        if ($keys === []) {
            return 0;
        }

        $placeholders = [];
        $params = [':ns' => $this->namespace];
        foreach (array_values($keys) as $i => $key) {
            $placeholder = ":k{$i}";
            $placeholders[] = $placeholder;
            $params[$placeholder] = $key;
        }

        $sql = 'DELETE FROM cache WHERE namespace = :ns AND key IN ('.implode(',', $placeholders).')';
        $stmt = $this->prepare($sql);
        $this->executeWithRetry(fn () => $stmt->execute($params));
        $removed = $stmt->rowCount();
        if ($removed > 0) {
            $this->changed('delete', array_values(array_unique($keys)), $removed);
        }

        return $removed;
    }

    /** Whether $key is present and not expired. */
    public function has(string $key): bool
    {
        $this->assertOpen();
        $stmt = $this->prepare('SELECT expires_at FROM cache WHERE namespace = :ns AND key = :key');
        $stmt->execute([':ns' => $this->namespace, ':key' => $key]);
        /** @var array{expires_at: int|null}|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        if ($row === false) {
            return false;
        }

        if ($row['expires_at'] !== null && (int) $row['expires_at'] <= self::nowMillis()) {
            $this->deleteRaw($key);

            return false;
        }

        return true;
    }

    /** Sets $key only if it is currently absent or expired (Redis "SET NX"), atomically. */
    public function add(string $key, mixed $value, ?float $ttl = null): bool
    {
        return $this->addInternal($key, $value, $ttl, true);
    }

    private function addInternal(string $key, mixed $value, ?float $ttl, bool $emit): bool
    {
        $this->assertOpen();
        [$data, $type] = Serializer::encode($value);

        if ($this->eviction === Eviction::NoEviction && ($this->maxKeys !== null || $this->maxBytes !== null)) {
            $this->checkCapacity($key);
        }

        $now = self::nowMillis();
        $expires = $ttl === null ? null : $now + (int) round($ttl * 1000);

        $sql = self::UPSERT_SQL."\nWHERE cache.expires_at IS NOT NULL AND cache.expires_at <= :now3";

        $stmt = $this->executeWithBlob($sql, [
            ':key' => $key,
            ':namespace' => $this->namespace,
            ':type' => $type,
            ':now' => $now,
            ':now2' => $now,
            ':now3' => $now,
            ':expires' => $expires,
            ':size' => strlen($data),
        ], ':value', $data);

        $added = $stmt->rowCount() > 0;
        if ($added) {
            if ($emit) {
                $this->changed('set', [$key], 1);
            }
            $this->maybeEvict();
        }
        $this->maybeMaintain();

        return $added;
    }

    /** Sets $key only if it is currently present and not expired (Redis "SET XX"), atomically. */
    public function replace(string $key, mixed $value, ?float $ttl = null): bool
    {
        $this->assertOpen();
        [$data, $type] = Serializer::encode($value);

        $now = self::nowMillis();
        $expires = $ttl === null ? null : $now + (int) round($ttl * 1000);

        $sql = <<<'SQL'
            UPDATE cache SET value = :value, value_type = :type, expires_at = :expires, last_accessed = :now, access_count = 0, size_bytes = :size
            WHERE namespace = :ns AND key = :key AND (expires_at IS NULL OR expires_at > :now2)
            SQL;

        $stmt = $this->executeWithBlob($sql, [
            ':type' => $type,
            ':expires' => $expires,
            ':now' => $now,
            ':now2' => $now,
            ':size' => strlen($data),
            ':ns' => $this->namespace,
            ':key' => $key,
        ], ':value', $data);

        $replaced = $stmt->rowCount() > 0;
        if ($replaced) {
            $this->changed('set', [$key], 1);
        }
        $this->maybeMaintain();

        return $replaced;
    }

    /**
     * Atomically swaps in $value and returns the previous value (or null
     * if there was none). Like Redis's GETSET, it clears any TTL that was
     * set on the previous value.
     */
    public function getSet(string $key, mixed $value): mixed
    {
        $this->assertOpen();
        [$data, $type] = Serializer::encode($value);

        $sql = <<<'SQL'
            INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
            VALUES (:key, :namespace, :value, :type, :now, NULL, :now2, 0, :size)
            ON CONFLICT(namespace, key) DO UPDATE SET
              value = excluded.value, value_type = excluded.value_type, created_at = excluded.created_at,
              expires_at = NULL, last_accessed = excluded.last_accessed, access_count = 0, size_bytes = excluded.size_bytes
            SQL;
        $stmt = $this->prepare($sql);

        $old = null;
        $hadPrevious = false;

        // A single BEGIN IMMEDIATE/COMMIT covers both the read and the
        // write, so the read-modify-write is atomic across processes --
        // executeWithBlob() cannot be reused here since it opens its own
        // transaction, and SQLite does not support nesting one.
        $this->inWriteTransaction(function () use ($stmt, $key, $type, $data, &$old, &$hadPrevious) {
            $old = $this->selectRaw($key);
            $now = self::nowMillis();

            $hadPrevious = $old !== null && ($old['expires_at'] === null || (int) $old['expires_at'] > $now);

            $stmt->bindValue(':value', $data, \PDO::PARAM_LOB);
            $stmt->bindValue(':key', $key);
            $stmt->bindValue(':namespace', $this->namespace);
            $stmt->bindValue(':type', $type);
            $stmt->bindValue(':now', $now);
            $stmt->bindValue(':now2', $now);
            $stmt->bindValue(':size', strlen($data));
            $stmt->execute();
        });

        $this->changed('set', [$key], 1);
        $this->maybeMaintain();

        if (! $hadPrevious || $old === null) {
            return null;
        }

        try {
            return Serializer::decode($old['value'], (int) $old['value_type']);
        } catch (SerializationException $e) {
            if ($this->strict) {
                throw $e;
            }

            return null;
        }
    }

    /**
     * Writes every entry in a single transaction, with a shared $ttl.
     *
     * @param  array<string, mixed>  $entries
     */
    public function setMany(array $entries, ?float $ttl = null): void
    {
        $this->assertOpen();
        if ($entries === []) {
            return;
        }

        $now = self::nowMillis();
        $expires = $ttl === null ? null : $now + (int) round($ttl * 1000);

        $stmt = $this->prepare(self::UPSERT_SQL);

        $this->inWriteTransaction(function () use ($stmt, $entries, $now, $expires) {
            foreach ($entries as $key => $value) {
                [$data, $type] = Serializer::encode($value);
                $stmt->bindValue(':value', $data, \PDO::PARAM_LOB);
                $stmt->bindValue(':key', (string) $key);
                $stmt->bindValue(':namespace', $this->namespace);
                $stmt->bindValue(':type', $type);
                $stmt->bindValue(':now', $now);
                $stmt->bindValue(':now2', $now);
                $stmt->bindValue(':expires', $expires);
                $stmt->bindValue(':size', strlen($data));
                $stmt->execute();
            }
        });

        // ONE change record for the whole batch, never one per key.
        $this->changed('set', array_map('strval', array_keys($entries)), count($entries));
        $this->maybeEvict();
        $this->maybeMaintain();
    }

    /**
     * Reads all of the given keys in a single query, skipping missing or
     * expired keys rather than erroring.
     *
     * @param  string[]  $keys
     * @return array<string, mixed>
     */
    public function getMany(array $keys): array
    {
        $this->assertOpen();
        $result = [];
        if ($keys === []) {
            return $result;
        }

        $placeholders = [];
        $params = [':ns' => $this->namespace];
        foreach (array_values($keys) as $i => $key) {
            $placeholder = ":k{$i}";
            $placeholders[] = $placeholder;
            $params[$placeholder] = $key;
        }

        $sql = 'SELECT key, value, value_type, expires_at FROM cache WHERE namespace = :ns AND key IN ('.implode(',', $placeholders).')';
        $stmt = $this->prepare($sql);
        $stmt->execute($params);

        $now = self::nowMillis();
        $expiredKeys = [];

        /** @var array{key: string, value: string, value_type: int, expires_at: int|null} $row */
        foreach ($stmt->fetchAll(\PDO::FETCH_ASSOC) as $row) {
            if ($row['expires_at'] !== null && (int) $row['expires_at'] <= $now) {
                $expiredKeys[] = $row['key'];

                continue;
            }

            try {
                $result[$row['key']] = Serializer::decode($row['value'], (int) $row['value_type']);
            } catch (SerializationException $e) {
                if ($this->strict) {
                    throw $e;
                }

                // Non-strict mode: skip this key, as if it were a miss.
                continue;
            }

            $this->bufferLru($row['key'], $now);
        }

        $this->maybeMaintain();

        if ($expiredKeys !== []) {
            $this->delete(...$expiredKeys);
        }

        $this->hits += count($result);
        $this->misses += count($keys) - count($result) - count($expiredKeys);
        $this->misses += count($expiredKeys);

        return $result;
    }

    /**
     * Enforces maxKeys/maxBytes for the NoEviction policy, ahead of a
     * single-key write, so a rejected write for a *new* key never has a
     * side effect. Updating an existing key is always allowed, since it
     * never grows the dataset.
     */
    private function checkCapacity(string $key): void
    {
        $stmt = $this->prepare('SELECT 1 FROM cache WHERE namespace = :ns AND key = :key');
        $stmt->execute([':ns' => $this->namespace, ':key' => $key]);
        $exists = $stmt->fetch() !== false;
        $stmt->closeCursor();
        if ($exists) {
            return;
        }

        if ($this->maxKeys !== null) {
            $stmt = $this->prepare('SELECT COUNT(*) FROM cache WHERE namespace = :ns');
            $stmt->execute([':ns' => $this->namespace]);
            $count = (int) $stmt->fetchColumn();
            $stmt->closeCursor();

            if ($count >= $this->maxKeys) {
                throw new CacheFullException(
                    "lytecache: namespace \"{$this->namespace}\" has reached its {$this->maxKeys}-key limit"
                );
            }
        }

        if ($this->maxBytes !== null) {
            $stmt = $this->prepare('SELECT COALESCE(SUM(size_bytes), 0) FROM cache WHERE namespace = :ns');
            $stmt->execute([':ns' => $this->namespace]);
            $total = (int) $stmt->fetchColumn();
            $stmt->closeCursor();

            if ($total >= $this->maxBytes) {
                throw new CacheFullException(
                    "lytecache: namespace \"{$this->namespace}\" has reached its {$this->maxBytes}-byte limit"
                );
            }
        }
    }

    // ---------------------------------------------------------------
    // Expiration
    // ---------------------------------------------------------------

    /** Sets or overwrites the TTL (in seconds) on an existing key. Returns whether the key existed. */
    public function expire(string $key, float $ttl): bool
    {
        $this->assertOpen();
        $now = self::nowMillis();
        $expires = $now + (int) round($ttl * 1000);

        $stmt = $this->prepare(<<<'SQL'
            UPDATE cache SET expires_at = :expires
            WHERE namespace = :ns AND key = :key AND (expires_at IS NULL OR expires_at > :now)
            SQL);
        $this->executeWithRetry(fn () => $stmt->execute([':expires' => $expires, ':ns' => $this->namespace, ':key' => $key, ':now' => $now]));

        return $stmt->rowCount() > 0;
    }

    /** Removes any TTL from an existing key. Returns whether the key existed. */
    public function persist(string $key): bool
    {
        $this->assertOpen();
        $now = self::nowMillis();

        $stmt = $this->prepare(<<<'SQL'
            UPDATE cache SET expires_at = NULL
            WHERE namespace = :ns AND key = :key AND (expires_at IS NULL OR expires_at > :now)
            SQL);
        $this->executeWithRetry(fn () => $stmt->execute([':ns' => $this->namespace, ':key' => $key, ':now' => $now]));

        return $stmt->rowCount() > 0;
    }

    /** Refreshes an existing key's TTL (sliding expiration). Equivalent to expire(). */
    public function touch(string $key, float $ttl): bool
    {
        return $this->expire($key, $ttl);
    }

    /**
     * Remaining time-to-live for $key, in seconds: -1 if the key exists
     * with no TTL, null if the key does not exist (or is already
     * expired).
     */
    public function ttl(string $key): float|int|null
    {
        $this->assertOpen();
        $stmt = $this->prepare('SELECT expires_at FROM cache WHERE namespace = :ns AND key = :key');
        $stmt->execute([':ns' => $this->namespace, ':key' => $key]);
        /** @var array{expires_at: int|null}|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        if ($row === false) {
            return null;
        }

        if ($row['expires_at'] === null) {
            return -1;
        }

        $now = self::nowMillis();
        $expiresAt = (int) $row['expires_at'];

        if ($expiresAt <= $now) {
            $this->deleteRaw($key);

            return null;
        }

        return ($expiresAt - $now) / 1000.0;
    }

    // ---------------------------------------------------------------
    // Atomic counters
    // ---------------------------------------------------------------

    /**
     * A single UPSERT: correct under concurrent access from many PHP-FPM
     * worker processes sharing the file, never a read-modify-write race.
     * It relies on the value being stored as UTF-8 decimal text (see
     * Serializer::encode): CAST(value AS TEXT) reads the digits, SQLite
     * coerces them to a number for the addition, and the outer
     * CAST(... AS TEXT) converts the result back to decimal digits before
     * storing it as a BLOB again. An expired existing row is treated as
     * absent (starts from zero) rather than as an error.
     */
    private function atomicIncr(string $key, string $amountText, int $resultType, string $allowedTypesSql): string
    {
        $now = self::nowMillis();
        $initial = $amountText;

        $sql = <<<SQL
            INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
            VALUES (:key, :ns, :initial, :rtype, :now, NULL, :now2, 0, :initlen)
            ON CONFLICT(namespace, key) DO UPDATE SET
              value = CAST(CAST(
                (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now3 THEN 0 ELSE CAST(cache.value AS TEXT) END)
                + :amount AS TEXT) AS BLOB),
              value_type = :rtype2,
              expires_at = CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now4 THEN NULL ELSE cache.expires_at END,
              last_accessed = :now5,
              access_count = cache.access_count + 1,
              size_bytes = length(CAST(CAST(
                (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= :now6 THEN 0 ELSE CAST(cache.value AS TEXT) END)
                + :amount2 AS TEXT) AS BLOB))
            WHERE (cache.expires_at IS NOT NULL AND cache.expires_at <= :now7) OR cache.value_type IN ({$allowedTypesSql})
            SQL;

        $stmt = $this->executeWithBlob($sql, [
            ':key' => $key,
            ':ns' => $this->namespace,
            ':rtype' => $resultType,
            ':rtype2' => $resultType,
            ':now' => $now,
            ':now2' => $now,
            ':now3' => $now,
            ':now4' => $now,
            ':now5' => $now,
            ':now6' => $now,
            ':now7' => $now,
            ':amount' => $amountText,
            ':amount2' => $amountText,
            ':initlen' => strlen($initial),
        ], ':initial', $initial);

        if ($stmt->rowCount() === 0) {
            throw new NotNumericException("lytecache: value for key \"{$key}\" is not numeric");
        }

        $select = $this->prepare('SELECT value FROM cache WHERE namespace = :ns AND key = :key');
        $select->execute([':ns' => $this->namespace, ':key' => $key]);
        $value = $select->fetchColumn();
        $select->closeCursor();

        $this->changed('set', [$key], 1);
        $this->maybeMaintain();

        return (string) $value;
    }

    /**
     * Atomically adds $amount (may be negative) to the integer stored at
     * $key, creating it (starting from 0) if absent, and returns the new
     * value. Throws {@see NotNumericException} if the existing value is
     * not an integer -- incr() never silently reinterprets a float.
     */
    public function incr(string $key, int $amount = 1): int
    {
        $this->assertOpen();
        $text = $this->atomicIncr($key, (string) $amount, Schema::TYPE_INT, (string) Schema::TYPE_INT);

        if (preg_match('/^-?\d+$/', $text) !== 1) {
            throw new NotNumericException("lytecache: stored value for key \"{$key}\" is not a valid integer: {$text}");
        }

        return (int) $text;
    }

    /** Equivalent to incr($key, -$amount). */
    public function decr(string $key, int $amount = 1): int
    {
        return $this->incr($key, -$amount);
    }

    /**
     * Atomically adds $amount to the numeric value stored at $key (which
     * may be an integer or a float), creating it (starting from 0) if
     * absent, and returns the new value as a float. Throws
     * {@see NotNumericException} if the existing value is not numeric.
     */
    public function incrFloat(string $key, float $amount): float
    {
        $this->assertOpen();
        $allowed = Schema::TYPE_INT.','.Schema::TYPE_FLOAT;
        $text = $this->atomicIncr($key, json_encode($amount, JSON_THROW_ON_ERROR), Schema::TYPE_FLOAT, $allowed);

        if (! is_numeric($text)) {
            throw new NotNumericException("lytecache: stored value for key \"{$key}\" is not a valid float: {$text}");
        }

        return (float) $text;
    }

    // ---------------------------------------------------------------
    // Hashes: field-level access to JSON object values
    // ---------------------------------------------------------------
    //
    // A hash is not a separate type: it is a view over an ordinary
    // value_type-4 row whose JSON root is an object. Anything written with
    // set($key, [...]) is usable with hget(), and get() on a hash returns the
    // whole document. The reason these exist is atomicity: on the JSON1 path
    // every write is computed inside SQLite from the row's *current* value
    // (json_set/json_remove), so two PHP-FPM workers updating different
    // fields never lose each other's write. Where an exact return value needs
    // more than one statement (hset's "created", hdel's count), guarded
    // writes run inside one BEGIN IMMEDIATE transaction; no write depends on
    // data read into PHP. The read-modify-write fallback (system SQLite
    // without JSON1 -- the realistic case for PHP -- or
    // LYTECACHE_FORCE_JSON_FALLBACK=1) holds the write lock throughout.

    private const FORCE_JSON_FALLBACK_ENV = 'LYTECACHE_FORCE_JSON_FALLBACK';

    private const HSET_MANY_PER_STATEMENT = 500;

    private const JSON_SET_PAIRS = 30;

    private const DOC = 'CAST(cache.value AS TEXT)';

    private const LIVE = '(cache.expires_at IS NULL OR cache.expires_at > :now)';

    private const EXPIRED = '(cache.expires_at IS NOT NULL AND cache.expires_at <= :now)';

    private const ROOT_TYPE = '(CASE WHEN json_valid('.self::DOC.') THEN json_type('.self::DOC.') END)';

    private const IS_OBJ = '(cache.value_type = 4 AND CASE WHEN json_valid('.self::DOC.') THEN json_type('.self::DOC.") END = 'object')";

    private const TYPE_NAMES = [0 => 'bytes', 1 => 'string', 2 => 'int', 3 => 'float', 4 => 'json', 5 => 'pickle', 6 => 'java-native'];

    /** Field names are always bound, never interpolated: $."field" with \ and " escaped, in SQL. */
    private static function jsonPath(string $param): string
    {
        return <<<SQL
            ('$."' || replace(replace(:{$param}, '\\', '\\\\'), '"', '\\"') || '"')
            SQL;
    }

    private static function fieldType(string $param): string
    {
        return '(CASE WHEN '.self::IS_OBJ.' THEN json_type('.self::DOC.', '.self::jsonPath($param).') END)';
    }

    /**
     * JSON text for a REAL with the fewest digits that round-trip exactly.
     * SQLite before ~3.5x renders a REAL into JSON (and CAST(x AS TEXT)) with
     * 15 significant digits, which would store 0.1 + 0.2 as 0.3; this picks
     * the shortest of 15/16/17 digits that reads back to the same double, on
     * every SQLite version.
     */
    private static function floatText(string $expr): string
    {
        $p = static fn (int $n): string => "printf('%!.{$n}g', {$expr})";

        return "(CASE WHEN CAST({$p(15)} AS REAL) = {$expr} THEN {$p(15)} WHEN CAST({$p(16)} AS REAL) = {$expr} THEN {$p(16)} ELSE {$p(17)} END)";
    }

    /**
     * An UPSERT that creates $freshDoc for a missing/expired key, or
     * rewrites a live JSON object to $liveDoc when $where holds. expires_at
     * is preserved on a live row and size_bytes is recomputed from the new
     * value in the same statement.
     */
    private static function upsertHashSql(string $liveDoc, string $freshDoc, string $where): string
    {
        $expired = self::EXPIRED;
        $newDoc = "CAST(CASE WHEN {$expired} THEN {$freshDoc} ELSE {$liveDoc} END AS BLOB)";

        return <<<SQL
            INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
            VALUES (:key, :ns, CAST({$freshDoc} AS BLOB), 4, :now, :exp, :now, 0, length(CAST({$freshDoc} AS BLOB)))
            ON CONFLICT(namespace, key) DO UPDATE SET
              value = {$newDoc},
              value_type = 4,
              created_at = CASE WHEN {$expired} THEN :now ELSE cache.created_at END,
              expires_at = CASE WHEN {$expired} THEN :exp ELSE cache.expires_at END,
              last_accessed = :now,
              access_count = CASE WHEN {$expired} THEN 0 ELSE cache.access_count + 1 END,
              size_bytes = length({$newDoc})
            WHERE {$expired} OR {$where}
            SQL;
    }

    /**
     * Probes what this SQLite's JSON1 can do. Native mode needs the JSON
     * functions, the -> operator, and path labels matched against the *decoded*
     * key: a document written as {"\u00e9":1} must be found by $."é", and a
     * backslash must round-trip. SQLite before 3.45 matches the raw encoded text
     * instead, so a document from another writer would get a duplicate key; it
     * stays on the fallback. Escaped double quotes in labels arrive later
     * (3.48 here; 3.46.1 lacks them), and are probed separately.
     *
     * @return array{bool, bool} [native, quoted labels]
     */
    private function detectJson1(): array
    {
        if ((string) getenv(self::FORCE_JSON_FALLBACK_ENV) !== '') {
            ($this->logger)('lytecache: '.self::FORCE_JSON_FALLBACK_ENV.' is set; hash operations use the read-modify-write fallback');

            return [false, false];
        }
        $native = false;
        try {
            $row = self::query($this->pdo, <<<'SQL'
                SELECT json_valid('{}') AS ok,
                       '{"\u00e9":[1]}' -> '$."é"' AS decoded,
                       '{"a\\b":[2]}' -> '$."a\\b"' AS backslash
                SQL)->fetch(\PDO::FETCH_ASSOC);
            $native = is_array($row) && (int) $row['ok'] === 1 && $row['decoded'] === '[1]' && $row['backslash'] === '[2]';
        } catch (\PDOException) {
            // no JSON1, or no -> operator
        }
        if ($native) {
            try {
                $quoted = self::query($this->pdo, <<<'SQL'
                    SELECT '{"a\"b":[1]}' -> '$."a\"b"'
                    SQL)->fetchColumn() === '[1]';
            } catch (\PDOException) {
                $quoted = false;
            }

            return [true, $quoted];
        }
        $version = (string) self::query($this->pdo, 'SELECT sqlite_version()')->fetchColumn();
        ($this->logger)("lytecache: SQLite {$version} lacks the JSON1 support hash operations need (SQLite 3.45 or newer: JSON "
            .'functions, the -> operator and decoded path labels); hash operations use the read-modify-write fallback under BEGIN IMMEDIATE');

        return [false, false];
    }

    /**
     * Whether a hash call naming these fields can run as single SQL statements.
     * False means the read-modify-write fallback, which has identical results.
     */
    private function nativeFor(string ...$fields): bool
    {
        if (! $this->json1) {
            return false;
        }
        if ($this->json1QuotedLabels) {
            return true;
        }
        foreach ($fields as $f) {
            if (str_contains($f, '"')) {
                return false;
            }
        }

        return true;
    }

    /** "json1" (single SQL statements) or "fallback" (read-modify-write under BEGIN IMMEDIATE). */
    public function hashMode(): string
    {
        return $this->json1 ? 'json1' : 'fallback';
    }

    /**
     * Prepares and runs $sql, binding ints as INTEGER (PDO would otherwise
     * bind them as TEXT, which json_object() would store as strings), null
     * as NULL and {@see Bytes} as a BLOB.
     *
     * @param  array<string, int|float|string|bool|null|Bytes>  $params
     */
    private function run(string $sql, array $params): \PDOStatement
    {
        $stmt = $this->prepare($sql);
        foreach ($params as $k => $v) {
            // pdo_sqlite rejects a bound name the statement doesn't use, so
            // only bind those that appear (":f1" must not match ":f10").
            if (preg_match('/'.preg_quote($k, '/').'(?![A-Za-z0-9_])/', $sql) !== 1) {
                continue;
            }
            match (true) {
                is_int($v) => $stmt->bindValue($k, $v, \PDO::PARAM_INT),
                is_bool($v) => $stmt->bindValue($k, $v ? 1 : 0, \PDO::PARAM_INT),
                $v === null => $stmt->bindValue($k, null, \PDO::PARAM_NULL),
                $v instanceof Bytes => $stmt->bindValue($k, $v->value, \PDO::PARAM_LOB),
                default => $stmt->bindValue($k, (string) $v),
            };
        }
        $stmt->execute();

        return $stmt;
    }

    /** @return array<string, int|string|null> */
    private function hashParams(string $key, int $now, ?float $ttl): array
    {
        return [':ns' => $this->namespace, ':key' => $key, ':now' => $now, ':exp' => $ttl === null ? null : $now + (int) round($ttl * 1000)];
    }

    /** @internal PDO::query() with ERRMODE_EXCEPTION, typed as never returning false. */
    public static function query(\PDO $pdo, string $sql): \PDOStatement
    {
        $stmt = $pdo->query($sql);
        if ($stmt === false) {
            throw new \RuntimeException("lytecache: query failed: {$sql}"); // @codeCoverageIgnore
        }

        return $stmt;
    }

    private static function wrongType(string $key, int $valueType, ?string $rootType): SerializationException
    {
        if ($valueType !== Schema::TYPE_JSON) {
            $name = self::TYPE_NAMES[$valueType] ?? 'unknown';

            return new SerializationException("lytecache: key \"{$key}\" holds a value of type {$name} (value_type={$valueType}), "
                .'not a JSON object; hash operations apply only to JSON objects (value_type=4)');
        }
        if ($rootType === null) {
            return new SerializationException("lytecache: key \"{$key}\" holds malformed JSON (value_type=4)");
        }

        return new SerializationException("lytecache: key \"{$key}\" holds a JSON {$rootType}, not a JSON object; hash operations apply only to JSON objects");
    }

    private function checkHashType(string $key, int $now): void
    {
        $stmt = $this->run('SELECT cache.value_type AS vt, '.self::ROOT_TYPE.' AS root FROM cache WHERE namespace = :ns AND key = :key AND '.self::LIVE,
            [':ns' => $this->namespace, ':key' => $key, ':now' => $now]);
        /** @var array{vt: int, root: string|null}|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();
        if ($row !== false && ! ((int) $row['vt'] === Schema::TYPE_JSON && $row['root'] === 'object')) {
            throw self::wrongType($key, (int) $row['vt'], $row['root']);
        }
    }

    /** Fallback path: the live document, or null if the key is missing/expired. */
    private function loadHash(string $key, int $now): ?JsonObject
    {
        $stmt = $this->run('SELECT value, value_type FROM cache WHERE namespace = :ns AND key = :key AND '.self::LIVE,
            [':ns' => $this->namespace, ':key' => $key, ':now' => $now]);
        /** @var array{value: string, value_type: int}|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();
        if ($row === false) {
            return null;
        }
        if ((int) $row['value_type'] !== Schema::TYPE_JSON) {
            throw self::wrongType($key, (int) $row['value_type'], null);
        }
        try {
            [$obj, $root] = JsonObject::parse($row['value']);
        } catch (\JsonException) {
            throw self::wrongType($key, Schema::TYPE_JSON, null);
        }
        if ($obj === null) {
            throw self::wrongType($key, Schema::TYPE_JSON, $root);
        }

        return $obj;
    }

    /**
     * Fallback path: read, mutate in PHP, write back -- all under BEGIN
     * IMMEDIATE. $mutate returns [changed, result].
     *
     * @param  callable(JsonObject): array{0: bool, 1: mixed}  $mutate
     */
    private function rmwHash(string $key, ?float $ttl, bool $create, callable $mutate): mixed
    {
        $now = self::nowMillis();
        $result = null;
        $wrote = false;
        $this->inWriteTransaction(function () use ($key, $ttl, $create, $mutate, $now, &$result, &$wrote) {
            $wrote = false;
            $doc = $this->loadHash($key, $now);
            $exists = $doc !== null;
            if ($doc === null) {
                if (! $create) {
                    $result = $mutate(new JsonObject)[1];

                    return;
                }
                if ($this->eviction === Eviction::NoEviction && ($this->maxKeys !== null || $this->maxBytes !== null)) {
                    $this->checkCapacity($key);
                }
                $doc = new JsonObject;
            }
            [$changed, $result] = $mutate($doc);
            if (! $changed) {
                return;
            }
            $blob = $doc->toJson();
            $params = $this->hashParams($key, $now, $ttl) + [':v' => new Bytes($blob), ':size' => strlen($blob)];
            if ($exists) {
                $this->run('UPDATE cache SET value = :v, size_bytes = :size, last_accessed = :now, access_count = access_count + 1 '
                    .'WHERE namespace = :ns AND key = :key', $params);
            } else {
                $this->run('INSERT OR REPLACE INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, '
                    .'access_count, size_bytes) VALUES (:key, :ns, :v, 4, :now, :exp, :now, 0, :size)', $params);
            }
            $wrote = true;
        });
        if ($wrote) {
            $this->changed('set', [$key], 1);
            $this->maybeEvict();
        }

        return $result;
    }

    /**
     * Encodes one field value as compact JSON text, with the same rules as
     * stored JSON values. {@see Bytes} is rejected, as are NAN/INF.
     */
    private static function encodeHashValue(mixed $value): string
    {
        if ($value instanceof Bytes) {
            throw new SerializationException('lytecache: hash fields hold JSON values; Bytes is not supported');
        }
        if (is_float($value) && (is_nan($value) || is_infinite($value))) {
            throw new SerializationException('lytecache: cannot store NAN or INF in a hash field');
        }
        if (is_string($value) || is_int($value) || is_float($value)) {
            try {
                return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR);
            } catch (\JsonException $e) {
                throw new SerializationException('lytecache: cannot encode hash field: '.$e->getMessage(), previous: $e);
            }
        }
        [$json] = Serializer::encode($value);

        return $json;
    }

    private static function decodeHashValue(string $json): mixed
    {
        try {
            return json_decode($json, associative: true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new SerializationException('lytecache: stored hash field is not valid JSON: '.$e->getMessage(), previous: $e);
        }
    }

    private function capacityCheckFor(string $key): void
    {
        if ($this->eviction === Eviction::NoEviction && ($this->maxKeys !== null || $this->maxBytes !== null)) {
            $this->checkCapacity($key);
        }
    }

    /**
     * Atomically sets one field of the JSON object at $key. Returns true if
     * the field was new, false if it replaced a value. A missing (or expired)
     * key is created as a new object with NO expiry unless $ttl is given;
     * $ttl is ignored for an existing key, whose expiry a field write never
     * changes. Throws {@see SerializationException} if $key holds anything
     * but a JSON object.
     */
    public function hset(string $key, string $field, mixed $value, ?float $ttl = null): bool
    {
        $this->assertOpen();
        $encoded = self::encodeHashValue($value);
        if (! $this->nativeFor($field)) {
            return (bool) $this->rmwHash($key, $ttl, true, static function (JsonObject $doc) use ($field, $encoded): array {
                $created = ! $doc->has($field);
                $doc->set($field, $encoded);

                return [true, $created];
            });
        }
        $this->capacityCheckFor($key);
        $now = self::nowMillis();
        $params = $this->hashParams($key, $now, $ttl) + [':f0' => $field, ':v0' => $encoded];
        $setExpr = 'json_set('.self::DOC.', '.self::jsonPath('f0').', json(:v0))';
        $created = false;
        $this->inWriteTransaction(function () use ($params, $setExpr, $key, $now, &$created) {
            $created = $this->run(self::upsertHashSql($setExpr, 'json_object(:f0, json(:v0))',
                '('.self::fieldType('f0').' IS NULL AND '.self::IS_OBJ.')'), $params)->rowCount() === 1;
            if ($created) {
                return;
            }
            $n = $this->run("UPDATE cache SET value = CAST({$setExpr} AS BLOB), size_bytes = length(CAST({$setExpr} AS BLOB)), "
                .'last_accessed = :now, access_count = access_count + 1 WHERE namespace = :ns AND key = :key AND '.self::LIVE
                .' AND '.self::IS_OBJ, $params)->rowCount();
            if ($n === 0) {
                $this->checkHashType($key, $now);
            }
        });
        $this->changed('set', [$key], 1);
        $this->maybeEvict();
        $this->maybeMaintain();

        return $created;
    }

    /** Sets $field only if it is absent, atomically; returns whether it was set. */
    public function hsetnx(string $key, string $field, mixed $value, ?float $ttl = null): bool
    {
        $this->assertOpen();
        $encoded = self::encodeHashValue($value);
        if (! $this->nativeFor($field)) {
            return (bool) $this->rmwHash($key, $ttl, true, static function (JsonObject $doc) use ($field, $encoded): array {
                if ($doc->has($field)) {
                    return [false, false];
                }
                $doc->set($field, $encoded);

                return [true, true];
            });
        }
        $this->capacityCheckFor($key);
        $now = self::nowMillis();
        $params = $this->hashParams($key, $now, $ttl) + [':f0' => $field, ':v0' => $encoded];
        $won = false;
        $this->inWriteTransaction(function () use ($params, $key, $now, &$won) {
            $won = $this->run(self::upsertHashSql('json_set('.self::DOC.', '.self::jsonPath('f0').', json(:v0))', 'json_object(:f0, json(:v0))',
                '('.self::fieldType('f0').' IS NULL AND '.self::IS_OBJ.')'), $params)->rowCount() === 1;
            if (! $won) {
                $this->checkHashType($key, $now);
            }
        });
        if ($won) {
            $this->changed('set', [$key], 1);
            $this->maybeEvict();
        }

        return $won;
    }

    /**
     * Sets several fields at once, atomically (Redis' multi-field HSET): all
     * land in one transaction or none do.
     *
     * @param  array<string, mixed>  $fields
     */
    public function hsetMany(string $key, array $fields, ?float $ttl = null): void
    {
        $this->assertOpen();
        $entries = [];
        foreach ($fields as $f => $v) {
            $entries[] = [(string) $f, self::encodeHashValue($v)];
        }
        if ($entries === []) {
            return;
        }
        if (! $this->nativeFor(...array_column($entries, 0))) {
            $this->rmwHash($key, $ttl, true, static function (JsonObject $doc) use ($entries): array {
                foreach ($entries as [$f, $v]) {
                    $doc->set($f, $v);
                }

                return [true, null];
            });

            return;
        }
        $this->capacityCheckFor($key);
        $now = self::nowMillis();
        $this->inWriteTransaction(function () use ($entries, $key, $now, $ttl) {
            // SQLite caps function arguments and bound variables, so a large
            // mapping is applied as several statements -- each json_set
            // nested over groups of pairs -- inside this one transaction.
            for ($start = 0; $start < count($entries); $start += self::HSET_MANY_PER_STATEMENT) {
                $end = min($start + self::HSET_MANY_PER_STATEMENT, count($entries));
                $params = $this->hashParams($key, $now, $ttl);
                $liveDoc = self::DOC;
                $freshDoc = "'{}'";
                for ($g = $start; $g < $end; $g += self::JSON_SET_PAIRS) {
                    $pairs = [];
                    for ($i = $g; $i < min($g + self::JSON_SET_PAIRS, $end); $i++) {
                        $params[":f{$i}"] = $entries[$i][0];
                        $params[":v{$i}"] = $entries[$i][1];
                        $pairs[] = self::jsonPath("f{$i}").", json(:v{$i})";
                    }
                    $joined = implode(', ', $pairs);
                    $liveDoc = "json_set({$liveDoc}, {$joined})";
                    $freshDoc = "json_set({$freshDoc}, {$joined})";
                }
                if ($this->run(self::upsertHashSql($liveDoc, $freshDoc, self::IS_OBJ), $params)->rowCount() === 0) {
                    $this->checkHashType($key, $now);
                }
            }
        });
        $this->changed('set', [$key], 1);
        $this->maybeEvict();
    }

    /**
     * One field's JSON text per requested field (null where missing), or
     * null for a missing key.
     *
     * @param  list<string>  $fields
     * @return list<string|null>|null
     */
    private function hmgetRaw(string $key, array $fields): ?array
    {
        $this->assertOpen();
        $now = self::nowMillis();
        if (! $this->nativeFor(...$fields)) {
            $doc = $this->loadHash($key, $now);
            $this->recordHashRead($key, $now, $doc !== null);

            return $doc === null ? null : array_map(static fn (string $f): ?string => $doc->get($f), $fields);
        }
        $params = [':ns' => $this->namespace, ':key' => $key, ':now' => $now];
        $cols = [];
        foreach ($fields as $i => $f) {
            $params[":f{$i}"] = $f;
            $cols[] = 'CASE WHEN '.self::IS_OBJ.' THEN '.self::DOC.' -> '.self::jsonPath("f{$i}")." END AS c{$i}";
        }
        $stmt = $this->run('SELECT cache.value_type AS vt, '.self::ROOT_TYPE.' AS root, '.implode(', ', $cols)
            .' FROM cache WHERE namespace = :ns AND key = :key AND '.self::LIVE, $params);
        /** @var array<string, mixed>|false $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();
        if ($row === false) {
            $this->recordHashRead($key, $now, false);

            return null;
        }
        if (! ((int) $row['vt'] === Schema::TYPE_JSON && $row['root'] === 'object')) {
            throw self::wrongType($key, (int) $row['vt'], is_string($row['root']) ? $row['root'] : null);
        }
        $this->recordHashRead($key, $now, true);
        $out = [];
        foreach (array_keys($fields) as $i) {
            $v = $row["c{$i}"];
            $out[] = is_string($v) ? $v : null;
        }

        return $out;
    }

    /**
     * Returns one field of the JSON object at $key (JSON objects decode to
     * arrays, as with get()), or $default on a missing key, missing field or
     * expired key -- never throws on a miss. A stored JSON null is returned
     * as null.
     */
    public function hget(string $key, string $field, mixed $default = null): mixed
    {
        $raw = $this->hmgetRaw($key, [$field]);
        $v = $raw[0] ?? null;

        return $v === null ? $default : self::decodeHashValue($v);
    }

    /**
     * Several fields, in order, with null for each one that is missing.
     *
     * @param  list<string>  $fields
     * @return list<mixed>
     */
    public function hmget(string $key, array $fields): array
    {
        if ($fields === []) {
            return [];
        }
        $raw = $this->hmgetRaw($key, $fields);

        return array_map(static fn (?string $v): mixed => $v === null ? null : self::decodeHashValue($v), $raw ?? array_fill(0, count($fields), null));
    }

    /**
     * The whole object at $key, in document order ([] if missing).
     *
     * @return array<string, mixed>
     */
    public function hgetAll(string $key): array
    {
        $out = [];
        foreach ($this->hashEntries($key, true) as [$k, $v]) {
            $out[$k] = self::decodeHashValue($v);
        }

        return $out;
    }

    /**
     * Removes fields; returns how many were present and removed. Unlike
     * Redis, removing the last field leaves an empty object {} rather than
     * deleting the key. A missing key returns 0.
     */
    public function hdel(string $key, string ...$fields): int
    {
        $this->assertOpen();
        $unique = array_values(array_unique($fields));
        if ($unique === []) {
            return 0;
        }
        if (! $this->nativeFor(...$unique)) {
            return (int) $this->rmwHash($key, null, false, static function (JsonObject $doc) use ($unique): array {
                $n = 0;
                foreach ($unique as $f) {
                    $n += $doc->delete($f) ? 1 : 0;
                }

                return [$n > 0, $n];
            });
        }
        $now = self::nowMillis();
        $remove = 'json_remove('.self::DOC.', '.self::jsonPath('f0').')';
        $sql = "UPDATE cache SET value = CAST({$remove} AS BLOB), size_bytes = length(CAST({$remove} AS BLOB)), last_accessed = :now, "
            .'access_count = access_count + :bump WHERE namespace = :ns AND key = :key AND '.self::LIVE.' AND '.self::fieldType('f0').' IS NOT NULL';
        $removed = 0;
        $this->inWriteTransaction(function () use ($unique, $sql, $key, $now, &$removed) {
            $removed = 0;
            foreach ($unique as $f) {
                // One hdel call is one access, however many statements it takes.
                $removed += $this->run($sql, [':ns' => $this->namespace, ':key' => $key, ':now' => $now, ':f0' => $f,
                    ':bump' => $removed === 0 ? 1 : 0])->rowCount();
            }
            if ($removed === 0) {
                $this->checkHashType($key, $now);
            }
        });
        if ($removed > 0) {
            $this->changed('set', [$key], 1);
        }

        return $removed;
    }

    /** Whether $key is a live JSON object containing $field. */
    public function hexists(string $key, string $field): bool
    {
        $raw = $this->hmgetRaw($key, [$field]);

        return $raw !== null && $raw[0] !== null;
    }

    /**
     * Field names in document order ([] if missing).
     *
     * @return list<string>
     */
    public function hkeys(string $key): array
    {
        return array_map(static fn (array $e): string => $e[0], $this->hashEntries($key, false));
    }

    /**
     * Field values in document order ([] if missing).
     *
     * @return list<mixed>
     */
    public function hvals(string $key): array
    {
        return array_map(static fn (array $e): mixed => self::decodeHashValue($e[1]), $this->hashEntries($key, true));
    }

    /** Number of fields (0 if missing). */
    public function hlen(string $key): int
    {
        return count($this->hashEntries($key, false));
    }

    /**
     * @return list<array{0: string, 1: string}>
     */
    private function hashEntries(string $key, bool $withValues, bool $forceFallback = false): array
    {
        $this->assertOpen();
        $now = self::nowMillis();
        if (! $this->json1 || $forceFallback) {
            $doc = $this->loadHash($key, $now);
            if ($doc !== null) {
                $this->bufferLru($key, $now);
            }

            return $doc === null ? [] : $doc->fields;
        }
        $valueCol = $withValues ? 'CASE WHEN j.key IS NOT NULL THEN '.self::DOC.' -> j.fullkey END' : 'NULL';
        $stmt = $this->run('SELECT cache.value_type AS vt, '.self::ROOT_TYPE." AS root, j.key AS k, {$valueCol} AS v FROM cache "
            .'LEFT JOIN json_each(CASE WHEN '.self::IS_OBJ.' THEN '.self::DOC." ELSE '{}' END) AS j "
            .'WHERE cache.namespace = :ns AND cache.key = :key AND '.self::LIVE, [':ns' => $this->namespace, ':key' => $key, ':now' => $now]);
        /** @var list<array{vt: int, root: string|null, k: string|null, v: string|null}> $rows */
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        if ($rows === []) {
            return [];
        }
        if (! ((int) $rows[0]['vt'] === Schema::TYPE_JSON && $rows[0]['root'] === 'object')) {
            throw self::wrongType($key, (int) $rows[0]['vt'], $rows[0]['root']);
        }
        // Values are read through paths SQLite builds from the stored keys
        // (j.fullkey). On SQLite 3.45-3.47 such a path can't address a key
        // containing ", so a document holding one is re-read via the fallback.
        if ($withValues && ! $this->json1QuotedLabels) {
            foreach ($rows as $r) {
                if ($r['k'] !== null && str_contains((string) $r['k'], '"')) {
                    return $this->hashEntries($key, true, true);
                }
            }
        }
        $this->bufferLru($key, $now);
        $out = [];
        foreach ($rows as $r) {
            if ($r['k'] !== null) {
                $out[] = [(string) $r['k'], $r['v'] ?? 'null'];
            }
        }

        return $out;
    }

    /**
     * Atomically adds $amount to an integer field and returns the new value.
     * A missing field starts at 0; a missing key is created (no expiry unless
     * $ttl). Throws {@see NotNumericException} if the field holds anything
     * but a JSON integer, and \OverflowException -- writing nothing -- if the
     * result would leave the signed 64-bit range.
     */
    public function hincrBy(string $key, string $field, int $amount = 1, ?float $ttl = null): int
    {
        return (int) $this->hincr($key, $field, $amount, $ttl, true);
    }

    /** Atomically adds $amount to a numeric field and returns the new value, stored as a float. */
    public function hincrByFloat(string $key, string $field, float $amount, ?float $ttl = null): float
    {
        if (is_nan($amount) || is_infinite($amount)) {
            throw new \InvalidArgumentException('lytecache: hincrByFloat amount must be finite');
        }

        return (float) $this->hincr($key, $field, $amount, $ttl, false);
    }

    private function hincr(string $key, string $field, int|float $amount, ?float $ttl, bool $integer): string
    {
        $this->assertOpen();
        $kind = $integer ? 'an integer' : 'a number';
        if (! $this->nativeFor($field)) {
            return (string) $this->rmwHash($key, $ttl, true, static function (JsonObject $doc) use ($field, $amount, $integer, $kind, $key): array {
                $raw = trim($doc->get($field) ?? '0');
                if ($integer) {
                    if (preg_match('/^-?\d+$/', $raw) !== 1) {
                        throw new NotNumericException("lytecache: hash field \"{$field}\" of key \"{$key}\" is not {$kind}");
                    }
                    $cur = filter_var($raw, FILTER_VALIDATE_INT);
                    if ($cur === false) {
                        throw new \OverflowException("lytecache: incrementing field \"{$field}\" of key \"{$key}\" would overflow");
                    }
                    $add = (int) $amount;
                    if (($add > 0 && $cur > PHP_INT_MAX - $add) || ($add < 0 && $cur < PHP_INT_MIN - $add)) {
                        throw new \OverflowException("lytecache: incrementing field \"{$field}\" of key \"{$key}\" would overflow");
                    }
                    $next = $cur + $add;
                    $doc->set($field, (string) $next);

                    return [true, (string) $next];
                }
                if (! is_numeric($raw)) {
                    throw new NotNumericException("lytecache: hash field \"{$field}\" of key \"{$key}\" is not {$kind}");
                }
                $next = (float) $raw + (float) $amount;
                if (is_infinite($next)) {
                    throw new \OverflowException("lytecache: incrementing field \"{$field}\" of key \"{$key}\" would overflow");
                }
                $text = json_encode($next, JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR);
                $doc->set($field, $text);

                return [true, $text];
            });
        }
        $this->capacityCheckFor($key);
        $now = self::nowMillis();
        $params = $this->hashParams($key, $now, $ttl) + [':f0' => $field, ':amount' => $integer ? (int) $amount : json_encode((float) $amount, JSON_THROW_ON_ERROR)];
        // PDO has no float binding, so the float amount travels as text and is CAST here.
        $amountSql = $integer ? ':amount' : 'CAST(:amount AS REAL)';
        $stored = 'json_extract('.self::DOC.', '.self::jsonPath('f0').')';
        $newVal = $integer ? "(COALESCE({$stored}, 0) + {$amountSql})" : "(CAST(COALESCE({$stored}, 0) AS REAL) + {$amountSql})";
        $guard = $integer
            ? 'COALESCE('.self::fieldType('f0').", 'integer') = 'integer' AND typeof({$newVal}) = 'integer'"
            : 'COALESCE('.self::fieldType('f0').", 'real') IN ('integer', 'real') AND {$newVal} BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308";
        $storedVal = $integer ? $newVal : 'json('.self::floatText($newVal).')';
        $sql = self::upsertHashSql('json_set('.self::DOC.', '.self::jsonPath('f0').", {$storedVal})", "json_object(:f0, {$amountSql})",
            '(CASE WHEN '.self::IS_OBJ." THEN ({$guard}) ELSE 0 END)")
            .' RETURNING CAST(json_extract(CAST(value AS TEXT), '.self::jsonPath('f0').') AS TEXT) AS r'
            .($integer ? '' : ', json_extract(CAST(value AS TEXT), '.self::jsonPath('f0').') AS real_value');
        $result = '';
        $this->inWriteTransaction(function () use ($sql, $params, $integer, $kind, $field, $key, $now, &$result) {
            $stmt = $this->run($sql, $params);
            /** @var array{r: string, real_value?: float}|false $row */
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
            $stmt->closeCursor();
            if ($row !== false) {
                // Floats come back as the REAL itself: CAST(real AS TEXT) keeps only 15 digits.
                $result = $integer ? (string) $row['r'] : json_encode((float) ($row['real_value'] ?? 0.0), JSON_THROW_ON_ERROR);

                return;
            }
            $this->checkHashType($key, $now);
            $ftStmt = $this->run('SELECT '.self::fieldType('f0').' AS t FROM cache WHERE namespace = :ns AND key = :key', $params);
            $ft = $ftStmt->fetchColumn();
            $ftStmt->closeCursor();
            if (($integer && $ft === 'integer') || (! $integer && ($ft === 'integer' || $ft === 'real'))) {
                throw new \OverflowException("lytecache: incrementing field \"{$field}\" of key \"{$key}\" would overflow");
            }
            throw new NotNumericException("lytecache: hash field \"{$field}\" of key \"{$key}\" holds a JSON {$ft}, not {$kind}");
        });
        $this->changed('set', [$key], 1);
        $this->maybeEvict();

        return $result;
    }

    private function recordHashRead(string $key, int $now, bool $found): void
    {
        if ($found) {
            $this->hits++;
            $this->bufferLru($key, $now);
        } else {
            $this->misses++;
        }
        $this->maybeMaintain();
    }

    // ---------------------------------------------------------------
    // Events: local notification (notify / watch)
    // ---------------------------------------------------------------
    //
    // SINGLE HOST ONLY. POLLED, NOT PUSHED. DURABLE. Not Redis Pub/Sub:
    // events are rows in the `events` table with a monotonic seq; watchers
    // on the same machine poll it. See docs/notifications.md.

    private const CACHE_CHANNEL = '__cache__';

    private const EVENTS_DDL = <<<'SQL'
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
        SQL;

    private const CHANGE_OPS = ['set', 'delete', 'flush'];

    private const MAX_DISPATCH_DEPTH = 8;

    private function ensureEvents(): void
    {
        if ($this->eventsReady) {
            return;
        }
        $this->executeWithRetry(fn () => $this->pdo->exec(self::EVENTS_DDL));
        $this->eventsReady = true;
    }

    private function eventsExist(): bool
    {
        if (! $this->eventsReady) {
            $this->eventsReady = self::query($this->pdo, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'")->fetchColumn() !== false;
        }

        return $this->eventsReady;
    }

    private static function validateChannel(string $channel): void
    {
        if ($channel === '') {
            throw new \InvalidArgumentException('lytecache: channel must be non-empty');
        }
        if (str_contains($channel, '*')) {
            throw new \InvalidArgumentException("lytecache: channel \"{$channel}\" may not contain '*' (reserved for watch patterns)");
        }
        if (str_starts_with($channel, '__')) {
            throw new \InvalidArgumentException("lytecache: channel names starting with '__' are reserved: \"{$channel}\"");
        }
    }

    /**
     * Publishes an event for watchers ON THIS HOST and returns its seq.
     * Single host only, delivered by polling (not pushed), durable until
     * retention deletes it ($eventRetention, default 1 hour). $payload is
     * encoded exactly like a cached value. Each call is one write under the
     * same lock as set(), so a high event rate slows cache writes; batch with
     * notifyMany(). Call it after your own database transaction commits,
     * never inside one you might roll back.
     */
    public function notify(string $channel, mixed $payload = null): int
    {
        $this->assertOpen();
        self::validateChannel($channel);

        return $this->insertEvents([[$channel, $payload]])[0];
    }

    /**
     * Publishes several [channel, payload] events in one transaction.
     *
     * @param  list<array{0: string, 1: mixed}>  $events
     * @return list<int> their seqs, in order
     */
    public function notifyMany(array $events): array
    {
        $this->assertOpen();
        foreach ($events as [$channel]) {
            self::validateChannel($channel);
        }

        return $events === [] ? [] : $this->insertEvents($events);
    }

    /**
     * @param  list<array{0: string, 1: mixed}>  $events
     * @return list<int>
     */
    private function insertEvents(array $events): array
    {
        $now = self::nowMillis();
        $rows = [];
        foreach ($events as [$channel, $payload]) {
            [$data, $type] = $payload === null ? ['null', Schema::TYPE_JSON] : Serializer::encode($payload);
            $rows[] = [$channel, $data, $type];
        }
        $this->ensureEvents();
        $seqs = [];
        $this->inWriteTransaction(function () use ($rows, $now, &$seqs) {
            $seqs = [];
            foreach ($rows as [$channel, $data, $type]) {
                $stmt = $this->run('INSERT INTO events (channel, namespace, payload, value_type, created_at) '
                    .'VALUES (:ch, :ns, :p, :t, :now) RETURNING seq',
                    [':ch' => $channel, ':ns' => $this->namespace, ':p' => new Bytes($data), ':t' => $type, ':now' => $now]);
                $seqs[] = (int) $stmt->fetchColumn();
                $stmt->closeCursor(); // a RETURNING row left unread would block COMMIT
            }
        });
        // In-process optimization only: skip this cache's watchers' remaining backoff.
        foreach ($this->subscriptions as $sub) {
            $sub->wake();
        }

        return $seqs;
    }

    /**
     * Subscribes to events published ON THIS HOST to matching channels:
     * exact names, or a trailing '*' for a prefix ("cache:invalidate:*");
     * "*" matches every channel. Only events from this cache's namespace are
     * seen. $fromSeq: null for only new events, N to resume after seq N, 0
     * to replay everything retained. Delivery is at-least-once, in total seq
     * order, to every subscriber (fan-out) -- handlers must be idempotent.
     * Lost events throw {@see GapException}.
     *
     * @param  string|list<string>  $channels
     */
    public function watch(string|array $channels, ?int $fromSeq = null, float $pollInterval = 0.05): Subscription
    {
        $this->assertOpen();
        $patterns = is_string($channels) ? [$channels] : $channels;
        if ($patterns === []) {
            throw new \InvalidArgumentException('lytecache: watch() needs at least one channel or pattern');
        }
        $ignored = [];
        Subscription::channelFilter($patterns, $ignored); // validates
        if ($fromSeq !== null && $fromSeq < 0) {
            throw new \InvalidArgumentException('lytecache: fromSeq must be >= 0');
        }
        if ($pollInterval <= 0) {
            throw new \InvalidArgumentException('lytecache: pollInterval must be positive');
        }
        $this->ensureEvents();
        $gap = null;
        $this->pdo->exec('BEGIN');
        try {
            if ($fromSeq === null) {
                $start = Subscription::lastAssignedSeq($this->pdo);
            } elseif ($fromSeq === 0) {
                $oldest = Subscription::oldestSeq($this->pdo);
                $start = $oldest !== null ? $oldest - 1 : Subscription::lastAssignedSeq($this->pdo);
            } else {
                $start = $fromSeq;
                $gap = Subscription::gapCheck($this->pdo, $start);
            }
        } finally {
            $this->pdo->exec('COMMIT');
        }
        $sub = new Subscription($this->path, $this->namespace, $patterns, $start, $gap, $pollInterval,
            function (Subscription $s): void {
                $this->subscriptions = array_values(array_filter($this->subscriptions, static fn ($x) => $x !== $s));
            });
        $this->subscriptions[] = $sub;

        return $sub;
    }

    /**
     * Persists a named subscriber cursor (in the meta table). Save an
     * event's seq after you have finished handling it, then resume with
     * watch(..., fromSeq: $cache->loadCursor($name)). Each save is a write:
     * on a busy stream save every N events, accepting up to N redeliveries.
     */
    public function saveCursor(string $name, int $seq): void
    {
        $this->assertOpen();
        $stmt = $this->prepare('INSERT INTO meta (k, v) VALUES (:k, :v) ON CONFLICT(k) DO UPDATE SET v = excluded.v');
        $this->executeWithRetry(fn () => $stmt->execute([':k' => "cursor:{$this->namespace}:{$name}", ':v' => (string) $seq]));
    }

    /** The cursor last saved under $name, or null. */
    public function loadCursor(string $name): ?int
    {
        $this->assertOpen();
        $stmt = $this->prepare('SELECT v FROM meta WHERE k = :k');
        $stmt->execute([':k' => "cursor:{$this->namespace}:{$name}"]);
        $v = $stmt->fetchColumn();
        $stmt->closeCursor();

        return $v === false ? null : (int) $v;
    }

    /**
     * Retention in bounded batches. Always deletes a prefix of the seq range
     * (everything up to the newest event past retention or beyond the count
     * cap), never a hole, which keeps gap detection sound even if clocks
     * disagree.
     */
    private function sweepEvents(): void
    {
        if (! $this->eventsExist()) {
            return;
        }
        $bounds = [];
        $stmt = $this->run('SELECT max(seq) FROM events WHERE created_at <= :cut', [':cut' => self::nowMillis() - (int) round($this->eventRetention * 1000)]);
        $byAge = $stmt->fetchColumn();
        $stmt->closeCursor();
        if ($byAge !== null && $byAge !== false) {
            $bounds[] = (int) $byAge;
        }
        if ($this->eventMaxCount !== null) {
            $stmt = $this->run('SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET :n', [':n' => $this->eventMaxCount]);
            $byCount = $stmt->fetchColumn();
            $stmt->closeCursor();
            if ($byCount !== false && $byCount !== null) {
                $bounds[] = (int) $byCount;
            }
        }
        if ($bounds === []) {
            return;
        }
        $bound = max($bounds);
        do {
            $n = $this->executeWithRetry(fn () => $this->run('DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= :b ORDER BY seq LIMIT :lim)',
                [':b' => $bound, ':lim' => self::MAINTENANCE_BATCH])->rowCount());
        } while ($n >= self::MAINTENANCE_BATCH);
    }

    // ---------------------------------------------------------------
    // Change listeners (in-process) and auto-publish
    // ---------------------------------------------------------------

    /**
     * Calls $listener(ChangeRecord) after mutations in THIS PROCESS commit.
     * In-process only, no SQLite involvement. PHP has no threads, so
     * listeners run synchronously right after the mutating call's COMMIT --
     * never inside the transaction, never holding the write lock -- which
     * means a rolled-back setMany() notifies nobody and a listener may write
     * to the cache itself. A throwing listener is logged and skipped; the
     * write already committed. Writes made by listeners can trigger
     * listeners again; past 8 levels those records are dropped with a
     * warning. Bulk operations produce ONE record. Returns an unsubscribe
     * closure.
     *
     * "set" covers every write that changes a value (set, setMany, add,
     * replace, getSet, counters, hash writes); "delete" is delete(); "flush"
     * is flush(). TTL expiry and eviction are not reported.
     *
     * @param  callable(ChangeRecord): mixed  $listener
     * @param  list<string>  $events  any of "set", "delete", "flush"
     */
    public function onChange(callable $listener, array $events = self::CHANGE_OPS): \Closure
    {
        $this->assertOpen();
        foreach ($events as $op) {
            if (! in_array($op, self::CHANGE_OPS, true)) {
                throw new \InvalidArgumentException("lytecache: unknown change operation \"{$op}\"");
            }
        }
        if ($events === []) {
            throw new \InvalidArgumentException('lytecache: onChange events must not be empty');
        }
        $id = $this->nextListenerId++;
        $this->listeners[$id] = [$listener(...), $events];

        return function () use ($id): void {
            unset($this->listeners[$id]);
        };
    }

    /**
     * Called after a mutation commits. With no listeners and no
     * publishChanges it returns immediately: the zero-cost common case.
     *
     * @param  'set'|'delete'|'flush'  $op
     * @param  list<string>|null  $keys
     */
    private function changed(string $op, ?array $keys, int $count): void
    {
        $publish = in_array($op, $this->publishChanges, true);
        if ($this->listeners === [] && ! $publish) {
            return;
        }
        $truncated = $keys === null || count($keys) > $this->changeKeyCap;
        $record = new ChangeRecord($op, $this->namespace, $truncated ? null : $keys, $count, $truncated);
        if ($publish) {
            try {
                $this->insertEvents([[self::CACHE_CHANNEL, $record->toArray()]]);
            } catch (\Throwable $e) {
                if ($this->strict) {
                    throw $e;
                }
                ($this->logger)("lytecache: failed to publish {$op} change event: ".$e->getMessage());
            }
        }
        if ($this->listeners === []) {
            return;
        }
        if ($this->dispatchDepth >= self::MAX_DISPATCH_DEPTH) {
            ($this->logger)("lytecache: dropping {$op} change record: onChange listeners re-entered the cache ".self::MAX_DISPATCH_DEPTH.' levels deep');

            return;
        }
        $this->dispatchDepth++;
        try {
            foreach (array_keys($this->listeners) as $id) {
                if (! array_key_exists($id, $this->listeners)) {
                    continue; // unsubscribed meanwhile: never called again
                }
                $entry = $this->listeners[$id];
                if (! in_array($op, $entry[1], true)) {
                    continue;
                }
                try {
                    ($entry[0])($record);
                } catch (\Throwable $e) {
                    ($this->logger)('lytecache: onChange listener threw: '.$e->getMessage());
                }
            }
        } finally {
            $this->dispatchDepth--;
        }
    }

    // ---------------------------------------------------------------
    // Introspection & maintenance
    // ---------------------------------------------------------------

    /**
     * Lazily iterates keys in the current namespace matching $pattern,
     * using SQLite's native GLOB syntax (*, ?, [...]) -- not SQL LIKE's
     * %/_ wildcards -- for consistency with the Python, Java, Node.js,
     * and Go implementations. Cursor-based (keyset pagination in batches
     * of 500), so it never loads every key into memory at once.
     *
     * @return \Generator<int, string>
     */
    public function keys(string $pattern = '*'): \Generator
    {
        $this->assertOpen();
        $lastKey = '';

        while (true) {
            $now = self::nowMillis();
            $stmt = $this->prepare(<<<'SQL'
                SELECT key FROM cache
                WHERE namespace = :ns AND key GLOB :pattern AND key > :lastKey AND (expires_at IS NULL OR expires_at > :now)
                ORDER BY key LIMIT :limit
                SQL);
            $stmt->bindValue(':ns', $this->namespace);
            $stmt->bindValue(':pattern', $pattern);
            $stmt->bindValue(':lastKey', $lastKey);
            $stmt->bindValue(':now', $now);
            $stmt->bindValue(':limit', self::MAINTENANCE_BATCH, \PDO::PARAM_INT);
            $stmt->execute();

            $batch = $stmt->fetchAll(\PDO::FETCH_COLUMN);
            if ($batch === []) {
                return;
            }

            foreach ($batch as $key) {
                yield $key;
            }

            $lastKey = $batch[count($batch) - 1];
            if (count($batch) < self::MAINTENANCE_BATCH) {
                return;
            }
        }
    }

    /**
     * Deletes every key in the current namespace. Takes no key or pattern
     * argument by design -- to clear a subset, delete by key or pattern
     * instead (iterate keys() and call delete()).
     */
    public function flush(): void
    {
        $this->assertOpen();
        $stmt = $this->prepare('DELETE FROM cache WHERE namespace = :ns');
        $stmt->execute([':ns' => $this->namespace]);
        $this->lruBuffer = [];
        if ($stmt->rowCount() > 0) {
            $this->changed('flush', null, $stmt->rowCount());
        }
    }

    public function stats(): CacheStats
    {
        $this->assertOpen();
        $this->flushLruBuffer();

        $stmt = $this->prepare('SELECT COUNT(*) AS c, COALESCE(SUM(size_bytes), 0) AS s FROM cache WHERE namespace = :ns');
        $stmt->execute([':ns' => $this->namespace]);
        /** @var array{c: int, s: int} $row */
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();

        $total = $this->hits + $this->misses;
        $hitRate = $total > 0 ? $this->hits / $total : 0.0;

        return new CacheStats(
            hits: $this->hits,
            misses: $this->misses,
            hitRate: $hitRate,
            keyCount: (int) $row['c'],
            sizeBytes: (int) $row['s'],
            evictions: $this->evictions,
            expiredRemoved: $this->expiredRemoved,
            path: $this->path,
            hashMode: $this->hashMode(),
        );
    }

    /** Reclaims disk space left behind by deleted rows. */
    public function vacuum(): void
    {
        $this->assertOpen();
        $this->pdo->exec('VACUUM');
    }

    /**
     * Flushes any buffered state and closes the underlying database
     * connection. Safe to call more than once. Also called automatically
     * by __destruct().
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        foreach ($this->subscriptions as $sub) {
            $sub->close();
        }
        $this->listeners = [];
        $this->flushLruBuffer();
        $this->stmtCache = [];
        // PDO has no explicit close() method; dropping the last reference
        // to the PDO object is what actually closes the connection.
        unset($this->pdo);
        $this->closed = true;
    }

    public function __destruct()
    {
        if (! $this->closed) {
            $this->close();
        }
    }

    /**
     * Runs one maintenance pass: flush buffered LRU bookkeeping, remove
     * expired rows in bounded batches, then enforce capacity limits. PHP
     * has no background threads, so this -- rather than a sweeper -- is
     * how expired rows actually get removed from disk over time. Called
     * automatically on roughly every 100th operation (see
     * maybeMaintain()); expose it publicly too so a scheduler (e.g.
     * Laravel's, via the lytecache:maintain artisan command) can call it
     * on a fixed cadence regardless of traffic.
     */
    public function maintain(): void
    {
        $this->assertOpen();
        $this->flushLruBuffer();
        $this->removeExpiredBatch();
        $this->enforceCapacity();
        $this->sweepEvents();
        $this->lastMaintenanceAt = microtime(true);
    }

    private function maybeMaintain(): void
    {
        $this->opsSinceMaintenance++;
        if ($this->opsSinceMaintenance < self::MAINTENANCE_EVERY_OPS) {
            return;
        }

        $this->opsSinceMaintenance = 0;

        if ($this->sweepInterval !== null) {
            $elapsed = microtime(true) - $this->lastMaintenanceAt;
            if ($elapsed < $this->sweepInterval) {
                return;
            }
        }

        $this->maintain();
    }

    private function removeExpiredBatch(): void
    {
        $now = self::nowMillis();

        while (true) {
            $stmt = $this->prepare(<<<'SQL'
                DELETE FROM cache WHERE namespace = :ns AND key IN (
                  SELECT key FROM cache WHERE namespace = :ns2 AND expires_at IS NOT NULL AND expires_at <= :now LIMIT :limit
                )
                SQL);
            $stmt->bindValue(':ns', $this->namespace);
            $stmt->bindValue(':ns2', $this->namespace);
            $stmt->bindValue(':now', $now);
            $stmt->bindValue(':limit', self::MAINTENANCE_BATCH, \PDO::PARAM_INT);
            $this->executeWithRetry(fn () => $stmt->execute());

            $n = $stmt->rowCount();
            $this->expiredRemoved += $n;

            if ($n < self::MAINTENANCE_BATCH) {
                return;
            }
        }
    }

    /**
     * Buffers a read's last_accessed/access_count update in memory. It is
     * flushed in batches (every 200 buffered keys, on maintain(), on
     * maybeEvict() for the LRU policy, or on close()) rather than written
     * synchronously on every read.
     */
    private function bufferLru(string $key, int $now): void
    {
        $entry = $this->lruBuffer[$key] ?? ['lastAccessed' => 0, 'accessCount' => 0];
        $entry['lastAccessed'] = $now;
        $entry['accessCount']++;
        $this->lruBuffer[$key] = $entry;

        if (count($this->lruBuffer) >= self::MAINTENANCE_BATCH) {
            $this->flushLruBuffer();
        }
    }

    /**
     * Flushes buffered LRU bookkeeping in one transaction. LRU bookkeeping
     * is inherently best-effort -- losing an update never corrupts data,
     * it only slightly stales eviction order -- so a transient SQLITE_BUSY
     * collision (e.g. another LyteCache instance on the same file
     * flushing or closing at the same moment, most visible when several
     * instances are destructed at process/script shutdown in quick
     * succession) is swallowed rather than thrown. Any other error is
     * still rethrown, since that could indicate a real problem (a full
     * disk, a corrupt database, etc.).
     */
    private function flushLruBuffer(): void
    {
        if ($this->lruBuffer === []) {
            return;
        }

        $pending = $this->lruBuffer;
        $this->lruBuffer = [];

        $stmt = $this->prepare(
            'UPDATE cache SET last_accessed = :now, access_count = access_count + :count WHERE namespace = :ns AND key = :key'
        );

        $this->pdo->beginTransaction();

        try {
            foreach ($pending as $key => $entry) {
                $stmt->execute([
                    ':now' => $entry['lastAccessed'],
                    ':count' => $entry['accessCount'],
                    ':ns' => $this->namespace,
                    ':key' => $key,
                ]);
            }

            $this->pdo->commit();
        } catch (\PDOException $e) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }

            if (! self::isSqliteBusy($e)) {
                throw $e;
            }
        }
    }

    private function maybeEvict(): void
    {
        if ($this->eviction === Eviction::NoEviction) {
            return;
        }

        if ($this->maxKeys === null && $this->maxBytes === null) {
            return;
        }

        if ($this->eviction === Eviction::LRU) {
            // Flush buffered last_accessed updates first so LRU eviction
            // order reflects the access that just happened, not a stale
            // on-disk timestamp from before the read was buffered.
            $this->flushLruBuffer();
        }

        $this->enforceCapacity();
    }

    private function evictionOrderBy(): string
    {
        return match ($this->eviction) {
            // Rows with an expiry sort before rows without one, and
            // within those, the soonest-to-expire sorts first.
            Eviction::TTL => 'expires_at IS NULL, expires_at ASC',
            Eviction::Random => 'RANDOM()',
            default => 'last_accessed ASC',
        };
    }

    private function enforceCapacity(): void
    {
        if ($this->eviction === Eviction::NoEviction) {
            return;
        }

        if ($this->maxKeys === null && $this->maxBytes === null) {
            return;
        }

        $orderBy = $this->evictionOrderBy();
        $deleteSql = <<<SQL
            DELETE FROM cache WHERE namespace = :ns AND key IN (
              SELECT key FROM cache WHERE namespace = :ns2 ORDER BY {$orderBy} LIMIT 1
            )
            SQL;

        for ($pass = 0; $pass < self::MAX_EVICTION_PASSES; $pass++) {
            $stmt = $this->prepare('SELECT COUNT(*) AS c, COALESCE(SUM(size_bytes), 0) AS s FROM cache WHERE namespace = :ns');
            $stmt->execute([':ns' => $this->namespace]);
            /** @var array{c: int, s: int} $row */
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
            $stmt->closeCursor();

            $overKeys = $this->maxKeys !== null && (int) $row['c'] > $this->maxKeys;
            $overBytes = $this->maxBytes !== null && (int) $row['s'] > $this->maxBytes;

            if (! $overKeys && ! $overBytes) {
                return;
            }

            $delete = $this->prepare($deleteSql);
            $this->executeWithRetry(fn () => $delete->execute([':ns' => $this->namespace, ':ns2' => $this->namespace]));

            $n = $delete->rowCount();
            $this->evictions += $n;

            if ($n === 0) {
                return;
            }
        }
    }

    // ---------------------------------------------------------------
    // Extras
    // ---------------------------------------------------------------

    /**
     * Reads $key; on a miss, calls $loader, stores the result with $ttl
     * (seconds), and returns it. On a hit, returns the cached value
     * without calling $loader.
     *
     * @param  callable(): mixed  $loader
     */
    public function remember(string $key, ?float $ttl, callable $loader): mixed
    {
        $this->assertOpen();
        $sentinel = new \stdClass;
        $value = $this->get($key, $sentinel);

        if ($value !== $sentinel) {
            return $value;
        }

        $value = $loader();
        $this->set($key, $value, $ttl);

        return $value;
    }

    /**
     * Acquires a process-safe lock named $name, polling roughly every
     * 50ms until $timeout seconds elapse. Built on the same atomic add()
     * (Redis "SET NX") semantics as everything else, so only one holder
     * -- across PHP-FPM worker processes sharing the cache file -- can
     * hold a given lock name at once.
     *
     * The lock is also given $timeout as its own TTL, as a safety net: if
     * the holder's process dies before calling release(), the lock still
     * expires on its own instead of wedging forever.
     */
    public function lock(string $name, float $timeout = 30.0): CacheLock
    {
        $this->assertOpen();
        $token = bin2hex(random_bytes(16));
        $lockKey = self::LOCK_KEY_PREFIX.$name;
        $deadline = microtime(true) + $timeout;

        while (true) {
            if ($this->addInternal($lockKey, $token, $timeout, false)) {
                return new CacheLock($this, $name, $token);
            }

            if (microtime(true) >= $deadline) {
                throw new LockTimeoutException("lytecache: could not acquire lock \"{$name}\" within {$timeout}s");
            }

            usleep((int) (self::LOCK_POLL_SECONDS * 1_000_000));
        }
    }

    /**
     * Releases a lock only if $token still matches what is stored.
     * Called by {@see CacheLock::release()}; not typically called
     * directly.
     */
    public function releaseLock(string $name, string $token): bool
    {
        $this->assertOpen();
        [$data] = Serializer::encode($token);

        $stmt = $this->prepare('DELETE FROM cache WHERE namespace = :ns AND key = :key AND value = :token');
        $stmt->bindValue(':ns', $this->namespace);
        $stmt->bindValue(':key', self::LOCK_KEY_PREFIX.$name);
        $stmt->bindValue(':token', $data, \PDO::PARAM_LOB);
        $this->executeWithRetry(fn () => $stmt->execute());

        return $stmt->rowCount() > 0;
    }
}
