<?php

declare(strict_types=1);

namespace Lytecache;

use Lytecache\Exceptions\CacheFullException;
use Lytecache\Exceptions\LockTimeoutException;
use Lytecache\Exceptions\NotNumericException;
use Lytecache\Exceptions\SchemaVersionException;
use Lytecache\Exceptions\SerializationException;
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
     */
    public function __construct(
        ?string $path = null,
        private readonly string $namespace = 'default',
        private readonly ?int $maxKeys = null,
        private readonly ?int $maxBytes = null,
        private readonly Eviction $eviction = Eviction::LRU,
        private readonly ?float $sweepInterval = 60.0,
        private readonly bool $strict = false,
    ) {
        $this->path = $path === null ? self::defaultPath() : Paths::expandHome($path);

        $dir = dirname($this->path);
        if ($dir !== '' && $dir !== '.' && ! is_dir($dir) && ! mkdir($dir, 0755, true) && ! is_dir($dir)) {
            throw new \RuntimeException("lytecache: could not create database directory {$dir}");
        }

        $this->pdo = $this->openWithRetry($this->path);
        $this->lastMaintenanceAt = microtime(true);
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

        return $stmt->rowCount();
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

        $this->maybeMaintain();

        return $stmt->rowCount() > 0;
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
            if ($this->add($lockKey, $token, $timeout)) {
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
