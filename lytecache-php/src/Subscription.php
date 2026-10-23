<?php

declare(strict_types=1);

namespace Lytecache;

use Lytecache\Exceptions\GapException;

/**
 * A polled view of the events table for some channels, returned by
 * {@see LyteCache::watch()}.
 *
 * SINGLE HOST ONLY, POLLED, DURABLE -- this is not Redis Pub/Sub. PHP has
 * no background threads, so polling happens inside the consuming call:
 * iterate it (`foreach ($sub as $event)`, which blocks), or use poll() /
 * tryNext(). It reads on its own read-only connection and never takes the
 * write lock. Polls run every $pollInterval while events flow and back off
 * to ~1 s while idle. Delivery is at-least-once from your stored cursor, in
 * seq order -- handlers must be idempotent. Lost events throw
 * {@see GapException}.
 *
 * @implements \IteratorAggregate<int, Event>
 */
final class Subscription implements \IteratorAggregate
{
    private const BATCH = 500;

    private const MAX_IDLE_SECONDS = 1.0;

    private ?\PDO $pdo;

    /** @var list<Event> */
    private array $buffer = [];

    private int $cursor;

    private int $lastSeq;

    private float $interval;

    private bool $closed = false;

    private string $filterSql;

    /** @var array<string, int|string> */
    private array $filterParams = [];

    /**
     * @param  list<string>  $patterns
     * @param  \Closure(self): void  $onClose
     *
     * @internal Use {@see LyteCache::watch()}.
     */
    public function __construct(
        string $path,
        private readonly string $namespace,
        public readonly array $patterns,
        int $startSeq,
        private ?GapException $gap,
        private readonly float $pollInterval,
        private readonly \Closure $onClose,
    ) {
        $this->cursor = $startSeq;
        $this->lastSeq = $startSeq;
        $this->interval = $pollInterval;
        $this->filterSql = self::channelFilter($patterns, $this->filterParams);
        if ($gap === null) {
            $this->pdo = new \PDO('sqlite:'.$path);
            $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            $this->pdo->exec('PRAGMA busy_timeout = 5000');
            $this->pdo->exec('PRAGMA query_only = 1'); // a subscription never writes
        } else {
            $this->pdo = null;
        }
    }

    /**
     * The matching rule: an exact name, or a trailing '*' prefix glob; '*'
     * alone matches everything; '*' anywhere else is literal.
     *
     * @param  list<string>  $patterns
     * @param  array<string, int|string>  $params
     */
    public static function channelFilter(array $patterns, array &$params): string
    {
        $clauses = [];
        foreach ($patterns as $i => $p) {
            if ($p === '') {
                throw new \InvalidArgumentException('lytecache: watch patterns must be non-empty');
            }
            if (str_ends_with($p, '*')) {
                $prefix = substr($p, 0, -1);
                if ($prefix === '') {
                    return '1';
                }
                $clauses[] = "substr(channel, 1, :n{$i}) = :c{$i}";
                $params[":n{$i}"] = mb_strlen($prefix, 'UTF-8');
                $params[":c{$i}"] = $prefix;
            } else {
                $clauses[] = "channel = :c{$i}";
                $params[":c{$i}"] = $p;
            }
        }

        return '('.implode(' OR ', $clauses).')';
    }

    /** The seq of the last event handed to you: the value to persist as a cursor. */
    public function lastSeq(): int
    {
        return $this->lastSeq;
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * Returns the next event, polling for up to $timeout seconds (forever if
     * null). Returns null on timeout or once closed.
     *
     * @throws GapException if events after the cursor were deleted by retention
     */
    public function poll(?float $timeout = null): ?Event
    {
        $deadline = $timeout === null ? INF : microtime(true) + $timeout;
        while (true) {
            $event = $this->tryNext();
            if ($event !== null || $this->closed) {
                return $event;
            }
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return null;
            }
            usleep((int) (min($this->interval, $remaining) * 1_000_000));
            $this->interval = min($this->interval * 2, max(self::MAX_IDLE_SECONDS, $this->pollInterval));
        }
    }

    /**
     * Returns an already-available event without sleeping: one read of the
     * table if nothing is buffered. Null if there is none.
     *
     * @throws GapException if events after the cursor were deleted by retention
     */
    public function tryNext(): ?Event
    {
        if ($this->buffer === [] && ! $this->closed) {
            $this->fetch();
        }
        $event = array_shift($this->buffer);
        if ($event !== null) {
            $this->lastSeq = $event->seq;
            $this->interval = $this->pollInterval; // events are flowing: snap back
        }

        return $event;
    }

    /**
     * Blocking iteration; ends when the subscription is closed (for example
     * from inside the loop).
     *
     * @return \Generator<int, Event>
     *
     * @throws GapException if events after the cursor were deleted by retention
     */
    public function getIterator(): \Generator
    {
        while (! $this->closed) {
            $event = $this->poll(1.0);
            if ($event !== null) {
                yield $event->seq => $event;
            }
        }
    }

    /** Wakes the next poll (an in-process notify): skip the remaining backoff. */
    public function wake(): void
    {
        $this->interval = $this->pollInterval;
    }

    /** Releases the read connection. Idempotent. */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->pdo = null;
        ($this->onClose)($this);
    }

    public function __destruct()
    {
        $this->close();
    }

    private function fetch(): void
    {
        if ($this->gap !== null) {
            $gap = $this->gap;
            $this->close();
            throw $gap;
        }
        $pdo = $this->pdo;
        if ($pdo === null) {
            return;
        }
        // One read transaction, so the gap check and the fetch see the same
        // snapshot: the sweeper cannot delete events between the two.
        $pdo->exec('BEGIN');
        try {
            $gap = self::gapCheck($pdo, $this->cursor);
            if ($gap === null) {
                $stmt = $pdo->prepare('SELECT seq, channel, namespace, payload, value_type, created_at FROM events '
                    .'WHERE seq > :cursor AND namespace = :ns AND '.$this->filterSql.' ORDER BY seq LIMIT '.self::BATCH);
                $stmt->bindValue(':cursor', $this->cursor, \PDO::PARAM_INT);
                $stmt->bindValue(':ns', $this->namespace);
                foreach ($this->filterParams as $k => $v) {
                    $stmt->bindValue($k, $v, is_int($v) ? \PDO::PARAM_INT : \PDO::PARAM_STR);
                }
                $stmt->execute();
                /** @var array{seq: int, channel: string, namespace: string, payload: string|null, value_type: int, created_at: int} $row */
                foreach ($stmt->fetchAll(\PDO::FETCH_ASSOC) as $row) {
                    $this->buffer[] = new Event((int) $row['seq'], $row['channel'], $row['namespace'], (int) $row['created_at'],
                        $row['payload'], (int) $row['value_type']);
                    $this->cursor = (int) $row['seq'];
                }
            }
        } finally {
            $pdo->exec('COMMIT');
        }
        if ($gap !== null) {
            $this->close();
            throw $gap;
        }
    }

    public static function lastAssignedSeq(\PDO $pdo): int
    {
        $v = LyteCache::query($pdo, "SELECT seq FROM sqlite_sequence WHERE name = 'events'")->fetchColumn();

        return $v === false || $v === null ? 0 : (int) $v;
    }

    public static function oldestSeq(\PDO $pdo): ?int
    {
        $v = LyteCache::query($pdo, 'SELECT min(seq) FROM events')->fetchColumn();

        return $v === false || $v === null ? null : (int) $v;
    }

    /**
     * SPEC.md's gap rule: a cursor older than the oldest retained event
     * minus one has lost events; one ahead of the last seq ever assigned
     * means the file was recreated.
     */
    public static function gapCheck(\PDO $pdo, int $cursor): ?GapException
    {
        $last = self::lastAssignedSeq($pdo);
        if ($cursor > $last) {
            return new GapException($cursor, $last + 1, true);
        }
        $oldest = self::oldestSeq($pdo) ?? $last + 1;

        return $cursor < $oldest - 1 ? new GapException($cursor, $oldest) : null;
    }
}
