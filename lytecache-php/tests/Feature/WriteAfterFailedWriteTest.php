<?php

declare(strict_types=1);

namespace Lytecache\Tests\Feature;

use Lytecache\LyteCache;
use Lytecache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * Regression test for silently lost writes (fixed in 0.3.1).
 *
 * When a write failed inside its transaction (here: a constraint violation
 * raised by a trigger), PDO left the cached prepared statement un-reset. The
 * next execute() of that statement returned false without throwing -- even
 * in ERRMODE_EXCEPTION -- so every later write through it was silently
 * dropped, on every PHP version. PHP 8.4 widened which writes were affected.
 * (Backported to 0.2.x: hash operations and change listeners arrive in 0.3.0.)
 *
 * Behavioural, not version-gated: after any failed write, the next write
 * through the same instance must be committed. "Committed" is asserted from
 * a separate connection and a freshly opened cache, never through the
 * writer's own connection.
 */
final class WriteAfterFailedWriteTest extends TestCase
{
    private const TRIGGER = "CREATE TRIGGER fail_boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' BEGIN SELECT RAISE(ABORT, 'forced failure'); END";

    /** @return array<string, array{\Closure(LyteCache): mixed}> */
    public static function failingWrites(): array
    {
        return [
            'set' => [static fn (LyteCache $c) => $c->set('boom', 'x')],
            'setMany' => [static fn (LyteCache $c) => $c->setMany(['ok-before' => 1, 'boom' => 2])],
            'add' => [static fn (LyteCache $c) => $c->add('boom', 'x')],
            'incr' => [static fn (LyteCache $c) => $c->incr('boom')],
        ];
    }

    /** @return array<string, array{\Closure(LyteCache): mixed, string, mixed}> */
    public static function followingWrites(): array
    {
        return [
            'set' => [static fn (LyteCache $c) => $c->set('after', 'v'), 'after', 'v'],
            'setMany' => [static fn (LyteCache $c) => $c->setMany(['after' => 'v', 'after2' => 'w']), 'after', 'v'],
            'add' => [static fn (LyteCache $c) => $c->add('after', 'v'), 'after', 'v'],
            'incr' => [static fn (LyteCache $c) => $c->incr('after', 7), 'after', 14], // run twice below
        ];
    }

    /** @return iterable<string, array{\Closure(LyteCache): mixed, \Closure(LyteCache): mixed, string, mixed}> */
    public static function matrix(): iterable
    {
        foreach (self::failingWrites() as $f => [$fail]) {
            foreach (self::followingWrites() as $n => [$next, $key, $expected]) {
                yield "{$f} fails, then {$n}" => [$fail, $next, $key, $expected];
            }
        }
    }

    #[DataProvider('matrix')]
    public function test_a_write_after_a_failed_write_is_committed(\Closure $fail, \Closure $next, string $key, mixed $expected): void
    {
        $path = $this->tempDbPath();
        $cache = new LyteCache(path: $path, sweepInterval: null);
        (new \PDO('sqlite:'.$path))->exec(self::TRIGGER);

        try {
            $fail($cache);
            self::fail('the forced failure did not fire');
        } catch (\PDOException) {
            // expected: the write was rejected and rolled back
        }

        $next($cache);
        $next($cache); // and again: the second use of the statement must work too

        self::assertSame(1, self::countFromAnotherConnection($path, $key), "'{$key}' was not committed");
        self::assertSame($expected, $cache->get($key));
        $cache->close();

        $reopened = new LyteCache(path: $path, sweepInterval: null);
        self::assertSame($expected, $reopened->get($key), "'{$key}' did not survive reopening");
        $reopened->close();
    }

    public function test_the_failed_write_itself_leaves_nothing_behind(): void
    {
        $path = $this->tempDbPath();
        $cache = new LyteCache(path: $path, sweepInterval: null);
        (new \PDO('sqlite:'.$path))->exec(self::TRIGGER);
        try {
            $cache->setMany(['ok-before' => 1, 'boom' => 2]);
        } catch (\PDOException) {
        }

        self::assertSame(0, self::countFromAnotherConnection($path, 'ok-before'), 'a rolled-back batch must not be partially applied');
        self::assertFalse($cache->has('ok-before'));
    }

    private static function countFromAnotherConnection(string $path, string $key): int
    {
        $stmt = (new \PDO('sqlite:'.$path))->prepare('SELECT COUNT(*) FROM cache WHERE key = ?');
        $stmt->execute([$key]);

        return (int) $stmt->fetchColumn();
    }
}
