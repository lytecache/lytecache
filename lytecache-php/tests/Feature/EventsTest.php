<?php

declare(strict_types=1);

namespace Lytecache\Tests\Feature;

use Lytecache\ChangeRecord;
use Lytecache\Event;
use Lytecache\Exceptions\GapException;
use Lytecache\Exceptions\SerializationException;
use Lytecache\LyteCache;
use Lytecache\Subscription;
use Lytecache\Tests\TestCase;

/** notify / watch (local, polled, durable events) and onChange / publishChanges. */
final class EventsTest extends TestCase
{
    private string $path;

    private function open(mixed ...$args): LyteCache
    {
        $this->path ??= $this->tempDbPath();

        return new LyteCache(...['path' => $this->path, 'sweepInterval' => null, ...$args]);
    }

    private static function next(Subscription $s): Event
    {
        $e = $s->poll(5.0);
        self::assertNotNull($e, 'expected an event');

        return $e;
    }

    private function eventsTableExists(): bool
    {
        $pdo = new \PDO('sqlite:'.$this->path);

        return $pdo->query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'")?->fetchColumn() !== false;
    }

    public function test_basics_from_seq_and_ordering(): void
    {
        $c = $this->open();
        $c->notify('orders', 'before');
        $s = $c->watch('orders');
        self::assertNull($s->poll(0.15));
        $s1 = $c->notify('orders', ['id' => 1]);
        $s2 = $c->notify('orders');
        $e1 = self::next($s);
        $e2 = self::next($s);
        self::assertSame([$s1, 'orders', 'default', ['id' => 1]], [$e1->seq, $e1->channel, $e1->namespace, $e1->payload()]);
        self::assertNull($e2->payload());
        self::assertGreaterThan(0, $e1->createdAt);
        self::assertSame($s2, $s->lastSeq());

        $seqs = [];
        for ($i = 0; $i < 5; $i++) {
            $seqs[] = $c->notify('c', $i);
        }
        $all = $c->watch('c', fromSeq: 0);
        foreach ($seqs as $seq) {
            self::assertSame($seq, self::next($all)->seq);
        }
        $after = $c->watch('c', fromSeq: $seqs[1]);
        foreach ([2, 3, 4] as $want) {
            self::assertSame($want, self::next($after)->payload());
        }
        self::assertNull($after->poll(0.15));

        $ab = $c->watch(['a', 'b']);
        $many = $c->notifyMany([['a', 1], ['b', 2], ['a', 3]]);
        foreach ($many as $seq) {
            self::assertSame($seq, self::next($ab)->seq);
        }
        self::assertSame([], $c->notifyMany([]));
    }

    public function test_iteration_try_next_and_close(): void
    {
        $c = $this->open();
        $s = $c->watch('c');
        self::assertNull($s->tryNext());
        $c->notify('c', 'x');
        $c->notify('c', 'y');
        self::assertSame('x', $s->tryNext()?->payload());
        $got = [];
        foreach ($s as $event) {
            $got[] = $event->payload();
            $s->close();
        }
        self::assertSame(['y'], $got);
        $s->close();
        self::assertTrue($s->isClosed());
        self::assertNull($s->poll(0.05));
    }

    public function test_channel_matching_validation_and_namespaces(): void
    {
        $c = $this->open();
        $s = $c->watch(['cache:invalidate:*', 'orders']);
        $mid = $c->watch('a*b');
        $star = $c->watch('*');
        foreach ([['cache:invalidate:user', 1], ['cache:invalidate:', 2], ['cache:invalidatex', 3], ['orders', 4], ['orders:new', 5], ['axb', 6]] as [$ch, $p]) {
            $c->notify($ch, $p);
        }
        foreach ([1, 2, 4] as $want) {
            self::assertSame($want, self::next($s)->payload());
        }
        self::assertNull($s->poll(0.2));
        self::assertNull($mid->poll(0.05));
        for ($i = 1; $i <= 6; $i++) {
            self::assertSame($i, self::next($star)->payload());
        }
        foreach (['', 'x*', '__cache__', '__x'] as $bad) {
            try {
                $c->notify($bad, 1);
                self::fail("accepted {$bad}");
            } catch (\InvalidArgumentException) {
            }
        }
        $other = $this->open(namespace: 'other');
        $ns = $c->watch('n');
        $other->notify('n', 1);
        $c->notify('n', 2);
        self::assertSame(2, self::next($ns)->payload());
        self::assertNull($ns->poll(0.1));
    }

    public function test_payload_types_and_spec_fixture_rows(): void
    {
        $c = $this->open();
        $s = $c->watch('t');
        $values = ['text', 42, -7, 3.25, ['a' => [1, null]], [1, 'x'], true, null];
        foreach ($values as $v) {
            $c->notify('t', $v);
        }
        foreach ($values as $v) {
            self::assertSame($v, self::next($s)->payload());
        }
        $pdo = new \PDO('sqlite:'.$this->path);
        $pdo->exec("INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES
            ('fx','default',CAST('bytes' AS BLOB),0,1),('fx','default',CAST('42' AS BLOB),2,1),
            ('fx','default',CAST('{\"k\":[1,2]}' AS BLOB),4,1),('fx','default',NULL,4,1),('fx','default',x'aced',6,1)");
        self::assertSame('{"a":[1,null]}', $pdo->query("SELECT payload FROM events WHERE channel='t' AND value_type=4 ORDER BY seq LIMIT 1")?->fetchColumn());
        $fx = $c->watch('fx', fromSeq: 0);
        self::assertSame('bytes', self::next($fx)->payload());
        self::assertSame(42, self::next($fx)->payload());
        self::assertSame(['k' => [1, 2]], self::next($fx)->payload());
        self::assertNull(self::next($fx)->payload());
        $java = self::next($fx);
        $this->expectException(SerializationException::class);
        $java->payload();
    }

    public function test_cursors_and_gaps(): void
    {
        $c = $this->open(eventRetention: 0.05);
        self::assertNull($c->loadCursor('w'));
        $c->saveCursor('w', 41);
        $c->saveCursor('w', 42);
        self::assertSame(42, $c->loadCursor('w'));
        $old = [];
        for ($i = 0; $i < 10; $i++) {
            $old[] = $c->notify('c', $i);
        }
        usleep(100_000);
        $fresh = $c->notify('c', 99);
        $c->maintain();
        $s = $c->watch('c', fromSeq: $old[3]);
        try {
            $s->poll(1.0);
            self::fail('no gap');
        } catch (GapException $e) {
            self::assertSame([$old[3], $fresh, $fresh - 1], [$e->requestedSeq, $e->oldestSeq, $e->resumeSeq]);
        }
        self::assertTrue($s->isClosed());
        self::assertSame(99, self::next($c->watch('c', fromSeq: $fresh - 1))->payload());
        self::assertSame($fresh, self::next($c->watch('c', fromSeq: 0))->seq);
        try {
            $c->watch('c', fromSeq: $fresh + 100)->poll(1.0);
            self::fail('no ahead gap');
        } catch (GapException $e) {
            self::assertTrue($e->ahead);
        }

        // mid-stream: a subscriber that falls behind retention
        $sub = $this->open();
        $pub = $this->open(eventMaxCount: 5);
        $w = $sub->watch('m');
        self::assertNull($w->tryNext());
        $seqs = [];
        for ($i = 0; $i < 20; $i++) {
            $seqs[] = $pub->notify('m', $i);
        }
        $pub->maintain();
        $this->expectException(GapException::class);
        $w->poll(1.0);
    }

    public function test_opt_in_cost_and_schema_markers(): void
    {
        $c = $this->open();
        for ($i = 0; $i < 20; $i++) {
            $c->set("k{$i}", $i);
        }
        $c->delete('k1');
        $c->flush();
        $c->maintain();
        self::assertFalse($this->eventsTableExists());
        $c->notify('c', 1);
        self::assertTrue($this->eventsTableExists());
        $meta = [];
        foreach ((new \PDO('sqlite:'.$this->path))->query('SELECT k, v FROM meta') ?: [] as $row) {
            $meta[$row['k']] = $row['v'];
        }
        self::assertSame('1', $meta['schema_version']);
        self::assertSame('1', $meta['events_version']);
        $s = $c->watch('c');
        $c->close();
        self::assertTrue($s->isClosed());
    }

    public function test_cross_process_delivery_and_resume(): void
    {
        $c = $this->open();
        $c->watch('x')->close();
        $dir = dirname($this->path);
        $script = __DIR__.'/../fixtures/events-worker.php';
        $n = 100;
        $subs = [];
        for ($i = 0; $i < 2; $i++) {
            $p = proc_open([PHP_BINARY, $script, $this->path, 'subscribe', 'orders*', (string) $n, "{$dir}/out{$i}", "{$dir}/ready{$i}"],
                [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
            self::assertIsResource($p);
            $subs[] = [$p, $pipes];
            $this->waitFor(fn () => file_exists("{$dir}/ready{$i}"));
        }
        $pub = proc_open([PHP_BINARY, $script, $this->path, 'publish', (string) $n], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pp);
        self::assertIsResource($pub);
        self::assertSame(0, proc_close($pub));
        foreach ($subs as $i => [$p, $pipes]) {
            $err = stream_get_contents($pipes[2]);
            self::assertSame(0, proc_close($p), (string) $err);
            $lines = explode("\n", (string) file_get_contents("{$dir}/out{$i}"));
            self::assertCount($n, $lines);
            $prev = 0;
            foreach ($lines as $j => $line) {
                [$seq, $channel, $payload] = explode(' ', $line);
                self::assertSame('orders:new', $channel);
                self::assertSame((string) $j, $payload);
                self::assertGreaterThan($prev, (int) $seq);
                $prev = (int) $seq;
            }
        }

        // resume after kill
        $p = proc_open([PHP_BINARY, $script, $this->path, 'cursorsub', "{$dir}/readyc"], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        self::assertIsResource($p);
        try {
            $this->waitFor(fn () => file_exists("{$dir}/readyc"));
            $first = $c->notify('jobs', 'probe');
            $this->waitFor(fn () => $c->loadCursor('worker') === $first);
            $seqs = [];
            for ($i = 0; $i < 40; $i++) {
                $seqs[] = $c->notify('jobs', $i);
            }
            $this->waitFor(fn () => ($c->loadCursor('worker') ?? 0) >= $seqs[10]);
        } finally {
            proc_terminate($p, 9);
            proc_close($p);
        }
        $saved = (int) $c->loadCursor('worker');
        for ($i = 40; $i < 60; $i++) {
            $seqs[] = $c->notify('jobs', $i);
        }
        $s = $c->watch('jobs', fromSeq: $saved);
        foreach (array_filter($seqs, fn ($x) => $x > $saved) as $want) {
            self::assertSame($want, self::next($s)->seq);
        }
        self::assertNull($s->poll(0.2));
    }

    private function waitFor(callable $cond): void
    {
        $deadline = microtime(true) + 20;
        while (! $cond()) {
            self::assertLessThan($deadline, microtime(true), 'timed out');
            usleep(10_000);
        }
    }

    // -- onChange / publishChanges --------------------------------------------

    public function test_bulk_one_record_keys_and_no_ops(): void
    {
        $c = $this->open();
        $got = [];
        $c->onChange(function (ChangeRecord $r) use (&$got): void {
            $got[] = $r->toArray();
        });
        $big = [];
        for ($i = 0; $i < 2000; $i++) {
            $big["k{$i}"] = $i;
        }
        $c->setMany($big);
        $c->setMany(['a' => 1, 'b' => 2]);
        $c->delete('a', 'b', 'a');
        $c->delete('missing');
        $c->flush();
        self::assertSame([
            ['op' => 'set', 'namespace' => 'default', 'keys' => null, 'count' => 2000, 'truncated' => true],
            ['op' => 'set', 'namespace' => 'default', 'keys' => ['a', 'b'], 'count' => 2, 'truncated' => false],
            ['op' => 'delete', 'namespace' => 'default', 'keys' => ['a', 'b'], 'count' => 2, 'truncated' => false],
            ['op' => 'flush', 'namespace' => 'default', 'keys' => null, 'count' => 2000, 'truncated' => true],
        ], $got);

        $keys = [];
        $c->onChange(function (ChangeRecord $r) use (&$keys): void {
            $keys[] = $r->keys[0] ?? '?';
        }, ['set']);
        $c->set('a', 1);
        $c->add('b', 1);
        $c->add('b', 2);
        $c->replace('a', 2);
        $c->replace('missing', 2);
        $c->getSet('a', 3);
        $c->incr('n');
        $c->hset('h', 'f', 1);
        $c->hsetnx('h', 'f', 2);
        $c->hincrBy('h', 'n');
        $c->hdel('h', 'nope');
        $c->delete('a');
        $c->lock('job')->release();
        self::assertSame(['a', 'b', 'a', 'a', 'n', 'h', 'h'], $keys);
    }

    public function test_rollback_throwing_listener_reentrancy_and_depth_guard(): void
    {
        $logs = [];
        $c = $this->open(logger: function (string $m) use (&$logs): void {
            $logs[] = $m;
        });
        (new \PDO('sqlite:'.$this->path))->exec("CREATE TRIGGER boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END");
        $got = [];
        $c->onChange(function (): void {
            throw new \RuntimeException('listener bug');
        });
        $c->onChange(function (ChangeRecord $r) use (&$got): void {
            $got[] = $r;
        });
        try {
            $c->setMany(['a' => 1, 'boom' => 2]);
            self::fail('trigger did not abort');
        } catch (\PDOException) {
        }
        self::assertSame([], $got);
        self::assertFalse($c->has('a'));
        $c->set('ok', 1);
        self::assertCount(1, $got);
        self::assertNotEmpty(array_filter($logs, fn ($m) => str_contains($m, 'listener bug')));

        $c->onChange(function (ChangeRecord $r) use ($c): void {
            if ($r->keys === ['src']) {
                $c->set('mirror', $c->get('src'));
            }
        }, ['set']);
        $c->set('src', 42);
        self::assertSame(42, $c->get('mirror'));

        $d = new LyteCache(path: $this->tempDbPath(), sweepInterval: null, logger: function (string $m) use (&$logs): void {
            $logs[] = $m;
        });
        $calls = 0;
        $d->onChange(function () use ($d, &$calls): void {
            $calls++;
            $d->incr('depth');
        }, ['set']);
        $d->set('start', 1);
        self::assertSame(8, $calls);
        self::assertSame(8, $d->get('depth'));
        self::assertNotEmpty(array_filter($logs, fn ($m) => str_contains($m, 'levels deep')));

        $e = $this->open();
        $n = 0;
        $unsubscribe = $e->onChange(function () use (&$n): void {
            $n++;
        });
        $e->set('x', 1);
        $unsubscribe();
        $unsubscribe();
        $e->set('y', 1);
        self::assertSame(1, $n);
    }

    public function test_publish_changes(): void
    {
        $plain = $this->open();
        $plain->set('a', 1);
        $plain->delete('a');
        self::assertFalse($this->eventsTableExists());
        $c = $this->open(publishChanges: ['delete', 'flush']);
        $s = $c->watch('__cache__');
        $c->set('a', 1);
        $big = [];
        for ($i = 0; $i < 150; $i++) {
            $big["k{$i}"] = $i;
        }
        $c->setMany($big);
        $c->delete('a', 'missing');
        $c->flush();
        self::assertSame(['op' => 'delete', 'namespace' => 'default', 'keys' => ['a', 'missing'], 'count' => 1, 'truncated' => false], self::next($s)->payload());
        self::assertSame(['op' => 'flush', 'namespace' => 'default', 'keys' => null, 'count' => 150, 'truncated' => true], self::next($s)->payload());
        self::assertNull($s->poll(0.2));
        $this->expectException(\InvalidArgumentException::class);
        new LyteCache(path: $this->path, publishChanges: ['expire']);
    }

    public function test_invalidation_recipe_runs_as_written(): void
    {
        $cache = $this->open();
        $workerB = $this->open();
        $localMemo = ['balance:42' => 100, 'balance:7' => 5];

        $sub = $workerB->watch(['invalidate']);

        // worker A, after committing a DB change
        $cache->delete('balance:42');
        $cache->notify('invalidate', ['key' => 'balance:42']);

        // workers B..N
        foreach ($sub as $event) {
            unset($localMemo[$event->payload()['key']]);
            $sub->close();
        }
        self::assertSame(['balance:7' => 5], $localMemo);
    }
}
