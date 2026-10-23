<?php

declare(strict_types=1);

namespace Lytecache\Tests\Unit;

use Lytecache\Bytes;
use Lytecache\Exceptions\NotNumericException;
use Lytecache\Exceptions\SerializationException;
use Lytecache\LyteCache;
use Lytecache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

/** Hash operations, run once on the JSON1 path and once with the fallback forced. */
final class HashTest extends TestCase
{
    private const WEIRD = ['a.b', 'a[0]', 'a]b', 'q"uote', 'back\\slash', '\\"', 'sp ace', 'ünï 字', '', '$', '*', '1', '2'];

    private string $path;

    /** @return array<string, array{string}> */
    public static function modes(): array
    {
        return ['json1' => ['json1'], 'fallback' => ['fallback']];
    }

    private function open(string $mode, mixed ...$args): LyteCache
    {
        putenv($mode === 'fallback' ? 'LYTECACHE_FORCE_JSON_FALLBACK=1' : 'LYTECACHE_FORCE_JSON_FALLBACK');
        $this->path ??= $this->tempDbPath();
        $c = new LyteCache(...['path' => $this->path, 'sweepInterval' => null, 'logger' => static fn (string $m) => null, ...$args]);
        self::assertSame($mode, $c->hashMode());
        self::assertSame($mode, $c->stats()->hashMode);

        return $c;
    }

    protected function tearDown(): void
    {
        putenv('LYTECACHE_FORCE_JSON_FALLBACK');
        parent::tearDown();
    }

    /** @return array{value: string, vt: int, exp: int|null, size: int, len: int, la: int, ac: int}|null */
    private function raw(string $key): ?array
    {
        $pdo = new \PDO('sqlite:'.$this->path);
        $stmt = $pdo->prepare("SELECT value, value_type AS vt, expires_at AS exp, size_bytes AS size, length(value) AS len, last_accessed AS la, access_count AS ac FROM cache WHERE namespace='default' AND key=?");
        $stmt->execute([$key]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        return $row === false ? null : $row;
    }

    private function insertRaw(string $key, string $blob, int $vt): void
    {
        $pdo = new \PDO('sqlite:'.$this->path);
        $stmt = $pdo->prepare("INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes) VALUES ('default', ?, ?, ?, 1, NULL, 1, 0, ?)");
        $stmt->bindValue(1, $key);
        $stmt->bindValue(2, $blob, \PDO::PARAM_LOB);
        $stmt->bindValue(3, $vt, \PDO::PARAM_INT);
        $stmt->bindValue(4, strlen($blob), \PDO::PARAM_INT);
        $stmt->execute();
    }

    #[DataProvider('modes')]
    public function test_round_trip_and_created_flag(string $mode): void
    {
        $c = $this->open($mode);
        $values = ['s' => 'text', 'i' => 42, 'f' => 1.5, 't' => true, 'n' => null, 'o' => ['x' => [1, 2]], 'l' => [1, 'a']];
        foreach ($values as $f => $v) {
            self::assertTrue($c->hset('h', $f, $v));
        }
        self::assertFalse($c->hset('h', 's', 'again'));
        self::assertSame('again', $c->hget('h', 's'));
        self::assertSame(42, $c->hget('h', 'i'));
        self::assertSame(1.5, $c->hget('h', 'f'));
        self::assertSame(['x' => [1, 2]], $c->hget('h', 'o'));
        self::assertNull($c->hget('h', 'n', 'dflt'));
        self::assertSame('dflt', $c->hget('h', 'missing', 'dflt'));
        self::assertSame(['s' => 'again'] + $values, $c->get('h'));
        $c->set('u', ['name' => 'Ada']);
        $c->hset('u', 'status', 'active');
        self::assertSame(['name' => 'Ada', 'status' => 'active'], $c->get('u'));
    }

    #[DataProvider('modes')]
    public function test_many_mget_del_keys_vals_len(string $mode): void
    {
        $c = $this->open($mode);
        $c->hsetMany('h', ['a' => 1, 'b' => 'two', 'c' => null]);
        self::assertSame(['two', null, 1], $c->hmget('h', ['b', 'nope', 'a']));
        self::assertSame(['a', 'b', 'c'], $c->hkeys('h'));
        self::assertSame([1, 'two', null], $c->hvals('h'));
        self::assertSame(3, $c->hlen('h'));
        self::assertTrue($c->hexists('h', 'c'));
        self::assertSame(1, $c->hdel('h', 'a', 'nope', 'a'));
        self::assertSame(2, $c->hdel('h', 'b', 'c'));
        self::assertTrue($c->has('h'));
        self::assertSame('{}', $this->raw('h')['value']);
        $big = [];
        for ($i = 0; $i < 1234; $i++) {
            $big["f{$i}"] = $i;
        }
        $c->hsetMany('big', $big);
        self::assertSame(1234, $c->hlen('big'));
        self::assertSame(777, $c->hget('big', 'f777'));
        self::assertTrue($c->hsetnx('n', 'a', 1));
        self::assertFalse($c->hsetnx('n', 'a', 2));
        self::assertSame(1, $c->hget('n', 'a'));
    }

    #[DataProvider('modes')]
    public function test_misses_create_nothing_and_expired_keys_stay_dead(string $mode): void
    {
        $c = $this->open($mode);
        self::assertNull($c->hget('missing', 'f'));
        self::assertSame([], $c->hgetAll('missing'));
        self::assertSame(0, $c->hlen('missing'));
        self::assertSame(0, $c->hdel('missing', 'f'));
        self::assertNull($this->raw('missing'));
        $c->hset('plain', 'f', 1);
        self::assertNull($this->raw('plain')['exp']);
        $c->hset('t', 'f', 1, ttl: 100);
        $exp = $this->raw('t')['exp'];
        self::assertNotNull($exp);
        $c->hset('t', 'g', 2, ttl: 5);
        self::assertSame($exp, $this->raw('t')['exp']);
        $c->set('e', ['old' => 1], ttl: 0.01);
        usleep(40_000);
        self::assertNull($c->hget('e', 'old'));
        self::assertTrue($c->hsetnx('e', 'new', 1));
        self::assertSame(['new' => 1], $c->hgetAll('e'));
        self::assertNull($this->raw('e')['exp']);
    }

    #[DataProvider('modes')]
    public function test_type_guards(string $mode): void
    {
        $c = $this->open($mode);
        foreach ([["\x00\x01", 0], ['hello', 1], ['42', 2], ['3.14', 3], ["\x80\x04", 5], ["\xac\xed", 6]] as [$blob, $vt]) {
            $key = "k{$vt}";
            $this->insertRaw($key, $blob, $vt);
            $ops = [
                fn () => $c->hset($key, 'f', 1), fn () => $c->hsetMany($key, ['f' => 1]), fn () => $c->hsetnx($key, 'f', 1),
                fn () => $c->hincrBy($key, 'f'), fn () => $c->hincrByFloat($key, 'f', 1.0), fn () => $c->hdel($key, 'f'),
                fn () => $c->hget($key, 'f'), fn () => $c->hgetAll($key), fn () => $c->hkeys($key), fn () => $c->hvals($key),
                fn () => $c->hlen($key),
            ];
            foreach ($ops as $i => $op) {
                try {
                    $op();
                    self::fail("value_type {$vt} op {$i} did not throw");
                } catch (SerializationException $e) {
                    self::assertStringContainsString("value_type={$vt}", $e->getMessage());
                }
            }
            self::assertSame($blob, $this->raw($key)['value']);
        }
        $c->set('arr', [1, 2]);
        try {
            $c->hset('arr', 'f', 1);
            self::fail('array root accepted');
        } catch (SerializationException $e) {
            self::assertStringContainsString('not a JSON object', $e->getMessage());
        }
        $this->insertRaw('bad', '{not json', 4);
        try {
            $c->hset('bad', 'f', 1);
            self::fail('malformed accepted');
        } catch (SerializationException $e) {
            self::assertStringContainsString('malformed JSON', $e->getMessage());
        }
        $this->expectException(SerializationException::class);
        $c->hset('h', 'f', new Bytes('x'));
    }

    #[DataProvider('modes')]
    public function test_awkward_field_names(string $mode): void
    {
        $c = $this->open($mode);
        foreach (self::WEIRD as $f) {
            $c->set('h', ['a' => ['b' => 'decoy'], 'other' => 0]);
            self::assertTrue($c->hset('h', $f, 'v'), $f);
            self::assertSame('v', $c->hget('h', $f), $f);
            $doc = $c->get('h');
            self::assertIsArray($doc);
            self::assertSame('v', $doc[$f] ?? null, $f);
            self::assertCount(3, $doc, $f);
            self::assertContains($f, $c->hkeys('h'));
            self::assertSame(2, $c->hincrBy('h', "{$f}#n", 2), $f);
            self::assertSame(1, $c->hdel('h', $f), $f);
        }
        $c->set('order', ['z' => 1]);
        foreach (['2', 'a', '1'] as $f) {
            $c->hset('order', $f, $f);
        }
        self::assertSame(['z', '2', 'a', '1'], $c->hkeys('order'));
    }

    #[DataProvider('modes')]
    public function test_ttl_size_and_access(string $mode): void
    {
        $c = $this->open($mode);
        $c->set('h', ['a' => 1], ttl: 60);
        $r0 = $this->raw('h');
        usleep(5_000);
        $ops = [
            fn () => $c->hset('h', 'b', '字字字字'), fn () => $c->hsetMany('h', ['c' => 3, 'd' => [1, 2]]),
            fn () => $c->hsetnx('h', 'e', 4), fn () => $c->hincrBy('h', 'n', 7), fn () => $c->hincrByFloat('h', 'x', 1.25),
            fn () => $c->hdel('h', 'a', 'c'),
        ];
        foreach ($ops as $op) {
            $op();
            $r = $this->raw('h');
            self::assertSame($r0['exp'], $r['exp']);
            self::assertSame($r['len'], $r['size']);
            self::assertSame(strlen($r['value']), $r['size']);
        }
        $r = $this->raw('h');
        self::assertSame($r0['ac'] + count($ops), $r['ac']);
        self::assertGreaterThan($r0['la'], $r['la']);
        $c->hget('h', 'b');
        $c->hgetAll('h');
        self::assertSame($r['ac'], $this->raw('h')['ac']);
    }

    #[DataProvider('modes')]
    public function test_hincr(string $mode): void
    {
        $c = $this->open($mode);
        self::assertSame(1, $c->hincrBy('h', 'n'));
        self::assertSame(5, $c->hincrBy('h', 'n', 4));
        self::assertSame(-5, $c->hincrBy('h', 'n', -10));
        self::assertSame('{"n":-5}', $this->raw('h')['value']);
        foreach (['5', true, null, ['a' => 1], [1], 1.5] as $bad) {
            $c->hset('h', 'bad', $bad);
            try {
                $c->hincrBy('h', 'bad');
                self::fail('non-integer field accepted');
            } catch (NotNumericException) {
            }
        }
        $c->hset('h', 'max', PHP_INT_MAX - 1);
        self::assertSame(PHP_INT_MAX, $c->hincrBy('h', 'max'));
        $before = $this->raw('h')['value'];
        try {
            $c->hincrBy('h', 'max');
            self::fail('overflow accepted');
        } catch (\OverflowException) {
        }
        self::assertSame($before, $this->raw('h')['value']);
        $c->hset('h', 'min', PHP_INT_MIN + 1);
        self::assertSame(PHP_INT_MIN, $c->hincrBy('h', 'min', -1));
        self::assertSame(0.1, $c->hincrByFloat('h', 'x', 0.1));
        self::assertSame(0.1 + 0.2, $c->hincrByFloat('h', 'x', 0.2));
        $c->hset('h', 'i', 2);
        self::assertSame(2.5, $c->hincrByFloat('h', 'i', 0.5));
        $c->hset('h', 's', 'str');
        $this->expectException(NotNumericException::class);
        $c->hincrByFloat('h', 's', 1.0);
    }

    #[DataProvider('modes')]
    public function test_canonical_encoding_and_foreign_formatting(string $mode): void
    {
        $c = $this->open($mode);
        $c->hset('h', 'name', 'Adé');
        $c->hset('h', 'n', 1);
        self::assertSame('{"name":"Adé","n":1}', $this->raw('h')['value']);
        $this->insertRaw('foreign', "{ \"a\" : 1,\n \"b.c\": [true, null], \"u\": \"\\u00e9\", \"big\": 123456789012345678901234567890 }", 4);
        self::assertSame(2, $c->hincrBy('foreign', 'a'));
        self::assertSame('é', $c->hget('foreign', 'u'));
        self::assertStringContainsString('123456789012345678901234567890', $this->raw('foreign')['value']);
    }

    #[DataProvider('modes')]
    public function test_no_lost_updates_across_processes(string $mode): void
    {
        $c = $this->open($mode);
        $c->set('shared', ['other' => 'untouched']);
        $script = __DIR__.'/../fixtures/hash-worker.php';
        $env = getenv();
        $handles = [];
        for ($i = 0; $i < 3; $i++) {
            foreach ([['hset', "f{$i}"], ['hincrby', 'n']] as [$m, $field]) {
                $p = proc_open([PHP_BINARY, $script, $this->path, $m, 'shared', $field, '60'], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, null, $env);
                self::assertIsResource($p);
                $handles[] = [$p, $pipes];
            }
        }
        foreach ($handles as [$p, $pipes]) {
            $err = stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            self::assertSame(0, proc_close($p), (string) $err);
        }
        $all = $c->hgetAll('shared');
        ksort($all); // field order follows the order the processes' writes landed in
        self::assertSame(['f0' => 59, 'f1' => 59, 'f2' => 59, 'n' => 180, 'other' => 'untouched'], $all);
    }
}
