<?php

declare(strict_types=1);

namespace Lytecache\Tests\Unit;

use Lytecache\Tests\TestCase;

final class TtlTest extends TestCase
{
    public function test_no_expiry_by_default(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v');
        self::assertSame(-1, $cache->ttl('k'));
    }

    public function test_ttl_of_missing_key(): void
    {
        $cache = $this->newCache();
        self::assertNull($cache->ttl('missing'));
    }

    public function test_zero_ttl_expires_immediately(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.0);
        self::assertFalse($cache->has('k'));
    }

    public function test_negative_ttl_expires_immediately(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: -1.0);
        self::assertFalse($cache->has('k'));
    }

    public function test_fractional_ttl(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.05);
        self::assertTrue($cache->has('k'));
        usleep(150_000);
        self::assertFalse($cache->has('k'));
    }

    public function test_ttl_boundary(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.3);
        usleep(50_000);
        self::assertTrue($cache->has('k'));
        usleep(350_000);
        self::assertFalse($cache->has('k'));
    }

    public function test_lazy_expiration_on_get(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.01);
        usleep(50_000);
        self::assertNull($cache->get('k'));
    }

    public function test_expire_overwrites_ttl(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 60.0);
        self::assertTrue($cache->expire('k', 0.05));
        usleep(150_000);
        self::assertFalse($cache->has('k'));
    }

    public function test_expire_on_missing_key_returns_false(): void
    {
        $cache = $this->newCache();
        self::assertFalse($cache->expire('missing', 60.0));
    }

    public function test_persist_removes_ttl(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.05);
        self::assertTrue($cache->persist('k'));
        usleep(150_000);
        self::assertTrue($cache->has('k'));
        self::assertSame(-1, $cache->ttl('k'));
    }

    public function test_touch_refreshes_ttl(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.2);
        usleep(50_000);
        self::assertTrue($cache->touch('k', 0.3));
        usleep(250_000); // past the original 200ms TTL, within the refreshed 300ms
        self::assertTrue($cache->has('k'));
    }

    public function test_maintain_removes_expired_keys(): void
    {
        $cache = $this->newCache();
        $cache->set('k', 'v', ttl: 0.01);
        usleep(50_000);
        $cache->maintain();

        $stats = $cache->stats();
        self::assertSame(0, $stats->keyCount);
        self::assertGreaterThan(0, $stats->expiredRemoved);
    }
}
