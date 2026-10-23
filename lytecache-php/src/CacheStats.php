<?php

declare(strict_types=1);

namespace Lytecache;

/**
 * Runtime counters for a {@see LyteCache} instance. Hits, misses, hitRate,
 * and evictions are per-instance (per PHP process/request), not shared
 * cluster-wide, since multiple PHP-FPM workers sharing one file each track
 * their own.
 */
final readonly class CacheStats
{
    public function __construct(
        public int $hits,
        public int $misses,
        public float $hitRate,
        public int $keyCount,
        public int $sizeBytes,
        public int $evictions,
        public int $expiredRemoved,
        public string $path,
        /** "json1" or "fallback" -- see {@see LyteCache::hashMode()}. */
        public string $hashMode = 'json1',
    ) {}

    /**
     * @return array{hits: int, misses: int, hitRate: float, keyCount: int, sizeBytes: int, evictions: int, expiredRemoved: int, path: string, hashMode: string}
     */
    public function toArray(): array
    {
        return [
            'hits' => $this->hits,
            'misses' => $this->misses,
            'hitRate' => $this->hitRate,
            'keyCount' => $this->keyCount,
            'sizeBytes' => $this->sizeBytes,
            'evictions' => $this->evictions,
            'expiredRemoved' => $this->expiredRemoved,
            'path' => $this->path,
            'hashMode' => $this->hashMode,
        ];
    }
}
