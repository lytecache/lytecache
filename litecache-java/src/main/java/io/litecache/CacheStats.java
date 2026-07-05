package io.litecache;

import java.nio.file.Path;

/**
 * Immutable snapshot of cache statistics.
 *
 * @param hits number of successful get operations
 * @param misses number of missed get operations
 * @param keyCount current number of keys in the cache
 * @param sizeBytes approximate size in bytes
 * @param evictions number of entries evicted due to capacity
 * @param expiredRemoved number of entries removed due to expiration
 * @param path the database file path
 */
public record CacheStats(
        long hits,
        long misses,
        long keyCount,
        long sizeBytes,
        long evictions,
        long expiredRemoved,
        Path path) {

    /**
     * Computes the hit rate as a percentage (0.0 to 100.0).
     *
     * @return hit rate percentage, or 0.0 if no operations
     */
    public double hitRate() {
        long total = hits + misses;
        return total == 0 ? 0.0 : (100.0 * hits) / total;
    }
}
