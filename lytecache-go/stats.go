package lytecache

import (
	"database/sql"
	"fmt"
)

// Stats reports runtime counters for a [Cache] instance. Hits, Misses,
// HitRate, and Evictions are per-process (this Cache instance only), not
// shared cluster-wide, since multiple processes sharing one file each
// track their own.
type Stats struct {
	Hits           int64
	Misses         int64
	HitRate        float64
	KeyCount       int64
	SizeBytes      int64
	Evictions      int64
	ExpiredRemoved int64
	// ExpiredPresent is the number of keys in this namespace past their
	// expires_at but not yet swept -- a healthy cache keeps this at or near
	// zero; a growing value indicates the sweeper isn't keeping up.
	ExpiredPresent int64
	Path           string
}

// Stats returns current statistics for the cache's namespace.
func (c *Cache) Stats() (Stats, error) {
	c.flushLRUBuffer()

	var keyCount, expiredPresent int64
	var sizeBytes sql.NullInt64
	err := c.readDB.QueryRow(`
SELECT COUNT(*), SUM(size_bytes),
       COALESCE(SUM(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 ELSE 0 END), 0)
FROM cache WHERE namespace = ?`, nowMillis(), c.namespace).
		Scan(&keyCount, &sizeBytes, &expiredPresent)
	if err != nil {
		return Stats{}, fmt.Errorf("lytecache: stats: %w", err)
	}

	hits := c.hits.Load()
	misses := c.misses.Load()
	var hitRate float64
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return Stats{
		Hits:           hits,
		Misses:         misses,
		HitRate:        hitRate,
		KeyCount:       keyCount,
		SizeBytes:      sizeBytes.Int64,
		Evictions:      c.evictions.Load(),
		ExpiredRemoved: c.expiredRemoved.Load(),
		ExpiredPresent: expiredPresent,
		Path:           c.path,
	}, nil
}

// Vacuum reclaims disk space left behind by deleted rows.
func (c *Cache) Vacuum() error {
	if _, err := c.writeDB.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("lytecache: vacuum: %w", err)
	}
	return nil
}
