package lytecache

import (
	"database/sql"
	"fmt"
	"time"
)

// sweepBatchSize bounds each expired-row delete pass so a single sweep tick
// can't monopolize the write connection on a namespace with a huge number
// of expired rows.
const sweepBatchSize = 500

// maxEvictionPasses is a defensive cap on enforceCapacity's loop, in case
// of an unexpected inconsistency between the count/size check and the
// delete -- it should never be reached in practice.
const maxEvictionPasses = 1_000_000

// sweepLoop is the background maintenance goroutine started by New when
// WithSweepInterval is nonzero. It exits promptly when sweepStop is
// closed, which Close does before returning.
func (c *Cache) sweepLoop() {
	defer close(c.sweepDone)
	ticker := time.NewTicker(c.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.sweepStop:
			return
		case <-ticker.C:
			c.sweepOnce()
		}
	}
}

// sweepOnce runs one maintenance pass: flush buffered LRU bookkeeping,
// remove expired rows in bounded batches, then enforce capacity limits.
func (c *Cache) sweepOnce() {
	c.flushLRUBuffer()
	c.removeExpiredBatch()
	c.enforceCapacity()
	c.sweepEvents()
}

// MaintainResult reports what one [Cache.Maintain] pass did.
type MaintainResult struct {
	ExpiredRemoved int64
	Evicted        int64
}

// Maintain runs one maintenance pass immediately -- the same work the
// background sweeper does automatically every WithSweepInterval (flushing
// buffered LRU bookkeeping, removing expired rows, and enforcing
// WithMaxKeys/WithMaxBytes via the configured eviction policy) -- and
// reports how many rows it removed for each reason.
//
// This is for callers who disabled the sweeper (WithSweepInterval(0)) and
// want to run a pass on their own schedule (a cron job, the lytecache CLI's
// `maintain` command, etc.); most programs never need to call it directly.
func (c *Cache) Maintain() (MaintainResult, error) {
	expiredBefore := c.expiredRemoved.Load()
	evictedBefore := c.evictions.Load()
	c.sweepOnce()
	return MaintainResult{
		ExpiredRemoved: c.expiredRemoved.Load() - expiredBefore,
		Evicted:        c.evictions.Load() - evictedBefore,
	}, nil
}

func (c *Cache) removeExpiredBatch() {
	now := nowMillis()
	for {
		res, err := c.writeDB.Exec(`
DELETE FROM cache WHERE namespace = ? AND key IN (
  SELECT key FROM cache WHERE namespace = ? AND expires_at IS NOT NULL AND expires_at <= ? LIMIT ?
)`, c.namespace, c.namespace, now, sweepBatchSize)
		if err != nil {
			c.logger.Warn("lytecache: sweep: removing expired keys", "error", err)
			return
		}
		n, err := res.RowsAffected()
		if err != nil {
			return
		}
		c.expiredRemoved.Add(n)
		if n < sweepBatchSize {
			return
		}
	}
}

// flushLRUBuffer writes buffered last_accessed/access_count updates in one
// transaction. Get/GetMany buffer these in memory (see bufferLRU) rather
// than writing synchronously on every read; this is what actually persists
// them, called from the sweeper, from maybeEvict (for LRU ordering), from
// Stats, and from Close.
func (c *Cache) flushLRUBuffer() {
	c.lruMu.Lock()
	if len(c.lruBuffer) == 0 {
		c.lruMu.Unlock()
		return
	}
	pending := c.lruBuffer
	c.lruBuffer = make(map[string]lruUpdate)
	c.lruMu.Unlock()

	tx, err := c.writeDB.Begin()
	if err != nil {
		c.logger.Warn("lytecache: flushing LRU buffer", "error", err)
		return
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	stmt, err := tx.Prepare(`UPDATE cache SET last_accessed = ?, access_count = access_count + ? WHERE namespace = ? AND key = ?`)
	if err != nil {
		c.logger.Warn("lytecache: flushing LRU buffer", "error", err)
		return
	}
	defer func() { _ = stmt.Close() }()

	for key, u := range pending {
		if _, err := stmt.Exec(u.lastAccessed, u.accessCount, c.namespace, key); err != nil {
			c.logger.Warn("lytecache: flushing LRU buffer", "key", key, "error", err)
		}
	}
	if err := tx.Commit(); err != nil {
		c.logger.Warn("lytecache: flushing LRU buffer", "error", err)
	}
}

// bufferLRU records a read's last_accessed/access_count update in memory.
// It is flushed in batches (every lruFlushEvery buffered keys, on sweep, on
// maybeEvict for the LRU policy, or on Close) rather than written
// synchronously on every read.
func (c *Cache) bufferLRU(key string, now int64) {
	c.lruMu.Lock()
	u := c.lruBuffer[key]
	u.lastAccessed = now
	u.accessCount++
	c.lruBuffer[key] = u
	shouldFlush := len(c.lruBuffer) >= lruFlushEvery
	c.lruMu.Unlock()

	if shouldFlush {
		c.flushLRUBuffer()
	}
}

// maybeEvict enforces capacity limits after a write, when an eviction
// policy other than NoEviction is configured.
func (c *Cache) maybeEvict() {
	if c.eviction == NoEviction {
		return
	}
	if c.maxKeys == nil && c.maxBytes == nil {
		return
	}
	if c.eviction == LRU {
		// Flush buffered last_accessed updates first so LRU eviction order
		// reflects the access that just happened, not a stale on-disk
		// timestamp from before the read was buffered.
		c.flushLRUBuffer()
	}
	c.enforceCapacity()
}

// evictionOrderBy returns the ORDER BY clause implementing the configured
// eviction policy: the row sorted first is evicted first.
func (c *Cache) evictionOrderBy() string {
	switch c.eviction {
	case TTLPolicy:
		// Rows with an expiry sort before rows without one (SQLite
		// evaluates "expires_at IS NULL" to 0/1), and within those, the
		// soonest-to-expire sorts first.
		return "expires_at IS NULL, expires_at ASC"
	case Random:
		return "RANDOM()"
	default: // LRU
		return "last_accessed ASC"
	}
}

// enforceCapacity evicts one row at a time (per the configured policy)
// until the namespace is back within WithMaxKeys/WithMaxBytes.
func (c *Cache) enforceCapacity() {
	if c.eviction == NoEviction || (c.maxKeys == nil && c.maxBytes == nil) {
		return
	}

	orderBy := c.evictionOrderBy()
	q := fmt.Sprintf(`
DELETE FROM cache WHERE namespace = ? AND key IN (
  SELECT key FROM cache WHERE namespace = ? ORDER BY %s LIMIT 1
)`, orderBy)

	for pass := 0; pass < maxEvictionPasses; pass++ {
		var count int64
		var size sql.NullInt64
		if err := c.readDB.QueryRow(`SELECT COUNT(*), SUM(size_bytes) FROM cache WHERE namespace = ?`, c.namespace).
			Scan(&count, &size); err != nil {
			c.logger.Warn("lytecache: checking capacity", "error", err)
			return
		}

		overKeys := c.maxKeys != nil && count > *c.maxKeys
		overBytes := c.maxBytes != nil && size.Valid && size.Int64 > *c.maxBytes
		if !overKeys && !overBytes {
			return
		}

		res, err := c.writeDB.Exec(q, c.namespace, c.namespace)
		if err != nil {
			c.logger.Warn("lytecache: evicting", "error", err)
			return
		}
		n, err := res.RowsAffected()
		if err != nil || n == 0 {
			return
		}
		c.evictions.Add(n)
	}
}
