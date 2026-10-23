package lytecache

import (
	"fmt"
	"iter"
)

// keysBatchSize bounds each page fetched by Keys, so it never loads every
// matching key into memory at once.
const keysBatchSize = 500

// Keys lazily iterates keys in the current namespace matching pattern,
// using SQLite's native GLOB syntax (*, ?, [...]) rather than SQL LIKE's
// %/_ wildcards -- this matches the pattern syntax used by the Python,
// Java, and Node.js implementations. An empty pattern matches everything.
//
// Iteration is cursor-based (keyset pagination in batches of 500), so it
// is safe to use over an arbitrarily large namespace. Range over the
// returned [iter.Seq2]; a non-nil error stops the iteration after that
// pair is yielded.
//
// A row that has expired but has not yet been swept is excluded from the
// results, but -- unlike Get -- Keys does not proactively delete it; that
// happens on its own via the background sweeper or the next read that
// touches it.
func (c *Cache) Keys(pattern string) iter.Seq2[string, error] {
	if pattern == "" {
		pattern = "*"
	}
	return func(yield func(string, error) bool) {
		lastKey := ""
		for {
			batch, err := c.keysPage(pattern, lastKey)
			if err != nil {
				yield("", err)
				return
			}
			if len(batch) == 0 {
				return
			}
			for _, k := range batch {
				if !yield(k, nil) {
					return
				}
			}
			lastKey = batch[len(batch)-1]
			if len(batch) < keysBatchSize {
				return
			}
		}
	}
}

// keysPage fetches one page of keys greater than lastKey, in its own
// function so the page's *sql.Rows can be closed with a straightforward
// defer instead of manual closes at every return point.
func (c *Cache) keysPage(pattern, lastKey string) ([]string, error) {
	now := nowMillis()
	rows, err := c.readDB.Query(`
SELECT key FROM cache
WHERE namespace = ? AND key GLOB ? AND key > ? AND (expires_at IS NULL OR expires_at > ?)
ORDER BY key LIMIT ?`, c.namespace, pattern, lastKey, now, keysBatchSize)
	if err != nil {
		return nil, fmt.Errorf("lytecache: keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batch []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("lytecache: keys: %w", err)
		}
		batch = append(batch, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: keys: %w", err)
	}
	return batch, nil
}

// Flush deletes every key in the current namespace. It takes no key or
// pattern argument by design -- to clear a subset, delete by key or by
// pattern instead (iterate [Cache.Keys] and call [Cache.Delete]).
func (c *Cache) Flush() error {
	res, err := c.writeDB.Exec(`DELETE FROM cache WHERE namespace = ?`, c.namespace)
	if err != nil {
		return fmt.Errorf("lytecache: flush: %w", err)
	}
	c.lruMu.Lock()
	c.lruBuffer = make(map[string]lruUpdate)
	c.lruMu.Unlock()
	if n := rowsAffected(res); n > 0 {
		c.changed(opFlush, nil, int(n))
	}
	return nil
}
