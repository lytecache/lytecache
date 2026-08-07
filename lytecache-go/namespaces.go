package lytecache

import "fmt"

// NamespaceInfo summarizes one namespace present in a database file.
type NamespaceInfo struct {
	Namespace string
	KeyCount  int64
	SizeBytes int64
	// ExpiredPresent is the number of keys in this namespace past their
	// expires_at but not yet swept -- see [Stats.ExpiredPresent].
	ExpiredPresent int64
}

// Namespaces enumerates every namespace present in the file this Cache is
// open against, regardless of which namespace this Cache itself is pinned
// to via [WithNamespace]. A Cache's other methods only ever see their own
// configured namespace; this is the one exception, meant for admin and
// introspection tooling (e.g. the lytecache CLI's `ui` command) that needs
// to show a whole file's contents, not application code.
//
// Namespaces is a pure read against readDB: unlike Get, Inspect, and
// TTLOf, it never opportunistically deletes expired rows or updates LRU
// bookkeeping, so it is safe to call on a fixed polling interval (a
// dashboard refresh, a Prometheus scrape) without perturbing the cache.
func (c *Cache) Namespaces() ([]NamespaceInfo, error) {
	const q = `
SELECT namespace,
       COUNT(*),
       COALESCE(SUM(size_bytes), 0),
       COALESCE(SUM(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 ELSE 0 END), 0)
FROM cache
GROUP BY namespace
ORDER BY namespace`

	rows, err := c.readDB.Query(q, nowMillis())
	if err != nil {
		return nil, fmt.Errorf("lytecache: namespaces: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []NamespaceInfo
	for rows.Next() {
		var info NamespaceInfo
		if err := rows.Scan(&info.Namespace, &info.KeyCount, &info.SizeBytes, &info.ExpiredPresent); err != nil {
			return nil, fmt.Errorf("lytecache: namespaces: %w", err)
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: namespaces: %w", err)
	}
	return out, nil
}
