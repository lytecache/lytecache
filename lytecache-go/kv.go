package lytecache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SetOption configures a call to [Cache.Set], [Cache.Add], [Cache.Replace],
// or [Cache.SetMany].
type SetOption func(*setOptions)

type setOptions struct {
	ttl *time.Duration
}

// TTL sets a time-to-live for the value being written. Absent, the key has
// no expiry. Zero or negative expires the key immediately -- the next read
// is a miss.
func TTL(d time.Duration) SetOption {
	return func(o *setOptions) { o.ttl = &d }
}

func applySetOptions(opts []SetOption) setOptions {
	var so setOptions
	for _, opt := range opts {
		opt(&so)
	}
	return so
}

const upsertSQL = `
INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
ON CONFLICT(namespace, key) DO UPDATE SET
  value = excluded.value,
  value_type = excluded.value_type,
  created_at = excluded.created_at,
  expires_at = excluded.expires_at,
  last_accessed = excluded.last_accessed,
  access_count = 0,
  size_bytes = excluded.size_bytes`

func expiresAtArg(ttl *time.Duration, now int64) any {
	if ttl == nil {
		return nil
	}
	return now + ttl.Milliseconds()
}

// Set stores value under key, replacing any existing value. See [TTL] for
// the optional expiry.
func (c *Cache) Set(key string, value any, opts ...SetOption) error {
	so := applySetOptions(opts)
	data, typeCode, err := encodeValue(value)
	if err != nil {
		return err
	}

	if c.eviction == NoEviction && (c.maxKeys != nil || c.maxBytes != nil) {
		if err := c.checkCapacity(key); err != nil {
			return err
		}
	}

	now := nowMillis()
	_, err = c.writeDB.Exec(upsertSQL, key, c.namespace, data, typeCode, now, expiresAtArg(so.ttl, now), now, len(data))
	if err != nil {
		return fmt.Errorf("lytecache: set %q: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeEvict()
	c.maybeOpportunisticSweep()
	return nil
}

// Get reads key into dest, which must be a non-nil pointer. found is false
// (with a nil error) if the key is missing or expired -- a miss is not an
// error, mirroring a map lookup.
//
// Supported destinations depend on how the value was stored: *[]byte,
// *string, *int and other integer types, *float32/*float64, *any, or a
// pointer to anything [encoding/json.Unmarshal] accepts (struct, map,
// slice, ...) for values stored as JSON. See SPEC.md for the full table.
func (c *Cache) Get(key string, dest any) (found bool, err error) {
	data, typeCode, found, err := c.getRaw(key)
	if err != nil || !found {
		return false, err
	}
	if err := decodeInto(data, typeCode, dest); err != nil {
		return false, c.degradeReadError(key, "deserialize", err)
	}
	return true, nil
}

// GetBytes is a typed convenience wrapper around [Cache.Get].
func (c *Cache) GetBytes(key string) ([]byte, bool, error) {
	var v []byte
	found, err := c.Get(key, &v)
	return v, found, err
}

// GetString is a typed convenience wrapper around [Cache.Get].
func (c *Cache) GetString(key string) (string, bool, error) {
	var v string
	found, err := c.Get(key, &v)
	return v, found, err
}

// GetInt64 is a typed convenience wrapper around [Cache.Get].
func (c *Cache) GetInt64(key string) (int64, bool, error) {
	var v int64
	found, err := c.Get(key, &v)
	return v, found, err
}

// GetFloat64 is a typed convenience wrapper around [Cache.Get].
func (c *Cache) GetFloat64(key string) (float64, bool, error) {
	var v float64
	found, err := c.Get(key, &v)
	return v, found, err
}

// getRaw runs the shared select-and-lazily-expire path used by Get and
// GetMany, recording hit/miss stats and buffering the LRU bookkeeping
// update rather than writing it synchronously.
func (c *Cache) getRaw(key string) (data []byte, typeCode int, found bool, err error) {
	const q = `SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?`
	var expiresAt sql.NullInt64
	scanErr := c.readDB.QueryRow(q, c.namespace, key).Scan(&data, &typeCode, &expiresAt)
	c.maybeOpportunisticSweep()

	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		c.misses.Add(1)
		return nil, 0, false, nil
	case scanErr != nil:
		wrapped := fmt.Errorf("lytecache: get %q: %w", key, scanErr)
		if c.strict {
			return nil, 0, false, wrapped
		}
		c.logger.Warn("lytecache: read error, degrading to miss", "key", key, "error", wrapped)
		return nil, 0, false, nil
	}

	now := nowMillis()
	if expiresAt.Valid && expiresAt.Int64 <= now {
		c.misses.Add(1)
		c.deleteRaw(key)
		return nil, 0, false, nil
	}

	c.hits.Add(1)
	c.bufferLRU(key, now)
	return data, typeCode, true, nil
}

// degradeReadError implements the strict/non-strict contract shared by
// every read path: strict mode returns the error, non-strict mode logs a
// warning and reports a miss.
func (c *Cache) degradeReadError(key, action string, err error) error {
	if c.strict {
		return err
	}
	c.logger.Warn("lytecache: failed to "+action+" key, degrading to miss", "key", key, "error", err)
	return nil
}

func (c *Cache) deleteRaw(key string) {
	_, _ = c.writeDB.Exec(`DELETE FROM cache WHERE namespace = ? AND key = ?`, c.namespace, key)
}

// Delete deletes the given keys and returns how many actually existed.
func (c *Cache) Delete(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	q, args := inClauseQuery(`DELETE FROM cache WHERE namespace = ? AND key IN (%s)`, c.namespace, keys)
	res, err := c.writeDB.Exec(q, args...)
	if err != nil {
		return 0, fmt.Errorf("lytecache: delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("lytecache: delete: %w", err)
	}
	if n > 0 {
		c.changed(opDelete, dedupe(keys), int(n))
	}
	return int(n), nil
}

// Exists reports whether key is present and not expired.
func (c *Cache) Exists(key string) (bool, error) {
	const q = `SELECT expires_at FROM cache WHERE namespace = ? AND key = ?`
	var expiresAt sql.NullInt64
	err := c.readDB.QueryRow(q, c.namespace, key).Scan(&expiresAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		wrapped := fmt.Errorf("lytecache: exists %q: %w", key, err)
		if c.strict {
			return false, wrapped
		}
		c.logger.Warn("lytecache: read error, degrading to miss", "key", key, "error", wrapped)
		return false, nil
	}
	if expiresAt.Valid && expiresAt.Int64 <= nowMillis() {
		c.deleteRaw(key)
		return false, nil
	}
	return true, nil
}

// Add sets key only if it is currently absent or expired (Redis "SET NX"),
// atomically. It reports whether the value was set.
func (c *Cache) Add(key string, value any, opts ...SetOption) (bool, error) {
	return c.add(key, value, true, opts...)
}

func (c *Cache) add(key string, value any, emit bool, opts ...SetOption) (bool, error) {
	so := applySetOptions(opts)
	data, typeCode, err := encodeValue(value)
	if err != nil {
		return false, err
	}
	if c.eviction == NoEviction && (c.maxKeys != nil || c.maxBytes != nil) {
		if err := c.checkCapacity(key); err != nil {
			return false, err
		}
	}

	now := nowMillis()
	const q = upsertSQL + `
WHERE cache.expires_at IS NOT NULL AND cache.expires_at <= ?`
	res, err := c.writeDB.Exec(q, key, c.namespace, data, typeCode, now, expiresAtArg(so.ttl, now), now, len(data), now)
	if err != nil {
		return false, fmt.Errorf("lytecache: add %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("lytecache: add %q: %w", key, err)
	}
	if n > 0 {
		if emit {
			c.changed(opSet, []string{key}, 1)
		}
		c.maybeEvict()
	}
	c.maybeOpportunisticSweep()
	return n > 0, nil
}

// Replace sets key only if it is currently present and not expired (Redis
// "SET XX"), atomically. It reports whether the value was set.
func (c *Cache) Replace(key string, value any, opts ...SetOption) (bool, error) {
	so := applySetOptions(opts)
	data, typeCode, err := encodeValue(value)
	if err != nil {
		return false, err
	}

	now := nowMillis()
	const q = `
UPDATE cache SET value = ?, value_type = ?, expires_at = ?, last_accessed = ?, access_count = 0, size_bytes = ?
WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)`
	res, err := c.writeDB.Exec(q, data, typeCode, expiresAtArg(so.ttl, now), now, len(data), c.namespace, key, now)
	if err != nil {
		return false, fmt.Errorf("lytecache: replace %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("lytecache: replace %q: %w", key, err)
	}
	if n > 0 {
		c.changed(opSet, []string{key}, 1)
	}
	c.maybeOpportunisticSweep()
	return n > 0, nil
}

// GetSet atomically swaps in value and decodes the previous value into
// dest, reporting whether a previous value existed. Like Redis's GETSET, it
// clears any TTL that was set on the previous value -- the new value has
// no expiry unless you call [Cache.Expire] afterward.
func (c *Cache) GetSet(key string, value any, dest any) (found bool, err error) {
	data, typeCode, err := encodeValue(value)
	if err != nil {
		return false, err
	}

	tx, err := c.writeDB.BeginTx(context.Background(), nil)
	if err != nil {
		return false, fmt.Errorf("lytecache: getset %q: %w", key, err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	var oldData []byte
	var oldType int
	var expiresAt sql.NullInt64
	scanErr := tx.QueryRow(`SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?`,
		c.namespace, key).Scan(&oldData, &oldType, &expiresAt)
	hadPrevious := true
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		hadPrevious = false
	case scanErr != nil:
		return false, fmt.Errorf("lytecache: getset %q: %w", key, scanErr)
	}
	if hadPrevious && expiresAt.Valid && expiresAt.Int64 <= nowMillis() {
		hadPrevious = false
	}

	now := nowMillis()
	const q = `
INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?)
ON CONFLICT(namespace, key) DO UPDATE SET
  value = excluded.value, value_type = excluded.value_type, created_at = excluded.created_at,
  expires_at = NULL, last_accessed = excluded.last_accessed, access_count = 0, size_bytes = excluded.size_bytes`
	if _, err := tx.Exec(q, key, c.namespace, data, typeCode, now, now, len(data)); err != nil {
		return false, fmt.Errorf("lytecache: getset %q: %w", key, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("lytecache: getset %q: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeOpportunisticSweep()

	if !hadPrevious {
		return false, nil
	}
	if err := decodeInto(oldData, oldType, dest); err != nil {
		return false, c.degradeReadError(key, "deserialize", err)
	}
	return true, nil
}

// SetMany writes every entry in a single transaction. See [TTL] for the
// optional shared expiry applied to every entry.
//
// Unlike Set/Add, SetMany does not individually enforce WithMaxKeys /
// WithMaxBytes under the NoEviction policy before writing each entry --
// capacity is still enforced afterward via the configured eviction policy,
// but a large batch write is not rejected partway through.
func (c *Cache) SetMany(entries map[string]any, opts ...SetOption) error {
	if len(entries) == 0 {
		return nil
	}
	so := applySetOptions(opts)

	tx, err := c.writeDB.Begin()
	if err != nil {
		return fmt.Errorf("lytecache: setmany: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	stmt, err := tx.Prepare(upsertSQL)
	if err != nil {
		return fmt.Errorf("lytecache: setmany: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	now := nowMillis()
	expiresAt := expiresAtArg(so.ttl, now)
	for key, value := range entries {
		data, typeCode, err := encodeValue(value)
		if err != nil {
			return fmt.Errorf("lytecache: setmany key %q: %w", key, err)
		}
		if _, err := stmt.Exec(key, c.namespace, data, typeCode, now, expiresAt, now, len(data)); err != nil {
			return fmt.Errorf("lytecache: setmany key %q: %w", key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("lytecache: setmany: %w", err)
	}
	// ONE change record for the whole batch, never one per key.
	c.changed(opSet, sortedKeys(entries), len(entries))
	c.maybeEvict()
	c.maybeOpportunisticSweep()
	return nil
}

// RawValue is a value returned by [Cache.GetMany]: the raw stored bytes
// plus enough information to decode them via [RawValue.Decode].
type RawValue struct {
	Data     []byte
	typeCode int
}

// Decode decodes the value into dest, which must be a non-nil pointer. See
// [Cache.Get] for the supported destination shapes.
func (rv RawValue) Decode(dest any) error {
	return decodeInto(rv.Data, rv.typeCode, dest)
}

// ValueType returns the stored value_type code (0-6; see SPEC.md), for
// tools that display a value without knowing its Go type in advance.
func (rv RawValue) ValueType() int { return rv.typeCode }

// GetMany reads all of the given keys in a single query, skipping missing
// or expired keys rather than erroring. Decode each result with
// [RawValue.Decode].
func (c *Cache) GetMany(keys []string) (map[string]RawValue, error) {
	result := make(map[string]RawValue, len(keys))
	if len(keys) == 0 {
		return result, nil
	}

	q, args := inClauseQuery(`SELECT key, value, value_type, expires_at FROM cache WHERE namespace = ? AND key IN (%s)`, c.namespace, keys)
	rows, err := c.readDB.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("lytecache: getmany: %w", err)
	}
	defer func() { _ = rows.Close() }()

	now := nowMillis()
	var expiredKeys []string
	for rows.Next() {
		var key string
		var data []byte
		var typeCode int
		var expiresAt sql.NullInt64
		if err := rows.Scan(&key, &data, &typeCode, &expiresAt); err != nil {
			return nil, fmt.Errorf("lytecache: getmany: %w", err)
		}
		if expiresAt.Valid && expiresAt.Int64 <= now {
			expiredKeys = append(expiredKeys, key)
			continue
		}
		result[key] = RawValue{Data: data, typeCode: typeCode}
		c.bufferLRU(key, now)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: getmany: %w", err)
	}
	c.maybeOpportunisticSweep()

	if len(expiredKeys) > 0 {
		_, _ = c.Delete(expiredKeys...)
	}
	c.hits.Add(int64(len(result)))
	c.misses.Add(int64(len(keys) - len(result) - len(expiredKeys)))
	c.misses.Add(int64(len(expiredKeys)))
	return result, nil
}

// checkCapacity enforces WithMaxKeys/WithMaxBytes for the NoEviction
// policy, ahead of a single-key write, so a rejected write for a *new* key
// never has a side effect. Updating an existing key is always allowed,
// since it never grows the dataset.
func (c *Cache) checkCapacity(key string) error {
	var exists int
	err := c.readDB.QueryRow(`SELECT 1 FROM cache WHERE namespace = ? AND key = ?`, c.namespace, key).Scan(&exists)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("lytecache: checking capacity: %w", err)
	}

	if c.maxKeys != nil {
		var count int64
		if err := c.readDB.QueryRow(`SELECT COUNT(*) FROM cache WHERE namespace = ?`, c.namespace).Scan(&count); err != nil {
			return fmt.Errorf("lytecache: checking capacity: %w", err)
		}
		if count >= *c.maxKeys {
			return fmt.Errorf("%w: namespace %q has reached its %d-key limit", ErrCacheFull, c.namespace, *c.maxKeys)
		}
	}
	if c.maxBytes != nil {
		var total sql.NullInt64
		if err := c.readDB.QueryRow(`SELECT SUM(size_bytes) FROM cache WHERE namespace = ?`, c.namespace).Scan(&total); err != nil {
			return fmt.Errorf("lytecache: checking capacity: %w", err)
		}
		if total.Valid && total.Int64 >= *c.maxBytes {
			return fmt.Errorf("%w: namespace %q has reached its %d-byte limit", ErrCacheFull, c.namespace, *c.maxBytes)
		}
	}
	return nil
}

// inClauseQuery builds a query with a "key IN (?, ?, ...)" clause, with
// namespace as the first bound parameter.
func inClauseQuery(format, namespace string, keys []string) (string, []any) {
	placeholders := make([]string, len(keys))
	args := make([]any, 0, len(keys)+1)
	args = append(args, namespace)
	for i, k := range keys {
		placeholders[i] = "?"
		args = append(args, k)
	}
	return fmt.Sprintf(format, strings.Join(placeholders, ",")), args
}
