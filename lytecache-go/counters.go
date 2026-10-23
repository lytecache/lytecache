package lytecache

import (
	"database/sql"
	"fmt"
	"strconv"
)

// atomicIncrSQL is a single UPSERT: correct under concurrent access from
// many goroutines or many OS processes sharing the file, never a
// read-modify-write race. It relies on the value being stored as UTF-8
// decimal text (see encodeIntBytes/encodeFloatBytes): CAST(value AS TEXT)
// reads the digits, SQLite coerces them to a number for the addition, and
// the outer CAST(... AS TEXT) converts the result back to decimal digits
// before storing it as a BLOB again. An expired existing row is treated as
// absent (starts from zero) rather than as an error.
const atomicIncrSQL = `
INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES (@key, @ns, @initial, @rtype, @now, NULL, @now, 0, @initlen)
ON CONFLICT(namespace, key) DO UPDATE SET
  value = CAST(CAST(
    (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN 0 ELSE CAST(cache.value AS TEXT) END)
    + @amount AS TEXT) AS BLOB),
  value_type = @rtype,
  expires_at = CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN NULL ELSE cache.expires_at END,
  last_accessed = @now,
  access_count = cache.access_count + 1,
  size_bytes = length(CAST(CAST(
    (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= @now THEN 0 ELSE CAST(cache.value AS TEXT) END)
    + @amount AS TEXT) AS BLOB))
WHERE (cache.expires_at IS NOT NULL AND cache.expires_at <= @now) OR cache.value_type IN (%s)`

// atomicIncr runs atomicIncrSQL and returns the new stored value as text.
func (c *Cache) atomicIncr(key string, amountText string, resultType int, allowedTypesSQL string) (string, error) {
	now := nowMillis()
	initial := []byte(amountText)
	q := fmt.Sprintf(atomicIncrSQL, allowedTypesSQL)

	res, err := c.writeDB.Exec(q,
		sql.Named("key", key),
		sql.Named("ns", c.namespace),
		sql.Named("initial", initial),
		sql.Named("rtype", resultType),
		sql.Named("now", now),
		sql.Named("initlen", len(initial)),
		sql.Named("amount", amountText),
	)
	if err != nil {
		return "", fmt.Errorf("lytecache: incr %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("lytecache: incr %q: %w", key, err)
	}
	if n == 0 {
		return "", fmt.Errorf("%w: key %q", ErrNotNumeric, key)
	}

	var stored []byte
	err = c.writeDB.QueryRow(`SELECT value FROM cache WHERE namespace = ? AND key = ?`, c.namespace, key).Scan(&stored)
	if err != nil {
		return "", fmt.Errorf("lytecache: incr %q: reading back result: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeOpportunisticSweep()
	return string(stored), nil
}

// Incr atomically adds amount (which may be negative) to the integer
// stored at key, creating it (starting from 0) if absent, and returns the
// new value. It returns an error wrapping [ErrNotNumeric] if the existing
// value is not an integer -- Incr never silently reinterprets a float.
func (c *Cache) Incr(key string, amount int64) (int64, error) {
	text, err := c.atomicIncr(key, strconv.FormatInt(amount, 10), typeInt, "2")
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("lytecache: incr %q: stored value %q is not a valid integer: %w", key, text, err)
	}
	return n, nil
}

// Decr is equivalent to Incr(key, -amount).
func (c *Cache) Decr(key string, amount int64) (int64, error) {
	return c.Incr(key, -amount)
}

// IncrFloat atomically adds amount to the numeric value stored at key
// (which may be an integer or a float), creating it (starting from 0) if
// absent, and returns the new value as a float. It returns an error
// wrapping [ErrNotNumeric] if the existing value is not numeric.
func (c *Cache) IncrFloat(key string, amount float64) (float64, error) {
	text, err := c.atomicIncr(key, strconv.FormatFloat(amount, 'g', -1, 64), typeFloat, "2,3")
	if err != nil {
		return 0, err
	}
	f, err := decodeFloatText(text)
	if err != nil {
		return 0, fmt.Errorf("lytecache: incrfloat %q: %w", key, err)
	}
	return f, nil
}
