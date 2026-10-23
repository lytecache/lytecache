package lytecache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// Hashes are not a separate type: a hash is a view over an ordinary
// value_type-4 row whose JSON root is an object. Anything written with
// Set(key, map/struct) is usable with HGet, and Get on a hash returns the
// whole document. See SPEC.md, "Hashes".
//
// The reason these exist is atomicity, not I/O. On the JSON1 path every
// write is computed inside SQLite from the row's *current* value
// (json_set/json_remove), so two processes updating different fields of one
// key never lose each other's write. Where an exact return value needs more
// than one statement (HSet's "created", HDel's count), the method runs
// guarded writes -- "set the field WHERE it is absent" -- in one
// transaction; no write ever depends on data read into Go. The
// read-modify-write fallback (only when the linked SQLite lacks JSON1, or
// LYTECACHE_FORCE_JSON_FALLBACK=1 is set) holds BEGIN IMMEDIATE throughout.

// forceJSONFallbackEnv forces the read-modify-write hash implementation, so
// tests can prove both paths equivalent.
const forceJSONFallbackEnv = "LYTECACHE_FORCE_JSON_FALLBACK"

// Field names are always bound (@f0, @f1, ...), never interpolated: the
// path is built in SQL as $."<field>" with backslashes and quotes escaped.
// The BLOB value is CAST to TEXT before every JSON function so SQLite 3.45+
// never reads it as JSONB, and results are CAST back to BLOB.
const (
	hashDoc      = `CAST(cache.value AS TEXT)`
	hashLive     = `(cache.expires_at IS NULL OR cache.expires_at > @now)`
	hashExpired  = `(cache.expires_at IS NOT NULL AND cache.expires_at <= @now)`
	hashRootType = `(CASE WHEN json_valid(` + hashDoc + `) THEN json_type(` + hashDoc + `) END)`
	hashIsObj    = `(cache.value_type = 4 AND CASE WHEN json_valid(` + hashDoc + `) THEN json_type(` + hashDoc + `) END = 'object')`
)

func hashPath(param string) string {
	return `('$."' || replace(replace(@` + param + `, '\', '\\'), '"', '\"') || '"')`
}

// jsonFloatText is JSON text for a REAL with the fewest digits that
// round-trip exactly. SQLite before ~3.5x renders a REAL into JSON (and
// CAST(x AS TEXT)) with 15 significant digits, which would store 0.1+0.2 as
// 0.3; this picks the shortest of 15/16/17 digits that reads back to the
// same double, on every SQLite version.
func jsonFloatText(expr string) string {
	p := func(n string) string { return `printf('%!.` + n + `g', ` + expr + `)` }
	return `(CASE WHEN CAST(` + p("15") + ` AS REAL) = ` + expr + ` THEN ` + p("15") +
		` WHEN CAST(` + p("16") + ` AS REAL) = ` + expr + ` THEN ` + p("16") + ` ELSE ` + p("17") + ` END)`
}

func hashFieldType(param string) string {
	return `(CASE WHEN ` + hashIsObj + ` THEN json_type(` + hashDoc + `, ` + hashPath(param) + `) END)`
}

const (
	hsetManyPerStatement = 500 // fields per statement (bound-variable limit)
	jsonSetPairsPerCall  = 30  // path/value pairs per json_set (function-argument limit)
)

var typeNames = map[int]string{0: "bytes", 1: "string", 2: "int", 3: "float", 4: "json", 5: "pickle", 6: "java-native"}

func wrongTypeError(key string, valueType int, rootType sql.NullString) error {
	if valueType != typeJSON {
		name, ok := typeNames[valueType]
		if !ok {
			name = "unknown"
		}
		return fmt.Errorf("%w: key %q holds a value of type %s (value_type=%d), not a JSON object; hash operations apply only to JSON objects (value_type=4)",
			ErrSerialization, key, name, valueType)
	}
	if !rootType.Valid {
		return fmt.Errorf("%w: key %q holds malformed JSON (value_type=4)", ErrSerialization, key)
	}
	return fmt.Errorf("%w: key %q holds a JSON %s, not a JSON object; hash operations apply only to JSON objects",
		ErrSerialization, key, rootType.String)
}

// detectJSON1 runs once, on the write connection, when the Cache opens.
func (c *Cache) detectJSON1() {
	if os.Getenv(forceJSONFallbackEnv) != "" {
		c.json1 = false
		c.logger.Warn("lytecache: " + forceJSONFallbackEnv + " is set; hash operations use the read-modify-write fallback")
		return
	}
	var valid int
	var arr string
	err := c.writeDB.QueryRow(`SELECT json_valid('{}'), '{"a\"b":[1]}' -> '$."a\"b"'`).Scan(&valid, &arr)
	c.json1 = err == nil && valid == 1 && arr == "[1]"
	if !c.json1 {
		var version string
		_ = c.writeDB.QueryRow(`SELECT sqlite_version()`).Scan(&version)
		c.logger.Warn("lytecache: SQLite lacks the JSON1 support hash operations need (SQLite >= 3.38); "+
			"falling back to a slower read-modify-write under BEGIN IMMEDIATE", "sqlite_version", version)
	}
}

// encodeHashValue encodes one field value as compact JSON text. []byte is
// rejected (encoding/json would silently store base64), as are NaN/Inf.
func encodeHashValue(value any) (string, error) {
	if _, ok := value.([]byte); ok {
		return "", fmt.Errorf("%w: hash fields hold JSON values; []byte is not supported", ErrSerialization)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return "", fmt.Errorf("%w: cannot store %T in a hash field: %v", ErrSerialization, value, err)
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

func (c *Cache) hashArgs(key string, now int64, so setOptions) []any {
	return []any{
		sql.Named("ns", c.namespace),
		sql.Named("key", key),
		sql.Named("now", now),
		sql.Named("exp", expiresAtArg(so.ttl, now)),
	}
}

// upsertHashSQL creates freshDoc for a missing/expired key, or rewrites a
// live JSON object to liveDoc when where holds. expires_at is preserved on
// a live row and size_bytes is recomputed from the new value in the same
// statement.
func upsertHashSQL(liveDoc, freshDoc, where string) string {
	newDoc := `CAST(CASE WHEN ` + hashExpired + ` THEN ` + freshDoc + ` ELSE ` + liveDoc + ` END AS BLOB)`
	return `
INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES (@key, @ns, CAST(` + freshDoc + ` AS BLOB), 4, @now, @exp, @now, 0, length(CAST(` + freshDoc + ` AS BLOB)))
ON CONFLICT(namespace, key) DO UPDATE SET
  value = ` + newDoc + `,
  value_type = 4,
  created_at = CASE WHEN ` + hashExpired + ` THEN @now ELSE cache.created_at END,
  expires_at = CASE WHEN ` + hashExpired + ` THEN @exp ELSE cache.expires_at END,
  last_accessed = @now,
  access_count = CASE WHEN ` + hashExpired + ` THEN 0 ELSE cache.access_count + 1 END,
  size_bytes = length(` + newDoc + `)
WHERE ` + hashExpired + ` OR ` + where
}

type querier interface {
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

func (c *Cache) checkHashType(q querier, key string, now int64) error {
	var vt int
	var root sql.NullString
	err := q.QueryRow(`SELECT cache.value_type, `+hashRootType+` FROM cache WHERE namespace = @ns AND key = @key AND `+hashLive,
		sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now)).Scan(&vt, &root)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if vt != typeJSON || root.String != "object" {
		return wrongTypeError(key, vt, root)
	}
	return nil
}

func (c *Cache) capacityCheck(key string) error {
	if c.eviction == NoEviction && (c.maxKeys != nil || c.maxBytes != nil) {
		return c.checkCapacity(key)
	}
	return nil
}

func rowsAffected(res sql.Result) int64 {
	n, err := res.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

// HSet sets one field of the JSON object at key, atomically. created
// reports whether the field was new (false: it replaced a value).
//
// A missing (or expired) key is created as a new object with no expiry,
// unless a [TTL] option is passed; that TTL is ignored when the key already
// exists, whose expiry a field write never changes. Returns an error
// wrapping [ErrSerialization] if key holds anything but a JSON object.
func (c *Cache) HSet(key, field string, value any, opts ...SetOption) (created bool, err error) {
	encoded, err := encodeHashValue(value)
	if err != nil {
		return false, err
	}
	so := applySetOptions(opts)
	if !c.json1 {
		r, err := c.rmwHash(key, so, true, func(o *jsonObject) (bool, any, error) {
			created := !o.has(field)
			o.set(field, json.RawMessage(encoded))
			return true, created, nil
		})
		if err != nil {
			return false, fmt.Errorf("lytecache: hset %q: %w", key, err)
		}
		return r.(bool), nil
	}
	if err := c.capacityCheck(key); err != nil {
		return false, err
	}
	now := nowMillis()
	args := append(c.hashArgs(key, now, so), sql.Named("f0", field), sql.Named("v0", encoded))
	setExpr := `json_set(` + hashDoc + `, ` + hashPath("f0") + `, json(@v0))`
	err = c.inTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(upsertHashSQL(setExpr, `json_object(@f0, json(@v0))`,
			`(`+hashFieldType("f0")+` IS NULL AND `+hashIsObj+`)`), args...)
		if err != nil {
			return err
		}
		if created = rowsAffected(res) == 1; created {
			return nil
		}
		res, err = tx.Exec(`UPDATE cache SET value = CAST(`+setExpr+` AS BLOB), size_bytes = length(CAST(`+setExpr+` AS BLOB)),
  last_accessed = @now, access_count = access_count + 1
WHERE namespace = @ns AND key = @key AND `+hashLive+` AND `+hashIsObj, args...)
		if err != nil {
			return err
		}
		if rowsAffected(res) == 0 {
			return c.checkHashType(tx, key, now)
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("lytecache: hset %q: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeEvict()
	c.maybeOpportunisticSweep()
	return created, nil
}

// HSetNX sets field only if it is absent, atomically, reporting whether it
// was set. A missing key is created exactly as [Cache.HSet] does.
func (c *Cache) HSetNX(key, field string, value any, opts ...SetOption) (bool, error) {
	encoded, err := encodeHashValue(value)
	if err != nil {
		return false, err
	}
	so := applySetOptions(opts)
	if !c.json1 {
		r, err := c.rmwHash(key, so, true, func(o *jsonObject) (bool, any, error) {
			if o.has(field) {
				return false, false, nil
			}
			o.set(field, json.RawMessage(encoded))
			return true, true, nil
		})
		if err != nil {
			return false, fmt.Errorf("lytecache: hsetnx %q: %w", key, err)
		}
		return r.(bool), nil
	}
	if err := c.capacityCheck(key); err != nil {
		return false, err
	}
	now := nowMillis()
	args := append(c.hashArgs(key, now, so), sql.Named("f0", field), sql.Named("v0", encoded))
	var won bool
	err = c.inTx(func(tx *sql.Tx) error {
		res, err := tx.Exec(upsertHashSQL(`json_set(`+hashDoc+`, `+hashPath("f0")+`, json(@v0))`,
			`json_object(@f0, json(@v0))`, `(`+hashFieldType("f0")+` IS NULL AND `+hashIsObj+`)`), args...)
		if err != nil {
			return err
		}
		if won = rowsAffected(res) == 1; !won {
			return c.checkHashType(tx, key, now)
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("lytecache: hsetnx %q: %w", key, err)
	}
	if won {
		c.changed(opSet, []string{key}, 1)
		c.maybeEvict()
	}
	return won, nil
}

// HSetMany sets several fields at once, atomically (Redis' multi-field
// HSET): all land in one transaction or none do. Missing-key creation and
// TTL handling are the same as [Cache.HSet].
func (c *Cache) HSetMany(key string, fields map[string]any, opts ...SetOption) error {
	if len(fields) == 0 {
		return nil
	}
	names := sortedKeys(fields)
	encoded := make([]string, len(names))
	for i, f := range names {
		e, err := encodeHashValue(fields[f])
		if err != nil {
			return err
		}
		encoded[i] = e
	}
	so := applySetOptions(opts)
	if !c.json1 {
		_, err := c.rmwHash(key, so, true, func(o *jsonObject) (bool, any, error) {
			for i, f := range names {
				o.set(f, json.RawMessage(encoded[i]))
			}
			return true, nil, nil
		})
		if err != nil {
			return fmt.Errorf("lytecache: hsetmany %q: %w", key, err)
		}
		return nil
	}
	if err := c.capacityCheck(key); err != nil {
		return err
	}
	now := nowMillis()
	err := c.inTx(func(tx *sql.Tx) error {
		// SQLite caps function arguments and bound variables, so a large
		// mapping is applied as several statements -- each json_set nested
		// over groups of pairs -- inside this one transaction.
		for start := 0; start < len(names); start += hsetManyPerStatement {
			end := min(start+hsetManyPerStatement, len(names))
			args := c.hashArgs(key, now, so)
			liveDoc, freshDoc := hashDoc, `'{}'`
			for g := start; g < end; g += jsonSetPairsPerCall {
				var pairs []string
				for i := g; i < min(g+jsonSetPairsPerCall, end); i++ {
					fp, vp := "f"+strconv.Itoa(i), "v"+strconv.Itoa(i)
					args = append(args, sql.Named(fp, names[i]), sql.Named(vp, encoded[i]))
					pairs = append(pairs, hashPath(fp)+`, json(@`+vp+`)`)
				}
				joined := strings.Join(pairs, ", ")
				liveDoc = `json_set(` + liveDoc + `, ` + joined + `)`
				freshDoc = `json_set(` + freshDoc + `, ` + joined + `)`
			}
			res, err := tx.Exec(upsertHashSQL(liveDoc, freshDoc, hashIsObj), args...)
			if err != nil {
				return err
			}
			if rowsAffected(res) == 0 {
				return c.checkHashType(tx, key, now)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("lytecache: hsetmany %q: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeEvict()
	return nil
}

// HGet decodes one field of the JSON object at key into dest (a non-nil
// pointer, as for [encoding/json.Unmarshal]). found is false, with a nil
// error, for a missing key, missing field or expired key -- a miss is never
// an error. A stored JSON null is found (dest gets its zero/nil value).
func (c *Cache) HGet(key, field string, dest any) (found bool, err error) {
	raw, found, err := c.HGetRaw(key, field)
	if err != nil || !found {
		return false, err
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return false, fmt.Errorf("%w: hget %q field %q: %v", ErrSerialization, key, field, err)
	}
	return true, nil
}

// HGetRaw returns one field's JSON text, without decoding it.
func (c *Cache) HGetRaw(key, field string) (json.RawMessage, bool, error) {
	vals, found, err := c.hmget(key, []string{field})
	if err != nil || !found {
		return nil, false, err
	}
	return vals[0], vals[0] != nil, nil
}

// HGetString is a typed convenience wrapper around [Cache.HGet].
func (c *Cache) HGetString(key, field string) (string, bool, error) {
	var v string
	found, err := c.HGet(key, field, &v)
	return v, found, err
}

// HGetInt64 is a typed convenience wrapper around [Cache.HGet].
func (c *Cache) HGetInt64(key, field string) (int64, bool, error) {
	var v int64
	found, err := c.HGet(key, field, &v)
	return v, found, err
}

// HMGet returns several fields' JSON text, in order, with nil for each one
// that is missing (all nil for a missing key).
func (c *Cache) HMGet(key string, fields []string) ([]json.RawMessage, error) {
	if len(fields) == 0 {
		return []json.RawMessage{}, nil
	}
	vals, found, err := c.hmget(key, fields)
	if err != nil {
		return nil, err
	}
	if !found {
		return make([]json.RawMessage, len(fields)), nil
	}
	return vals, nil
}

func (c *Cache) hmget(key string, fields []string) ([]json.RawMessage, bool, error) {
	now := nowMillis()
	out := make([]json.RawMessage, len(fields))
	if !c.json1 {
		o, err := c.loadHash(c.readDB, key, now)
		if err != nil {
			return nil, false, fmt.Errorf("lytecache: hget %q: %w", key, err)
		}
		c.recordHashRead(key, now, o != nil)
		if o == nil {
			return nil, false, nil
		}
		for i, f := range fields {
			if v, ok := o.get(f); ok {
				out[i] = v
			}
		}
		return out, true, nil
	}
	args := []any{sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now)}
	cols := make([]string, len(fields))
	for i, f := range fields {
		p := "f" + strconv.Itoa(i)
		args = append(args, sql.Named(p, f))
		cols[i] = `CASE WHEN ` + hashIsObj + ` THEN ` + hashDoc + ` -> ` + hashPath(p) + ` END`
	}
	var vt int
	var root sql.NullString
	texts := make([]sql.NullString, len(fields))
	dests := []any{&vt, &root}
	for i := range texts {
		dests = append(dests, &texts[i])
	}
	err := c.readDB.QueryRow(`SELECT cache.value_type, `+hashRootType+`, `+strings.Join(cols, ", ")+
		` FROM cache WHERE namespace = @ns AND key = @key AND `+hashLive, args...).Scan(dests...)
	if errors.Is(err, sql.ErrNoRows) {
		c.recordHashRead(key, now, false)
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("lytecache: hget %q: %w", key, err)
	}
	if vt != typeJSON || root.String != "object" {
		return nil, false, wrongTypeError(key, vt, root)
	}
	for i, t := range texts {
		if t.Valid {
			out[i] = json.RawMessage(t.String)
		}
	}
	c.recordHashRead(key, now, true)
	return out, true, nil
}

// HGetAll returns every field of the object at key (an empty map if the
// key is missing). Decode the whole document into a struct with
// [Cache.Get] instead, if that is what you want.
func (c *Cache) HGetAll(key string) (map[string]json.RawMessage, error) {
	now := nowMillis()
	o, err := c.loadHash(c.readDB, key, now)
	if err != nil {
		return nil, fmt.Errorf("lytecache: hgetall %q: %w", key, err)
	}
	c.recordHashRead(key, now, o != nil)
	out := map[string]json.RawMessage{}
	if o != nil {
		for _, f := range o.fields {
			out[f.key] = f.value
		}
	}
	return out, nil
}

// HDel removes fields and returns how many were present and removed.
// Unlike Redis, removing the last field leaves an empty object {} rather
// than deleting the key. A missing key returns 0.
func (c *Cache) HDel(key string, fields ...string) (int, error) {
	unique := dedupe(fields)
	if len(unique) == 0 {
		return 0, nil
	}
	if !c.json1 {
		r, err := c.rmwHash(key, setOptions{}, false, func(o *jsonObject) (bool, any, error) {
			n := 0
			for _, f := range unique {
				if o.del(f) {
					n++
				}
			}
			return n > 0, n, nil
		})
		if err != nil {
			return 0, fmt.Errorf("lytecache: hdel %q: %w", key, err)
		}
		return r.(int), nil
	}
	now := nowMillis()
	removed := 0
	err := c.inTx(func(tx *sql.Tx) error {
		removeExpr := `json_remove(` + hashDoc + `, ` + hashPath("f0") + `)`
		for _, f := range unique {
			// One HDel call is one access, however many statements it takes.
			bump := 0
			if removed == 0 {
				bump = 1
			}
			res, err := tx.Exec(`UPDATE cache SET value = CAST(`+removeExpr+` AS BLOB), size_bytes = length(CAST(`+removeExpr+` AS BLOB)),
  last_accessed = @now, access_count = access_count + @bump
WHERE namespace = @ns AND key = @key AND `+hashLive+` AND `+hashFieldType("f0")+` IS NOT NULL`,
				sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now), sql.Named("f0", f), sql.Named("bump", bump))
			if err != nil {
				return err
			}
			removed += int(rowsAffected(res))
		}
		if removed == 0 {
			return c.checkHashType(tx, key, now)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("lytecache: hdel %q: %w", key, err)
	}
	if removed > 0 {
		c.changed(opSet, []string{key}, 1)
	}
	return removed, nil
}

// HExists reports whether key is a live JSON object containing field.
func (c *Cache) HExists(key, field string) (bool, error) {
	vals, found, err := c.hmget(key, []string{field})
	if err != nil || !found {
		return false, err
	}
	return vals[0] != nil, nil
}

// HKeys returns the object's field names in document order (nil if missing).
func (c *Cache) HKeys(key string) ([]string, error) {
	entries, err := c.hashEntries(key, false)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.key
	}
	return out, nil
}

// HVals returns the object's field values as JSON text, in document order.
func (c *Cache) HVals(key string) ([]json.RawMessage, error) {
	entries, err := c.hashEntries(key, true)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, len(entries))
	for i, e := range entries {
		out[i] = e.value
	}
	return out, nil
}

// HLen returns the number of fields in the object at key (0 if missing).
func (c *Cache) HLen(key string) (int, error) {
	now := nowMillis()
	if !c.json1 {
		o, err := c.loadHash(c.readDB, key, now)
		if err != nil {
			return 0, fmt.Errorf("lytecache: hlen %q: %w", key, err)
		}
		if o == nil {
			return 0, nil
		}
		c.bufferLRU(key, now)
		return len(o.fields), nil
	}
	var vt int
	var root sql.NullString
	var n sql.NullInt64
	err := c.readDB.QueryRow(`SELECT cache.value_type, `+hashRootType+`,
  CASE WHEN `+hashIsObj+` THEN (SELECT count(*) FROM json_each(`+hashDoc+`)) END
FROM cache WHERE namespace = @ns AND key = @key AND `+hashLive,
		sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now)).Scan(&vt, &root, &n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("lytecache: hlen %q: %w", key, err)
	}
	if vt != typeJSON || root.String != "object" {
		return 0, wrongTypeError(key, vt, root)
	}
	c.bufferLRU(key, now)
	return int(n.Int64), nil
}

func (c *Cache) hashEntries(key string, withValues bool) ([]jsonField, error) {
	now := nowMillis()
	if !c.json1 {
		o, err := c.loadHash(c.readDB, key, now)
		if err != nil {
			return nil, fmt.Errorf("lytecache: hkeys %q: %w", key, err)
		}
		if o == nil {
			return nil, nil
		}
		c.bufferLRU(key, now)
		return o.fields, nil
	}
	valueCol := `NULL`
	if withValues {
		valueCol = `CASE WHEN j.key IS NOT NULL THEN ` + hashDoc + ` -> j.fullkey END`
	}
	rows, err := c.readDB.Query(`SELECT cache.value_type, `+hashRootType+`, j.key, `+valueCol+` FROM cache
LEFT JOIN json_each(CASE WHEN `+hashIsObj+` THEN `+hashDoc+` ELSE '{}' END) AS j
WHERE cache.namespace = @ns AND cache.key = @key AND `+hashLive,
		sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now))
	if err != nil {
		return nil, fmt.Errorf("lytecache: hkeys %q: %w", key, err)
	}
	defer func() { _ = rows.Close() }()
	var out []jsonField
	seen := false
	for rows.Next() {
		var vt int
		var root, k, v sql.NullString
		if err := rows.Scan(&vt, &root, &k, &v); err != nil {
			return nil, fmt.Errorf("lytecache: hkeys %q: %w", key, err)
		}
		if !seen && (vt != typeJSON || root.String != "object") {
			return nil, wrongTypeError(key, vt, root)
		}
		seen = true
		if k.Valid {
			out = append(out, jsonField{key: k.String, value: json.RawMessage(v.String)})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: hkeys %q: %w", key, err)
	}
	if seen {
		c.bufferLRU(key, now)
	}
	return out, nil
}

// HIncrBy atomically adds amount to an integer field and returns the new
// value. A missing field starts at 0 and a missing key is created (no
// expiry unless a [TTL] option is passed). It returns an error wrapping
// [ErrNotNumeric] if the field holds anything but a JSON integer, and one
// wrapping [ErrOverflow] -- writing nothing -- if the result would leave
// the signed 64-bit range.
func (c *Cache) HIncrBy(key, field string, amount int64, opts ...SetOption) (int64, error) {
	r, err := c.hincr(key, field, amount, applySetOptions(opts), true)
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

// HIncrByFloat atomically adds amount to a numeric (integer or float) field
// and returns the new value, stored as a float. Otherwise like HIncrBy.
func (c *Cache) HIncrByFloat(key, field string, amount float64, opts ...SetOption) (float64, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, fmt.Errorf("%w: hincrbyfloat amount must be finite", ErrSerialization)
	}
	r, err := c.hincr(key, field, amount, applySetOptions(opts), false)
	if err != nil {
		return 0, err
	}
	return r.(float64), nil
}

func (c *Cache) hincr(key, field string, amount any, so setOptions, integer bool) (any, error) {
	kind := "a number"
	if integer {
		kind = "an integer"
	}
	if !c.json1 {
		r, err := c.rmwHash(key, so, true, func(o *jsonObject) (bool, any, error) {
			raw, ok := o.get(field)
			if !ok {
				raw = json.RawMessage("0")
			}
			if integer {
				cur, err := parseJSONInt(raw)
				if err != nil {
					return false, nil, fmt.Errorf("%w: hash field %q of key %q is not %s", ErrNotNumeric, field, key, kind)
				}
				a := amount.(int64)
				if (a > 0 && cur > math.MaxInt64-a) || (a < 0 && cur < math.MinInt64-a) {
					return false, nil, fmt.Errorf("%w: incrementing field %q of key %q", ErrOverflow, field, key)
				}
				o.set(field, json.RawMessage(strconv.FormatInt(cur+a, 10)))
				return true, cur + a, nil
			}
			cur, err := parseJSONNumber(raw)
			if err != nil {
				return false, nil, fmt.Errorf("%w: hash field %q of key %q is not %s", ErrNotNumeric, field, key, kind)
			}
			next := cur + amount.(float64)
			if math.IsInf(next, 0) {
				return false, nil, fmt.Errorf("%w: incrementing field %q of key %q", ErrOverflow, field, key)
			}
			o.set(field, json.RawMessage(formatJSONFloat(next)))
			return true, next, nil
		})
		if err != nil {
			return nil, fmt.Errorf("lytecache: hincrby %q: %w", key, err)
		}
		return r, nil
	}

	if err := c.capacityCheck(key); err != nil {
		return nil, err
	}
	now := nowMillis()
	args := append(c.hashArgs(key, now, so), sql.Named("f0", field), sql.Named("amount", amount))
	stored := `json_extract(` + hashDoc + `, ` + hashPath("f0") + `)`
	var newVal, guard string
	if integer {
		newVal = `(COALESCE(` + stored + `, 0) + @amount)`
		guard = `COALESCE(` + hashFieldType("f0") + `, 'integer') = 'integer' AND typeof(` + newVal + `) = 'integer'`
	} else {
		newVal = `(CAST(COALESCE(` + stored + `, 0) AS REAL) + @amount)`
		guard = `COALESCE(` + hashFieldType("f0") + `, 'real') IN ('integer', 'real') AND ` + newVal +
			` BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308`
	}
	storedVal := newVal
	if !integer {
		storedVal = `json(` + jsonFloatText(newVal) + `)`
	}
	q := upsertHashSQL(`json_set(`+hashDoc+`, `+hashPath("f0")+`, `+storedVal+`)`, `json_object(@f0, @amount)`,
		`(CASE WHEN `+hashIsObj+` THEN (`+guard+`) ELSE 0 END)`) +
		` RETURNING json_extract(CAST(value AS TEXT), ` + hashPath("f0") + `)`
	var result any
	err := c.inTx(func(tx *sql.Tx) error {
		var r sql.NullString
		err := tx.QueryRow(q, args...).Scan(&r)
		if err == nil {
			if integer {
				n, perr := strconv.ParseInt(r.String, 10, 64)
				result = n
				return perr
			}
			f, perr := strconv.ParseFloat(r.String, 64)
			result = f
			return perr
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := c.checkHashType(tx, key, now); err != nil {
			return err
		}
		var ftype sql.NullString
		if err := tx.QueryRow(`SELECT `+hashFieldType("f0")+` FROM cache WHERE namespace = @ns AND key = @key`, args...).Scan(&ftype); err != nil {
			return err
		}
		if (integer && ftype.String == "integer") || (!integer && (ftype.String == "integer" || ftype.String == "real")) {
			return fmt.Errorf("%w: incrementing field %q of key %q", ErrOverflow, field, key)
		}
		return fmt.Errorf("%w: hash field %q of key %q holds a JSON %s, not %s", ErrNotNumeric, field, key, ftype.String, kind)
	})
	if err != nil {
		return nil, fmt.Errorf("lytecache: hincrby %q: %w", key, err)
	}
	c.changed(opSet, []string{key}, 1)
	c.maybeEvict()
	return result, nil
}

func (c *Cache) recordHashRead(key string, now int64, found bool) {
	if found {
		c.hits.Add(1)
		c.bufferLRU(key, now)
	} else {
		c.misses.Add(1)
	}
	c.maybeOpportunisticSweep()
}

// inTx runs fn in one write transaction on the (single) write connection.
// Its first statement is always a write, so the write lock is taken up
// front and there is no read-to-write upgrade to fail.
func (c *Cache) inTx(fn func(tx *sql.Tx) error) error {
	tx, err := c.writeDB.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// immediateConn adapts a *sql.Conn to querier.
type immediateConn struct {
	conn *sql.Conn
	ctx  context.Context
}

func (ic immediateConn) QueryRow(q string, args ...any) *sql.Row {
	return ic.conn.QueryRowContext(ic.ctx, q, args...)
}

func (ic immediateConn) Exec(q string, args ...any) (sql.Result, error) {
	return ic.conn.ExecContext(ic.ctx, q, args...)
}

// inImmediate runs fn under BEGIN IMMEDIATE -- the write lock is held from
// the first read -- for the read-modify-write fallback.
func (c *Cache) inImmediate(fn func(q querier) error) error {
	ctx := context.Background()
	conn, err := c.writeDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	if err := fn(immediateConn{conn, ctx}); err != nil {
		_, _ = conn.ExecContext(ctx, `ROLLBACK`)
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}

// loadHash (fallback path, and HGetAll) returns the live object at key, or
// nil if the key is missing or expired.
func (c *Cache) loadHash(q querier, key string, now int64) (*jsonObject, error) {
	var data []byte
	var vt int
	err := q.QueryRow(`SELECT value, value_type FROM cache WHERE namespace = @ns AND key = @key AND `+hashLive,
		sql.Named("ns", c.namespace), sql.Named("key", key), sql.Named("now", now)).Scan(&data, &vt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if vt != typeJSON {
		return nil, wrongTypeError(key, vt, sql.NullString{})
	}
	o, root, err := parseJSONObject(data)
	if err != nil {
		return nil, wrongTypeError(key, typeJSON, sql.NullString{})
	}
	if o == nil {
		return nil, wrongTypeError(key, typeJSON, sql.NullString{String: root, Valid: true})
	}
	return o, nil
}

// rmwHash is the fallback: read, mutate in Go, write back, all under
// BEGIN IMMEDIATE. mutate returns (changed, result, err).
func (c *Cache) rmwHash(key string, so setOptions, create bool, mutate func(*jsonObject) (bool, any, error)) (any, error) {
	now := nowMillis()
	var result any
	wrote := false
	err := c.inImmediate(func(q querier) error {
		o, err := c.loadHash(q, key, now)
		if err != nil {
			return err
		}
		exists := o != nil
		if !exists {
			o = &jsonObject{}
			if create && c.eviction == NoEviction && (c.maxKeys != nil || c.maxBytes != nil) {
				if err := c.checkCapacityIn(q, key); err != nil {
					return err
				}
			}
		}
		changed, r, err := mutate(o)
		result = r
		if err != nil || !changed || (!exists && !create) {
			return err
		}
		blob := o.marshal()
		if exists {
			_, err = q.Exec(`UPDATE cache SET value = @v, size_bytes = @size, last_accessed = @now, access_count = access_count + 1
WHERE namespace = @ns AND key = @key`, sql.Named("v", blob), sql.Named("size", len(blob)), sql.Named("now", now),
				sql.Named("ns", c.namespace), sql.Named("key", key))
		} else {
			_, err = q.Exec(`INSERT OR REPLACE INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES (@key, @ns, @v, 4, @now, @exp, @now, 0, @size)`, sql.Named("key", key), sql.Named("ns", c.namespace), sql.Named("v", blob),
				sql.Named("now", now), sql.Named("exp", expiresAtArg(so.ttl, now)), sql.Named("size", len(blob)))
		}
		wrote = err == nil
		return err
	})
	if err != nil {
		return nil, err
	}
	if wrote {
		c.changed(opSet, []string{key}, 1)
		c.maybeEvict()
	}
	return result, nil
}

// checkCapacityIn is checkCapacity run on the connection that holds the
// write lock, so the check and the write see the same state.
func (c *Cache) checkCapacityIn(q querier, _ string) error {
	var count int64
	var total sql.NullInt64
	if err := q.QueryRow(`SELECT COUNT(*), SUM(size_bytes) FROM cache WHERE namespace = @ns`, sql.Named("ns", c.namespace)).
		Scan(&count, &total); err != nil {
		return err
	}
	if c.maxKeys != nil && count >= *c.maxKeys {
		return fmt.Errorf("%w: namespace %q has reached its %d-key limit", ErrCacheFull, c.namespace, *c.maxKeys)
	}
	if c.maxBytes != nil && total.Valid && total.Int64 >= *c.maxBytes {
		return fmt.Errorf("%w: namespace %q has reached its %d-byte limit", ErrCacheFull, c.namespace, *c.maxBytes)
	}
	return nil
}

// HashMode reports which hash implementation this Cache uses: "json1"
// (single SQL statements) or "fallback" (read-modify-write under BEGIN
// IMMEDIATE, when the linked SQLite lacks JSON1).
func (c *Cache) HashMode() string {
	if c.json1 {
		return "json1"
	}
	return "fallback"
}

// -- order-preserving JSON object, for the fallback path ---------------------

type jsonField struct {
	key   string
	value json.RawMessage
}

// jsonObject keeps field order and each value's exact JSON text (so big
// integers and number formatting survive a round trip), unlike a Go map.
type jsonObject struct{ fields []jsonField }

func (o *jsonObject) index(k string) int {
	for i, f := range o.fields {
		if f.key == k {
			return i
		}
	}
	return -1
}

func (o *jsonObject) has(k string) bool { return o.index(k) >= 0 }

func (o *jsonObject) get(k string) (json.RawMessage, bool) {
	if i := o.index(k); i >= 0 {
		return o.fields[i].value, true
	}
	return nil, false
}

func (o *jsonObject) set(k string, v json.RawMessage) {
	if i := o.index(k); i >= 0 {
		o.fields[i].value = v
		return
	}
	o.fields = append(o.fields, jsonField{k, v})
}

func (o *jsonObject) del(k string) bool {
	i := o.index(k)
	if i < 0 {
		return false
	}
	o.fields = append(o.fields[:i], o.fields[i+1:]...)
	return true
}

func (o *jsonObject) marshal() []byte {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range o.fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		k, _ := encodeHashValue(f.key)
		buf.WriteString(k)
		buf.WriteByte(':')
		_ = json.Compact(&buf, f.value)
	}
	buf.WriteByte('}')
	return buf.Bytes()
}

// parseJSONObject returns the object, or (nil, rootType, nil) for a valid
// non-object root, or an error for malformed JSON.
func parseJSONObject(data []byte) (*jsonObject, string, error) {
	if !json.Valid(data) {
		return nil, "", fmt.Errorf("malformed JSON")
	}
	trimmed := bytes.TrimSpace(data)
	switch trimmed[0] {
	case '[':
		return nil, "array", nil
	case '"':
		return nil, "text", nil
	case 't':
		return nil, "true", nil
	case 'f':
		return nil, "false", nil
	case 'n':
		return nil, "null", nil
	case '{':
	default:
		return nil, "number", nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	if _, err := dec.Token(); err != nil {
		return nil, "", err
	}
	o := &jsonObject{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, "", err
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, "", err
		}
		o.set(tok.(string), raw)
	}
	return o, "object", nil
}

func parseJSONInt(raw json.RawMessage) (int64, error) {
	s := strings.TrimSpace(string(raw))
	if strings.ContainsAny(s, ".eE") {
		return 0, fmt.Errorf("not an integer")
	}
	return strconv.ParseInt(s, 10, 64)
}

func parseJSONNumber(raw json.RawMessage) (float64, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) {
		return 0, fmt.Errorf("not a number")
	}
	return strconv.ParseFloat(s, 64)
}

// formatJSONFloat renders a float the way SQLite's json_set does: always
// with a fractional part or exponent, so it reads back as a float.
func formatJSONFloat(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

func dedupe(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, s := range items {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
