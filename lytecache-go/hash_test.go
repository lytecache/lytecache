package lytecache_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	lytecache "github.com/lytecache/lytecache-go"
)

var weirdFields = []string{"a.b", "a[0]", "a]b", `q"uote`, `back\slash`, `\"`, "sp ace", "ünï 字", "", "$", "*"}

// forEachMode runs fn against the JSON1 path and the forced fallback, so
// both implementations are proven equivalent.
func forEachMode(t *testing.T, fn func(t *testing.T, c *lytecache.Cache, path string)) {
	for _, mode := range []string{"json1", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "fallback" {
				t.Setenv("LYTECACHE_FORCE_JSON_FALLBACK", "1")
			}
			path := tempDBPath(t)
			c, err := lytecache.New(lytecache.WithPath(path), lytecache.WithSweepInterval(0))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			if c.HashMode() != mode {
				t.Fatalf("HashMode = %s, want %s", c.HashMode(), mode)
			}
			fn(t, c, path)
		})
	}
}

type rawRow struct {
	value       []byte
	valueType   int
	expiresAt   sql.NullInt64
	sizeBytes   int64
	length      int64
	lastAcc     int64
	accessCount int64
}

func readRaw(t *testing.T, path, key string) (rawRow, bool) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var r rawRow
	err = db.QueryRow(`SELECT value, value_type, expires_at, size_bytes, length(value), last_accessed, access_count
FROM cache WHERE namespace = 'default' AND key = ?`, key).
		Scan(&r.value, &r.valueType, &r.expiresAt, &r.sizeBytes, &r.length, &r.lastAcc, &r.accessCount)
	if errors.Is(err, sql.ErrNoRows) {
		return rawRow{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return r, true
}

func insertRaw(t *testing.T, path, key string, blob []byte, vt int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	now := time.Now().UnixMilli()
	if _, err := db.Exec(`INSERT INTO cache (namespace, key, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
VALUES ('default', ?, ?, ?, ?, NULL, ?, 0, ?)`, key, blob, vt, now, now, len(blob)); err != nil {
		t.Fatal(err)
	}
}

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func jsonOf(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	if raw == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestHSetHGetRoundTripAndCreatedFlag(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		mb := must[bool](t)
		if !mb(c.HSet("h", "s", "text")) || !mb(c.HSet("h", "n", 42)) || !mb(c.HSet("h", "o", map[string]any{"x": []int{1}})) {
			t.Fatal("first writes should report created")
		}
		if mb(c.HSet("h", "s", "again")) {
			t.Fatal("replacing should report created=false")
		}
		s, found, err := c.HGetString("h", "s")
		if err != nil || !found || s != "again" {
			t.Fatalf("HGetString = %q %v %v", s, found, err)
		}
		n, _, _ := c.HGetInt64("h", "n")
		if n != 42 {
			t.Fatalf("n = %d", n)
		}
		var o struct{ X []int }
		if found, err := c.HGet("h", "o", &o); err != nil || !found || len(o.X) != 1 {
			t.Fatalf("HGet struct = %+v %v %v", o, found, err)
		}
		var doc map[string]any
		if _, err := c.Get("h", &doc); err != nil || doc["s"] != "again" {
			t.Fatalf("Get on a hash = %v %v", doc, err)
		}
	})
}

func TestHashIsAViewOverJSONValues(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		if err := c.Set("u", map[string]any{"name": "Ada"}); err != nil {
			t.Fatal(err)
		}
		if s, _, _ := c.HGetString("u", "name"); s != "Ada" {
			t.Fatalf("got %q", s)
		}
		must[bool](t)(c.HSet("u", "status", "active"))
		var doc map[string]string
		_, _ = c.Get("u", &doc)
		if doc["name"] != "Ada" || doc["status"] != "active" {
			t.Fatalf("doc = %v", doc)
		}
	})
}

func TestHSetManyHMGetHDelHKeysHVals(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		if err := c.HSetMany("h", map[string]any{"a": 1, "b": "two", "c": nil}); err != nil {
			t.Fatal(err)
		}
		vals, err := c.HMGet("h", []string{"b", "nope", "a"})
		if err != nil || jsonOf(t, vals[0]) != "two" || vals[1] != nil || jsonOf(t, vals[2]) != float64(1) {
			t.Fatalf("HMGet = %s %v", vals, err)
		}
		if keys, _ := c.HKeys("h"); len(keys) != 3 {
			t.Fatalf("HKeys = %v", keys)
		}
		if n, _ := c.HLen("h"); n != 3 {
			t.Fatalf("HLen = %d", n)
		}
		if ok, _ := c.HExists("h", "c"); !ok {
			t.Fatal("a stored null is a present field")
		}
		if n, _ := c.HDel("h", "a", "nope", "a"); n != 1 {
			t.Fatalf("HDel = %d", n)
		}
		if n, _ := c.HDel("h", "b", "c"); n != 2 {
			t.Fatalf("HDel = %d", n)
		}
		var doc map[string]any
		if found, _ := c.Get("h", &doc); !found || len(doc) != 0 {
			t.Fatalf("emptied hash should remain as {}: %v %v", doc, found)
		}
	})
}

func TestHSetManyBeyondSQLiteLimits(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		m := map[string]any{}
		for i := 0; i < 1234; i++ {
			m["f"+strconv.Itoa(i)] = i
		}
		if err := c.HSetMany("big", m); err != nil {
			t.Fatal(err)
		}
		if n, _ := c.HLen("big"); n != 1234 {
			t.Fatalf("HLen = %d", n)
		}
		if v, _, _ := c.HGetInt64("big", "f777"); v != 777 {
			t.Fatalf("f777 = %d", v)
		}
	})
}

func TestHSetNX(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		mb := must[bool](t)
		if !mb(c.HSetNX("h", "a", 1)) || mb(c.HSetNX("h", "a", 2)) {
			t.Fatal("HSetNX semantics")
		}
		if v, _, _ := c.HGetInt64("h", "a"); v != 1 {
			t.Fatalf("a = %d", v)
		}
	})
}

func TestHashReadsOnMissingKeyCreateNothing(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		if _, found, err := c.HGetRaw("missing", "f"); found || err != nil {
			t.Fatal("miss")
		}
		if m, err := c.HGetAll("missing"); err != nil || len(m) != 0 {
			t.Fatal("HGetAll miss")
		}
		if n, _ := c.HLen("missing"); n != 0 {
			t.Fatal("HLen miss")
		}
		if n, _ := c.HDel("missing", "f"); n != 0 {
			t.Fatal("HDel miss")
		}
		if k, _ := c.HKeys("missing"); len(k) != 0 {
			t.Fatal("HKeys miss")
		}
		if _, ok := readRaw(t, path, "missing"); ok {
			t.Fatal("a read created a row")
		}
	})
}

func TestHashMissingKeyCreationAndTTL(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		must[bool](t)(c.HSet("plain", "f", 1))
		if _, has, _, _ := c.TTLOf("plain"); has {
			t.Fatal("created hash should have no expiry")
		}
		must[bool](t)(c.HSet("ttl", "f", 1, lytecache.TTL(100*time.Second)))
		before, has, _, _ := c.TTLOf("ttl")
		if !has {
			t.Fatal("TTL option should apply on creation")
		}
		must[bool](t)(c.HSet("ttl", "g", 2, lytecache.TTL(5*time.Second))) // ignored: key exists
		after, _, _, _ := c.TTLOf("ttl")
		if after < 50*time.Second || after > before {
			t.Fatalf("existing key's TTL changed: %v -> %v", before, after)
		}
	})
}

func TestHashExpiredKeyIsNotResurrected(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		if err := c.Set("h", map[string]any{"old": 1}, lytecache.TTL(10*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
		if _, found, _ := c.HGetRaw("h", "old"); found {
			t.Fatal("expired field readable")
		}
		if !must[bool](t)(c.HSetNX("h", "new", 1)) {
			t.Fatal("expired key should behave as missing")
		}
		all, _ := c.HGetAll("h")
		if len(all) != 1 || all["new"] == nil {
			t.Fatalf("old fields resurrected: %s", all)
		}
		if _, has, _, _ := c.TTLOf("h"); has {
			t.Fatal("stale expiry carried over")
		}
		_ = c.Set("n", map[string]any{"n": 100}, lytecache.TTL(10*time.Millisecond))
		time.Sleep(40 * time.Millisecond)
		if v := must[int64](t)(c.HIncrBy("n", "n", 1)); v != 1 {
			t.Fatalf("HIncrBy on expired = %d", v)
		}
	})
}

func TestHashTypeGuards(t *testing.T) {
	cases := []struct {
		blob []byte
		vt   int
	}{{[]byte{0, 1}, 0}, {[]byte("hello"), 1}, {[]byte("42"), 2}, {[]byte("3.14"), 3}, {[]byte{0x80, 4}, 5}, {[]byte{0xac, 0xed}, 6}}
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		for _, tc := range cases {
			key := "k" + strconv.Itoa(tc.vt)
			insertRaw(t, path, key, tc.blob, tc.vt)
			ops := []func() error{
				func() error { _, err := c.HSet(key, "f", 1); return err },
				func() error { return c.HSetMany(key, map[string]any{"f": 1}) },
				func() error { _, err := c.HSetNX(key, "f", 1); return err },
				func() error { _, err := c.HIncrBy(key, "f", 1); return err },
				func() error { _, err := c.HIncrByFloat(key, "f", 1); return err },
				func() error { _, err := c.HDel(key, "f"); return err },
				func() error { _, _, err := c.HGetRaw(key, "f"); return err },
				func() error { _, err := c.HGetAll(key); return err },
				func() error { _, err := c.HKeys(key); return err },
				func() error { _, err := c.HVals(key); return err },
				func() error { _, err := c.HLen(key); return err },
			}
			for i, op := range ops {
				err := op()
				if !errors.Is(err, lytecache.ErrSerialization) {
					t.Fatalf("value_type %d op %d: err = %v", tc.vt, i, err)
				}
			}
			r, _ := readRaw(t, path, key)
			if string(r.value) != string(tc.blob) || r.valueType != tc.vt {
				t.Fatalf("value_type %d row modified", tc.vt)
			}
		}
		_ = c.Set("arr", []int{1, 2})
		if _, err := c.HSet("arr", "f", 1); !errors.Is(err, lytecache.ErrSerialization) {
			t.Fatalf("array root: %v", err)
		}
		insertRaw(t, path, "bad", []byte("{not json"), 4)
		if _, err := c.HSet("bad", "f", 1); !errors.Is(err, lytecache.ErrSerialization) {
			t.Fatalf("malformed: %v", err)
		}
		if _, err := c.HSet("h", "f", []byte("x")); !errors.Is(err, lytecache.ErrSerialization) {
			t.Fatalf("[]byte value: %v", err)
		}
	})
}

func TestHashAwkwardFieldNames(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		for _, f := range weirdFields {
			_ = c.Set("h", map[string]any{"a": map[string]any{"b": "decoy"}, "other": 0})
			if !must[bool](t)(c.HSet("h", f, "v")) {
				t.Fatalf("%q: not created", f)
			}
			if s, found, _ := c.HGetString("h", f); !found || s != "v" {
				t.Fatalf("%q: HGet = %q %v", f, s, found)
			}
			var doc map[string]any
			_, _ = c.Get("h", &doc)
			if doc[f] != "v" || len(doc) != 3 {
				t.Fatalf("%q: doc = %v", f, doc)
			}
			if v := must[int64](t)(c.HIncrBy("h", f+"#n", 2)); v != 2 {
				t.Fatalf("%q: HIncrBy = %d", f, v)
			}
			if n, _ := c.HDel("h", f); n != 1 {
				t.Fatalf("%q: HDel = %d", f, n)
			}
		}
		m := map[string]any{}
		for i, f := range weirdFields {
			m[f] = i
		}
		_ = c.Set("all", map[string]any{})
		if err := c.HSetMany("all", m); err != nil {
			t.Fatal(err)
		}
		all, _ := c.HGetAll("all")
		if len(all) != len(weirdFields) {
			t.Fatalf("HGetAll = %s", all)
		}
		if n, _ := c.HDel("all", weirdFields...); n != len(weirdFields) {
			t.Fatalf("HDel all = %d", n)
		}
	})
}

func TestHashWritesPreserveTTLAndTrackSizeAndAccess(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		_ = c.Set("h", map[string]any{"a": 1}, lytecache.TTL(time.Minute))
		r0, _ := readRaw(t, path, "h")
		time.Sleep(5 * time.Millisecond)
		ops := []func(){
			func() { _, _ = c.HSet("h", "b", "字字字字") },
			func() { _ = c.HSetMany("h", map[string]any{"c": 3, "d": []int{1, 2}}) },
			func() { _, _ = c.HSetNX("h", "e", 4) },
			func() { _, _ = c.HIncrBy("h", "n", 7) },
			func() { _, _ = c.HIncrByFloat("h", "x", 1.25) },
			func() { _, _ = c.HDel("h", "a", "c") },
		}
		for i, op := range ops {
			op()
			r, _ := readRaw(t, path, "h")
			if r.expiresAt != r0.expiresAt {
				t.Fatalf("op %d changed expires_at", i)
			}
			if r.sizeBytes != r.length || r.sizeBytes != int64(len(r.value)) {
				t.Fatalf("op %d: size_bytes %d != length %d", i, r.sizeBytes, r.length)
			}
		}
		r, _ := readRaw(t, path, "h")
		if r.accessCount != r0.accessCount+int64(len(ops)) || r.lastAcc <= r0.lastAcc {
			t.Fatalf("access metadata not bumped: %+v -> %+v", r0, r)
		}
		before := r
		_, _, _ = c.HGetRaw("h", "b")
		_, _ = c.HGetAll("h")
		if after, _ := readRaw(t, path, "h"); after.accessCount != before.accessCount {
			t.Fatal("hash read wrote synchronously")
		}
	})
}

func TestHIncrBy(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		mi := must[int64](t)
		if mi(c.HIncrBy("h", "n", 1)) != 1 || mi(c.HIncrBy("h", "n", 4)) != 5 || mi(c.HIncrBy("h", "n", -10)) != -5 {
			t.Fatal("arithmetic")
		}
		r, _ := readRaw(t, path, "h")
		if string(r.value) != `{"n":-5}` {
			t.Fatalf("integer-ness not preserved: %s", r.value)
		}
		for _, bad := range []any{"5", true, nil, map[string]int{"a": 1}, []int{1}, 1.5} {
			_, _ = c.HSet("h", "bad", bad)
			if _, err := c.HIncrBy("h", "bad", 1); !errors.Is(err, lytecache.ErrNotNumeric) {
				t.Fatalf("HIncrBy on %v: %v", bad, err)
			}
		}
		_, _ = c.HSet("h", "max", int64(math.MaxInt64-1))
		if mi(c.HIncrBy("h", "max", 1)) != math.MaxInt64 {
			t.Fatal("up to max")
		}
		before, _ := readRaw(t, path, "h")
		if _, err := c.HIncrBy("h", "max", 1); !errors.Is(err, lytecache.ErrOverflow) {
			t.Fatalf("overflow: %v", err)
		}
		if after, _ := readRaw(t, path, "h"); string(after.value) != string(before.value) {
			t.Fatal("overflow wrote something")
		}
		_, _ = c.HSet("h", "min", int64(math.MinInt64+1))
		if mi(c.HIncrBy("h", "min", -1)) != math.MinInt64 {
			t.Fatal("down to min")
		}
		if _, err := c.HIncrBy("h", "min", -1); !errors.Is(err, lytecache.ErrOverflow) {
			t.Fatalf("underflow: %v", err)
		}
	})
}

func TestHIncrByFloat(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, _ string) {
		mf := must[float64](t)
		a, b := 0.1, 0.2 // variables: Go would fold the constant 0.1+0.2 to exactly 0.3
		if mf(c.HIncrByFloat("h", "x", a)) != a || mf(c.HIncrByFloat("h", "x", b)) != a+b {
			t.Fatal("float arithmetic / precision")
		}
		_, _ = c.HSet("h", "i", 2)
		if mf(c.HIncrByFloat("h", "i", 0.5)) != 2.5 {
			t.Fatal("int field")
		}
		_, _ = c.HSet("h", "s", "str")
		if _, err := c.HIncrByFloat("h", "s", 1); !errors.Is(err, lytecache.ErrNotNumeric) {
			t.Fatalf("string field: %v", err)
		}
		_, _ = c.HSet("h", "huge", 1.7e308)
		if _, err := c.HIncrByFloat("h", "huge", 1.7e308); !errors.Is(err, lytecache.ErrOverflow) {
			t.Fatalf("float overflow: %v", err)
		}
	})
}

func TestHashCanonicalEncodingAndForeignFormatting(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		_, _ = c.HSet("h", "name", "Adé")
		_, _ = c.HSet("h", "n", 1)
		if r, _ := readRaw(t, path, "h"); string(r.value) != `{"name":"Adé","n":1}` || r.valueType != 4 {
			t.Fatalf("stored %s", r.value)
		}
		insertRaw(t, path, "foreign", []byte("{ \"a\" : 1,\n \"b.c\": [true, null], \"u\": \"\\u00e9\" }"), 4)
		if v, _ := c.HIncrBy("foreign", "a", 1); v != 2 {
			t.Fatalf("a = %d", v)
		}
		if s, _, _ := c.HGetString("foreign", "u"); s != "é" {
			t.Fatalf("u = %q", s)
		}
	})
}

func TestHashConcurrentGoroutinesLoseNothing(t *testing.T) {
	forEachMode(t, func(t *testing.T, c *lytecache.Cache, path string) {
		_ = c.Set("shared", map[string]any{})
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				other, err := lytecache.New(lytecache.WithPath(path), lytecache.WithSweepInterval(0))
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = other.Close() }()
				for i := 0; i < 50; i++ {
					if _, err := other.HSet("shared", "f"+strconv.Itoa(w), i); err != nil {
						t.Error(err)
					}
					if _, err := other.HIncrBy("shared", "total", 1); err != nil {
						t.Error(err)
					}
				}
			}()
		}
		wg.Wait()
		all, _ := c.HGetAll("shared")
		if len(all) != 5 || string(all["total"]) != "200" {
			t.Fatalf("lost updates: %s", all)
		}
	})
}
