package lytecache_test

import (
	"database/sql"
	"testing"

	lytecache "github.com/lytecache/lytecache-go"
)

func TestNamespacesReportsEveryNamespaceInFile(t *testing.T) {
	path := tempDBPath(t)

	a, err := lytecache.New(lytecache.WithPath(path), lytecache.WithNamespace("ns-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if err := a.SetMany(map[string]any{"k1": "v", "k2": "v"}); err != nil {
		t.Fatal(err)
	}

	b, err := lytecache.New(lytecache.WithPath(path), lytecache.WithNamespace("ns-b"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if err := b.Set("k1", "v"); err != nil {
		t.Fatal(err)
	}

	// Namespaces is called against `a`, which is itself pinned to "ns-a" --
	// it must still report "ns-b" too, since it enumerates the whole file.
	infos, err := a.Namespaces()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]lytecache.NamespaceInfo, len(infos))
	for _, info := range infos {
		byName[info.Namespace] = info
	}

	if got := byName["ns-a"].KeyCount; got != 2 {
		t.Errorf("ns-a KeyCount = %d, want 2", got)
	}
	if got := byName["ns-b"].KeyCount; got != 1 {
		t.Errorf("ns-b KeyCount = %d, want 1", got)
	}
	if byName["ns-a"].SizeBytes <= 0 {
		t.Errorf("ns-a SizeBytes = %d, want > 0", byName["ns-a"].SizeBytes)
	}
}

func TestNamespacesCountsExpiredPresent(t *testing.T) {
	c := newTestCache(t)
	if err := c.Set("live", "v"); err != nil {
		t.Fatal(err)
	}
	if err := c.Set("stale", "v", lytecache.TTL(0)); err != nil {
		t.Fatal(err)
	}

	infos, err := c.Namespaces()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 namespace, got %d", len(infos))
	}
	if infos[0].KeyCount != 2 {
		t.Errorf("KeyCount = %d, want 2 (expired row not yet swept still counts)", infos[0].KeyCount)
	}
	if infos[0].ExpiredPresent != 1 {
		t.Errorf("ExpiredPresent = %d, want 1", infos[0].ExpiredPresent)
	}
}

func TestStatsReportsExpiredPresent(t *testing.T) {
	c := newTestCache(t)
	if err := c.Set("stale", "v", lytecache.TTL(0)); err != nil {
		t.Fatal(err)
	}

	stats, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ExpiredPresent != 1 {
		t.Errorf("ExpiredPresent = %d, want 1", stats.ExpiredPresent)
	}
}

// TestNamespacesAndStatsNeverMutate is the property that justifies adding
// these as new methods instead of reusing Get/Inspect: a dashboard poll or
// Prometheus scrape must never perturb the cache it's reading. Get/Inspect
// both opportunistically delete expired rows the moment they see them, and
// every write bumps last_accessed/access_count -- Namespaces/Stats must do
// neither, even when they observe an expired-but-unswept row.
func TestNamespacesAndStatsNeverMutate(t *testing.T) {
	path := tempDBPath(t)
	c, err := lytecache.New(lytecache.WithPath(path), lytecache.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Set("stale", "v", lytecache.TTL(0)); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()

	before, err := rawRowState(raw, "stale")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.Namespaces(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stats(); err != nil {
		t.Fatal(err)
	}

	after, err := rawRowState(raw, "stale")
	if err != nil {
		t.Fatalf("row was deleted by a supposedly read-only call: %v", err)
	}
	if before != after {
		t.Errorf("row state changed after Namespaces()/Stats(): before=%v after=%v", before, after)
	}
}

type rowState struct {
	lastAccessed int64
	accessCount  int64
}

func rawRowState(db *sql.DB, key string) (rowState, error) {
	var s rowState
	err := db.QueryRow(`SELECT last_accessed, access_count FROM cache WHERE namespace = 'default' AND key = ?`, key).
		Scan(&s.lastAccessed, &s.accessCount)
	return s, err
}

func TestSchemaVersionOnFreshFile(t *testing.T) {
	c := newTestCache(t)
	if got := c.SchemaVersion(); got != 1 {
		t.Errorf("SchemaVersion() = %d, want 1", got)
	}
}

func TestLimitsUnsetByDefault(t *testing.T) {
	c := newTestCache(t)
	l := c.Limits()
	if l.MaxKeys != nil || l.MaxBytes != nil {
		t.Errorf("Limits() = %+v, want both nil", l)
	}
}

func TestLimitsReflectsConfiguredCaps(t *testing.T) {
	c := newTestCache(t, lytecache.WithMaxKeys(10), lytecache.WithMaxBytes(1024))
	l := c.Limits()
	if l.MaxKeys == nil || *l.MaxKeys != 10 {
		t.Errorf("MaxKeys = %v, want 10", l.MaxKeys)
	}
	if l.MaxBytes == nil || *l.MaxBytes != 1024 {
		t.Errorf("MaxBytes = %v, want 1024", l.MaxBytes)
	}

	// Mutating the returned pointers must not reach back into the Cache's
	// own state -- Limits() hands out copies, not internal pointers.
	*l.MaxKeys = 999
	l2 := c.Limits()
	if *l2.MaxKeys != 10 {
		t.Errorf("Limits() leaked a mutable internal pointer: got %d after external mutation", *l2.MaxKeys)
	}
}
