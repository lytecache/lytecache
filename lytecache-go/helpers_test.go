package lytecache_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	lytecache "github.com/lytecache/lytecache-go"
)

// newTestCache opens a Cache backed by a fresh temp-dir file, closing it
// automatically at the end of the test.
func newTestCache(t *testing.T, opts ...lytecache.Option) *lytecache.Cache {
	t.Helper()
	allOpts := append([]lytecache.Option{lytecache.WithPath(tempDBPath(t))}, opts...)
	c, err := lytecache.New(allOpts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "cache.db")
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
