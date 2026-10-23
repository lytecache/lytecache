package lytecache_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	lytecache "github.com/lytecache/lytecache-go"
)

func TestMatchChannelAgreesWithWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	c := newEventsCache(t, path)
	channels := []string{"orders", "orders:paid", "orders*", "ordersx", "payments", "naïve:é", "a*b"}
	for _, ch := range channels {
		// '*' is not publishable, so seed the literal-star channels directly.
		if _, err := c.Notify(ch, nil); err != nil {
			if !errors.Is(err, lytecache.ErrInvalidArgument) {
				t.Fatal(err)
			}
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, ch := range []string{"orders*", "a*b"} {
		if _, err := db.Exec(`INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES (?, 'default', NULL, 4, ?)`,
			ch, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}

	all, err := c.RecentEvents(100, -1)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"orders", "orders*", "orders:*", "*", "naïve:*", "a*b", "a*", "nope", "or*ders"} {
		got, err := c.RecentEvents(100, -1, p)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, e := range all {
			if lytecache.MatchChannel(p, e.Channel) {
				want = append(want, e.Channel)
			}
		}
		var gotCh []string
		for _, e := range got {
			gotCh = append(gotCh, e.Channel)
		}
		if len(gotCh) != len(want) {
			t.Fatalf("pattern %q: SQL matched %v, MatchChannel matched %v", p, gotCh, want)
		}
		for i := range want {
			if gotCh[i] != want[i] {
				t.Fatalf("pattern %q: SQL matched %v, MatchChannel matched %v", p, gotCh, want)
			}
		}
	}
	if lytecache.MatchChannel("", "x") {
		t.Fatal("empty pattern matched")
	}
	if got, _ := c.RecentEvents(100, -1, "orders", "payments"); len(got) != 2 {
		t.Fatalf("two patterns: got %d events, want 2", len(got))
	}
}

func TestAdminReadsDoNotCreateEventsTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	c := newEventsCache(t, path)
	if s, err := c.EventChannelStats(); err != nil || s != nil {
		t.Fatalf("EventChannelStats = %v, %v", s, err)
	}
	if n, err := c.EventCountSince(time.Time{}); err != nil || n != 0 {
		t.Fatalf("EventCountSince = %d, %v", n, err)
	}
	if ok, err := c.DeleteEvent(1); err != nil || ok {
		t.Fatalf("DeleteEvent = %v, %v", ok, err)
	}
	if n, err := c.DeleteEventsBefore(time.Now()); err != nil || n != 0 {
		t.Fatalf("DeleteEventsBefore = %d, %v", n, err)
	}
	if n, err := c.DeleteChannelEvents("x"); err != nil || n != 0 {
		t.Fatalf("DeleteChannelEvents = %d, %v", n, err)
	}
	if info, _ := c.EventStats(); info.Enabled {
		t.Fatal("an admin read created the events table")
	}
}

func TestEventChannelStatsAndCountSince(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	c := newEventsCache(t, path)
	for _, ch := range []string{"b", "a", "b", "b"} {
		if _, err := c.Notify(ch, "xy"); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := c.EventChannelStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 || stats[0].Channel != "a" || stats[0].Count != 1 || stats[1].Count != 3 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats[1].OldestSeq != 1 || stats[1].NewestSeq != 4 || stats[0].PayloadBytes == 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if n, _ := c.EventCountSince(time.Now().Add(-time.Minute)); n != 4 {
		t.Fatalf("EventCountSince(-1m) = %d, want 4", n)
	}
	if n, _ := c.EventCountSince(time.Now().Add(time.Minute)); n != 0 {
		t.Fatalf("EventCountSince(+1m) = %d, want 0", n)
	}
}

func TestCursorsAcrossNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	def := newEventsCache(t, path)
	tenant := newEventsCache(t, path, lytecache.WithNamespace("tenant:eu"))
	if err := tenant.Set("k", 1); err != nil { // makes "tenant:eu" a known namespace
		t.Fatal(err)
	}
	if err := def.SaveCursor("worker-b", 7); err != nil {
		t.Fatal(err)
	}
	if err := tenant.SaveCursor("invalidate:main", 3); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO meta (k, v) VALUES ('cursor:default:broken', 'x')`); err != nil {
		t.Fatal(err)
	}

	got, err := def.Cursors()
	if err != nil {
		t.Fatal(err)
	}
	want := []lytecache.CursorInfo{
		{Namespace: "default", Name: "worker-b", Seq: 7},
		{Namespace: "tenant:eu", Name: "invalidate:main", Seq: 3},
	}
	if len(got) != len(want) {
		t.Fatalf("Cursors = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Cursors = %+v, want %+v", got, want)
		}
	}
}

func TestDeleteEventsBeforeKeepsAPrefixAndRaisesGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	c := newEventsCache(t, path)
	for i := 0; i < 5; i++ {
		if _, err := c.Notify("jobs", i); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// Backdate seqs 1-3; give seq 2 a later clock to prove the purge is a
	// prefix by seq, not a created_at filter that would leave a hole.
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	if _, err := db.Exec(`UPDATE events SET created_at = ? WHERE seq IN (1, 3)`, old); err != nil {
		t.Fatal(err)
	}
	n, err := c.DeleteEventsBefore(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d, want 3 (seqs 1-3)", n)
	}
	if info, _ := c.EventStats(); info.OldestSeq != 4 || info.NewestSeq != 5 {
		t.Fatalf("after purge: %+v", info)
	}
	sub := watch(t, c, []string{"jobs"}, lytecache.FromSeq(1))
	if _, _, err := sub.TryNext(); !errors.Is(err, lytecache.ErrGap) {
		t.Fatalf("resume inside the purged prefix: err = %v, want ErrGap", err)
	}
}

func TestDeleteEventAndChannel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	c := newEventsCache(t, path)
	for _, ch := range []string{"a", "b", "a", "a"} {
		if _, err := c.Notify(ch, nil); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := c.DeleteEvent(2); err != nil || !ok {
		t.Fatalf("DeleteEvent(2) = %v, %v", ok, err)
	}
	if ok, _ := c.DeleteEvent(2); ok {
		t.Fatal("DeleteEvent(2) twice reported a deletion")
	}
	if n, err := c.DeleteChannelEvents("a"); err != nil || n != 3 {
		t.Fatalf("DeleteChannelEvents(a) = %d, %v", n, err)
	}
	if _, err := c.DeleteChannelEvents(""); !errors.Is(err, lytecache.ErrInvalidArgument) {
		t.Fatalf("empty channel: err = %v", err)
	}
	info, _ := c.EventStats()
	if info.Count != 0 || info.NewestSeq != 4 {
		t.Fatalf("after purge: %+v (seq must keep counting)", info)
	}
	if seq, _ := c.Notify("a", nil); seq != 5 {
		t.Fatalf("next seq = %d, want 5", seq)
	}
}
