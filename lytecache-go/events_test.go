package lytecache_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lytecache "github.com/lytecache/lytecache-go"
)

func newEventsCache(t *testing.T, path string, opts ...lytecache.Option) *lytecache.Cache {
	t.Helper()
	c, err := lytecache.New(append([]lytecache.Option{lytecache.WithPath(path), lytecache.WithSweepInterval(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func watch(t *testing.T, c *lytecache.Cache, patterns []string, opts ...lytecache.WatchOption) *lytecache.Subscription {
	t.Helper()
	s, err := c.Watch(patterns, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func next(t *testing.T, s *lytecache.Subscription) lytecache.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return e
}

func nothing(t *testing.T, s *lytecache.Subscription, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if e, err := s.Next(ctx); err == nil {
		t.Fatalf("unexpected event %+v", e)
	}
}

func payloadInt(t *testing.T, e lytecache.Event) int {
	t.Helper()
	var v int
	if err := e.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func notify(t *testing.T, c *lytecache.Cache, ch string, p any) int64 {
	t.Helper()
	seq, err := c.Notify(ch, p)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func tableExists(t *testing.T, path string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var one int
	return db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'`).Scan(&one) == nil
}

func TestNotifyWatchBasics(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	notify(t, c, "orders", "before")
	s := watch(t, c, []string{"orders"})
	nothing(t, s, 150*time.Millisecond)
	s1 := notify(t, c, "orders", map[string]int{"id": 1})
	s2 := notify(t, c, "orders", nil)
	e1, e2 := next(t, s), next(t, s)
	var p map[string]int
	_ = e1.Decode(&p)
	if e1.Seq != s1 || e2.Seq != s2 || p["id"] != 1 || !e2.Null || e1.Namespace != "default" || e1.CreatedAt.IsZero() {
		t.Fatalf("events %+v %+v", e1, e2)
	}
	if s.LastSeq() != s2 {
		t.Fatalf("LastSeq = %d", s.LastSeq())
	}
}

func TestWatchFromSeq(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	var seqs []int64
	for i := 0; i < 5; i++ {
		seqs = append(seqs, notify(t, c, "c", i))
	}
	all := watch(t, c, []string{"c"}, lytecache.FromSeq(0))
	for i := 0; i < 5; i++ {
		if e := next(t, all); e.Seq != seqs[i] {
			t.Fatalf("replay %d: %d", i, e.Seq)
		}
	}
	after := watch(t, c, []string{"c"}, lytecache.FromSeq(seqs[1]))
	for i := 2; i < 5; i++ {
		if v := payloadInt(t, next(t, after)); v != i {
			t.Fatalf("resume got %d want %d", v, i)
		}
	}
	nothing(t, after, 150*time.Millisecond)
}

func TestWatchOrderingFanOutNotifyMany(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	a := watch(t, c, []string{"a", "b"})
	b := watch(t, c, []string{"*"})
	seqs, err := c.NotifyMany([]lytecache.Notification{{"a", 1}, {"b", 2}, {"a", 3}})
	if err != nil || len(seqs) != 3 || seqs[0] >= seqs[1] || seqs[1] >= seqs[2] {
		t.Fatalf("NotifyMany = %v %v", seqs, err)
	}
	for _, s := range []*lytecache.Subscription{a, b} {
		for i := 0; i < 3; i++ {
			if e := next(t, s); e.Seq != seqs[i] || payloadInt(t, e) != i+1 {
				t.Fatalf("order: %+v", e)
			}
		}
	}
}

func TestChannelMatching(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	s := watch(t, c, []string{"cache:invalidate:*", "orders"})
	mid := watch(t, c, []string{"a*b"})
	plan := []struct {
		ch string
		p  int
	}{{"cache:invalidate:user", 1}, {"cache:invalidate:", 2}, {"cache:invalidatex", 3}, {"orders", 4}, {"orders:new", 5}, {"axb", 6}}
	for _, p := range plan {
		notify(t, c, p.ch, p.p)
	}
	for _, want := range []int{1, 2, 4} {
		if v := payloadInt(t, next(t, s)); v != want {
			t.Fatalf("got %d want %d", v, want)
		}
	}
	nothing(t, s, 200*time.Millisecond)
	nothing(t, mid, 50*time.Millisecond)
}

func TestChannelValidationAndNamespaces(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	for _, bad := range []string{"", "x*", "__cache__", "__x"} {
		if _, err := c.Notify(bad, 1); !errors.Is(err, lytecache.ErrInvalidArgument) {
			t.Fatalf("Notify(%q) = %v", bad, err)
		}
	}
	if _, err := c.Watch(nil); !errors.Is(err, lytecache.ErrInvalidArgument) {
		t.Fatal("Watch with no patterns")
	}
	other := newEventsCache(t, path, lytecache.WithNamespace("other"))
	s := watch(t, c, []string{"c"})
	notify(t, other, "c", 1)
	notify(t, c, "c", 2)
	if e := next(t, s); payloadInt(t, e) != 2 {
		t.Fatalf("namespace leak: %+v", e)
	}
	nothing(t, s, 150*time.Millisecond)
}

func TestEventPayloadTypesAndSpecFixture(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	s := watch(t, c, []string{"t"}, lytecache.FromSeq(0))
	notify(t, c, "t", []byte("raw"))
	notify(t, c, "t", "text")
	notify(t, c, "t", int64(-7))
	notify(t, c, "t", 3.25)
	notify(t, c, "t", map[string]any{"a": []int{1}})
	var b []byte
	_ = next(t, s).Decode(&b)
	var str string
	_ = next(t, s).Decode(&str)
	var i int64
	_ = next(t, s).Decode(&i)
	var f float64
	_ = next(t, s).Decode(&f)
	var m map[string][]int
	_ = next(t, s).Decode(&m)
	if string(b) != "raw" || str != "text" || i != -7 || f != 3.25 || m["a"][0] != 1 {
		t.Fatalf("round trip: %q %q %d %v %v", b, str, i, f, m)
	}

	// Rows built from SPEC.md alone, as another implementation writes them.
	db, _ := sql.Open("sqlite", path)
	_, err := db.Exec(`INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES
('fx','default',CAST('bytes' AS BLOB),0,1),('fx','default',CAST('42' AS BLOB),2,1),
('fx','default',CAST('{"k":[1,2]}' AS BLOB),4,1),('fx','default',NULL,4,1),('fx','default',x'aced',6,1)`)
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	fx := watch(t, c, []string{"fx"}, lytecache.FromSeq(0))
	var fb []byte
	_ = next(t, fx).Decode(&fb)
	var fi int
	_ = next(t, fx).Decode(&fi)
	var fm map[string][]int
	_ = next(t, fx).Decode(&fm)
	null := next(t, fx)
	java := next(t, fx)
	var dummy any
	if string(fb) != "bytes" || fi != 42 || len(fm["k"]) != 2 || !null.Null || !errors.Is(java.Decode(&dummy), lytecache.ErrSerialization) {
		t.Fatalf("fixture: %q %d %v %v", fb, fi, fm, null)
	}

	raw, _ := sql.Open("sqlite", path)
	defer func() { _ = raw.Close() }()
	var blob []byte
	var vt int
	_ = raw.QueryRow(`SELECT payload, value_type FROM events WHERE channel='t' ORDER BY seq DESC LIMIT 1`).Scan(&blob, &vt)
	if string(blob) != `{"a":[1]}` || vt != 4 {
		t.Fatalf("stored %s %d", blob, vt)
	}
}

func TestCursorHelpersAndGap(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path, lytecache.WithEventRetention(50*time.Millisecond))
	if _, found, _ := c.LoadCursor("w"); found {
		t.Fatal("no cursor yet")
	}
	_ = c.SaveCursor("w", 41)
	_ = c.SaveCursor("w", 42)
	if v, found, _ := c.LoadCursor("w"); !found || v != 42 {
		t.Fatalf("LoadCursor = %d", v)
	}

	var old []int64
	for i := 0; i < 10; i++ {
		old = append(old, notify(t, c, "c", i))
	}
	time.Sleep(100 * time.Millisecond)
	fresh := notify(t, c, "c", 99)
	if _, err := c.Maintain(); err != nil {
		t.Fatal(err)
	}
	s := watch(t, c, []string{"c"}, lytecache.FromSeq(old[3]))
	_, err := s.Next(context.Background())
	var gap *lytecache.GapError
	if !errors.As(err, &gap) || !errors.Is(err, lytecache.ErrGap) || gap.RequestedSeq != old[3] || gap.OldestSeq != fresh || gap.ResumeSeq != fresh-1 {
		t.Fatalf("gap = %v", err)
	}
	ok := watch(t, c, []string{"c"}, lytecache.FromSeq(gap.ResumeSeq))
	if payloadInt(t, next(t, ok)) != 99 {
		t.Fatal("resume after accepting the gap")
	}
	zero := watch(t, c, []string{"c"}, lytecache.FromSeq(0))
	if next(t, zero).Seq != fresh {
		t.Fatal("FromSeq(0) never gaps")
	}
	ahead := watch(t, c, []string{"c"}, lytecache.FromSeq(fresh+100))
	if _, err := ahead.Next(context.Background()); !errors.As(err, &gap) || !gap.Ahead {
		t.Fatalf("ahead = %v", err)
	}
}

func TestGapDetectedMidStreamAndCountCap(t *testing.T) {
	path := tempDBPath(t)
	sub := newEventsCache(t, path)
	pub := newEventsCache(t, path, lytecache.WithEventMaxCount(5))
	notify(t, pub, "c", "start")
	s := watch(t, sub, []string{"c"}, lytecache.PollInterval(1500*time.Millisecond))
	time.Sleep(300 * time.Millisecond) // first (empty) poll done; next is ~1.5 s away
	var seqs []int64
	for i := 0; i < 20; i++ {
		seqs = append(seqs, notify(t, pub, "c", i))
	}
	_, _ = pub.Maintain()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.Next(ctx); !errors.Is(err, lytecache.ErrGap) {
		t.Fatalf("mid-stream gap = %v", err)
	}
	kept := watch(t, pub, []string{"c"}, lytecache.FromSeq(0))
	for _, want := range seqs[15:] {
		if e := next(t, kept); e.Seq != want {
			t.Fatalf("count cap kept %d, want %d", e.Seq, want)
		}
	}
}

func TestNoEventsTableOrGoroutineUntilUsed(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	for i := 0; i < 20; i++ {
		_ = c.Set("k"+strconv.Itoa(i), i)
	}
	_, _ = c.Delete("k1")
	_ = c.Flush()
	_, _ = c.Maintain()
	if info, _ := c.EventStats(); info.Enabled {
		t.Fatal("EventStats created the table")
	}
	if tableExists(t, path) {
		t.Fatal("events table created without notify/watch")
	}
	// Goroutine leaks are checked for the whole package by goleak in TestMain.
}

func TestSubscriptionCloseIsIdempotentAndCacheCloseStopsSubscriptions(t *testing.T) {
	c, err := lytecache.New(lytecache.WithPath(tempDBPath(t)), lytecache.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := c.Watch([]string{"a"})
	b, _ := c.Watch([]string{"b"})
	_ = a.Close()
	_ = a.Close()
	if _, err := a.Next(context.Background()); !errors.Is(err, lytecache.ErrSubscriptionClosed) {
		t.Fatalf("after close: %v", err)
	}
	_ = c.Close() // closes b
	if _, ok := <-b.Events(); ok {
		t.Fatal("cache Close should end its subscriptions")
	}
}

func TestV1FileGainsEventsTableAndStaysVersion1(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	_ = c.Set("k", "v")
	if tableExists(t, path) {
		t.Fatal("table before first use")
	}
	notify(t, c, "c", 1)
	db, _ := sql.Open("sqlite", path)
	defer func() { _ = db.Close() }()
	var sv, ev string
	_ = db.QueryRow(`SELECT v FROM meta WHERE k='schema_version'`).Scan(&sv)
	_ = db.QueryRow(`SELECT v FROM meta WHERE k='events_version'`).Scan(&ev)
	if sv != "1" || ev != "1" || !tableExists(t, path) {
		t.Fatalf("schema_version=%q events_version=%q", sv, ev)
	}
}

func TestInProcessWakeIsImmediate(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	s := watch(t, c, []string{"c"}, lytecache.PollInterval(5*time.Second))
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	notify(t, c, "c", 1)
	next(t, s)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("local notify took %v", d)
	}
}

// -- multi-process ------------------------------------------------------------

func startHelper(t *testing.T, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(append(os.Environ(), "LYTECACHE_WANT_HELPER_PROCESS=1"), env...)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func TestMultiProcessDeliveryToTwoSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process")
	}
	path := tempDBPath(t)
	setup := newEventsCache(t, path)
	_ = watch(t, setup, []string{"x"}).Close()
	const n = 150
	dir := t.TempDir()
	var subs []*exec.Cmd
	for i := 0; i < 2; i++ {
		ready := filepath.Join(dir, "ready"+strconv.Itoa(i))
		subs = append(subs, startHelper(t, "LYTECACHE_HELPER_MODE=subscribe", "LYTECACHE_HELPER_PATH="+path,
			"LYTECACHE_HELPER_ITERATIONS="+strconv.Itoa(n), "LYTECACHE_HELPER_READY="+ready,
			"LYTECACHE_HELPER_LOG="+filepath.Join(dir, "log"+strconv.Itoa(i))))
		waitFile(t, ready)
	}
	runHelperProcess(t, "LYTECACHE_HELPER_MODE=publish", "LYTECACHE_HELPER_PATH="+path, "LYTECACHE_HELPER_ITERATIONS="+strconv.Itoa(n))
	for i, cmd := range subs {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("subscriber %d: %v", i, err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "log"+strconv.Itoa(i)))
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != n {
			t.Fatalf("subscriber %d got %d events", i, len(lines))
		}
		prev := int64(0)
		for j, line := range lines {
			f := strings.Fields(line)
			seq, _ := strconv.ParseInt(f[0], 10, 64)
			if f[1] != "orders:new" || f[2] != strconv.Itoa(j) || seq <= prev {
				t.Fatalf("subscriber %d line %d: %q", i, j, line)
			}
			prev = seq
		}
	}
}

func TestMultiProcessResumeAfterKill(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process")
	}
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	_ = watch(t, c, []string{"jobs"}).Close()
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := startHelper(t, "LYTECACHE_HELPER_MODE=cursorsub", "LYTECACHE_HELPER_PATH="+path, "LYTECACHE_HELPER_READY="+ready)
	killed := false
	defer func() {
		if !killed {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	waitFile(t, ready)
	cursor := func() int64 { v, _, _ := c.LoadCursor("worker"); return v }
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(20 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal("timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := notify(t, c, "jobs", "probe")
	waitFor(func() bool { return cursor() == first })
	var seqs []int64
	for i := 0; i < 40; i++ {
		seqs = append(seqs, notify(t, c, "jobs", i))
	}
	waitFor(func() bool { return cursor() >= seqs[10] })
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	killed = true
	saved := cursor()
	for i := 40; i < 60; i++ {
		seqs = append(seqs, notify(t, c, "jobs", i))
	}
	s := watch(t, c, []string{"jobs"}, lytecache.FromSeq(saved))
	for _, want := range seqs {
		if want <= saved {
			continue
		}
		if e := next(t, s); e.Seq != want {
			t.Fatalf("resume got %d want %d", e.Seq, want)
		}
	}
	nothing(t, s, 200*time.Millisecond)
}

func TestMultiProcessHashLostUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process")
	}
	for _, mode := range []string{"json1", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "fallback" {
				t.Setenv("LYTECACHE_FORCE_JSON_FALLBACK", "1")
			}
			path := tempDBPath(t)
			c := newEventsCache(t, path)
			_ = c.Set("shared", map[string]any{"other": "untouched"})
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(2)
				go func() {
					defer wg.Done()
					runHelperProcess(t, "LYTECACHE_HELPER_MODE=hset", "LYTECACHE_HELPER_PATH="+path,
						"LYTECACHE_HELPER_KEY=shared", "LYTECACHE_HELPER_FIELD=f"+strconv.Itoa(i), "LYTECACHE_HELPER_ITERATIONS=100")
				}()
				go func() {
					defer wg.Done()
					runHelperProcess(t, "LYTECACHE_HELPER_MODE=hincrby", "LYTECACHE_HELPER_PATH="+path,
						"LYTECACHE_HELPER_KEY=shared", "LYTECACHE_HELPER_FIELD=n", "LYTECACHE_HELPER_ITERATIONS=100")
				}()
			}
			wg.Wait()
			all, err := c.HGetAll("shared")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				if string(all["f"+strconv.Itoa(i)]) != "99" {
					t.Fatalf("field f%d = %s", i, all["f"+strconv.Itoa(i)])
				}
			}
			if string(all["n"]) != "400" || string(all["other"]) != `"untouched"` {
				t.Fatalf("lost updates: %s", all)
			}
		})
	}
}

func TestNotifyStreamAlongsideWritesAcrossProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process")
	}
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	_ = watch(t, c, []string{"x"}).Close()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		runHelperProcess(t, "LYTECACHE_HELPER_MODE=publish", "LYTECACHE_HELPER_PATH="+path, "LYTECACHE_HELPER_ITERATIONS=200")
	}()
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			runHelperProcess(t, "LYTECACHE_HELPER_MODE=incr", "LYTECACHE_HELPER_PATH="+path,
				"LYTECACHE_HELPER_KEY=counter", "LYTECACHE_HELPER_ITERATIONS=200")
		}()
	}
	wg.Wait()
	if v, _, _ := c.GetInt64("counter"); v != 400 {
		t.Fatalf("lost writes: counter = %d", v)
	}
	if info, _ := c.EventStats(); info.Count != 400 {
		t.Fatalf("events = %d", info.Count)
	}
}

// -- change listeners ---------------------------------------------------------

type recorder struct {
	mu   sync.Mutex
	recs []lytecache.ChangeRecord
}

func (r *recorder) add(rec lytecache.ChangeRecord) {
	r.mu.Lock()
	r.recs = append(r.recs, rec)
	r.mu.Unlock()
}

func (r *recorder) settle() []lytecache.ChangeRecord {
	time.Sleep(200 * time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]lytecache.ChangeRecord(nil), r.recs...)
}

func TestOnChangeBulkEmitsOneTruncatedRecord(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	r := &recorder{}
	if _, err := c.OnChange(r.add); err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	for i := 0; i < 2000; i++ {
		m["k"+strconv.Itoa(i)] = i
	}
	_ = c.SetMany(m)
	_ = c.SetMany(map[string]any{"a": 1, "b": 2})
	_, _ = c.Delete("a", "b", "a")
	_, _ = c.Delete("missing")
	_ = c.Flush()
	got := r.settle()
	if len(got) != 4 {
		t.Fatalf("records: %+v", got)
	}
	if g := got[0]; g.Op != "set" || g.Count != 2000 || !g.Truncated || g.Keys != nil {
		t.Fatalf("bulk: %+v", g)
	}
	if g := got[1]; strings.Join(g.Keys, ",") != "a,b" || g.Truncated {
		t.Fatalf("small bulk: %+v", g)
	}
	if g := got[2]; g.Op != "delete" || strings.Join(g.Keys, ",") != "a,b" || g.Count != 2 {
		t.Fatalf("delete: %+v", g)
	}
	if g := got[3]; g.Op != "flush" || !g.Truncated || g.Count != 2000 {
		t.Fatalf("flush: %+v", g)
	}
}

func TestOnChangeEveryValueWriteAndNoOps(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	r := &recorder{}
	_, _ = c.OnChange(r.add, lytecache.OpSet)
	_ = c.Set("a", 1)
	_, _ = c.Add("b", 1)
	_, _ = c.Add("b", 2) // no change
	_, _ = c.Replace("a", 2)
	_, _ = c.Replace("missing", 2) // no change
	var old int
	_, _ = c.GetSet("a", 3, &old)
	_, _ = c.Incr("n", 1)
	_, _ = c.HSet("h", "f", 1)
	_, _ = c.HSetNX("h", "f", 2) // no change
	_, _ = c.HIncrBy("h", "n", 1)
	_, _ = c.HDel("h", "nope") // no change
	_, _ = c.Delete("a")       // not a "set"
	lock, _ := c.Lock("job", time.Second)
	_ = lock.Release()
	var keys []string
	for _, rec := range r.settle() {
		keys = append(keys, rec.Keys[0])
	}
	if strings.Join(keys, ",") != "a,b,a,a,n,h,h" {
		t.Fatalf("keys = %v", keys)
	}
	if _, err := c.OnChange(r.add, "expire"); !errors.Is(err, lytecache.ErrInvalidArgument) {
		t.Fatal("unknown op accepted")
	}
}

func TestOnChangeNotCalledOnRollback(t *testing.T) {
	path := tempDBPath(t)
	c := newEventsCache(t, path)
	db, _ := sql.Open("sqlite", path)
	_, _ = db.Exec(`CREATE TRIGGER boom BEFORE INSERT ON cache WHEN NEW.key = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END`)
	_ = db.Close()
	r := &recorder{}
	_, _ = c.OnChange(r.add)
	if err := c.SetMany(map[string]any{"a": 1, "boom": 2}); err == nil {
		t.Fatal("expected the trigger to abort")
	}
	if got := r.settle(); len(got) != 0 {
		t.Fatalf("rolled-back write notified: %+v", got)
	}
	if ok, _ := c.Exists("a"); ok {
		t.Fatal("not rolled back")
	}
}

func TestOnChangePanicSlowReentrantDepthGuard(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t), lytecache.WithLogger(discardLogger()))
	_, _ = c.OnChange(func(lytecache.ChangeRecord) { panic("listener bug") })
	r := &recorder{}
	_, _ = c.OnChange(r.add)
	if err := c.Set("a", 1); err != nil {
		t.Fatal("a panicking listener failed the write")
	}
	if len(r.settle()) != 1 {
		t.Fatal("later listener did not run after a panic")
	}

	c2 := newEventsCache(t, tempDBPath(t))
	release := make(chan struct{})
	_, _ = c2.OnChange(func(lytecache.ChangeRecord) { <-release })
	start := time.Now()
	for i := 0; i < 20; i++ {
		_ = c2.Set("k"+strconv.Itoa(i), i)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("slow listener blocked writers")
	}
	close(release)

	c3 := newEventsCache(t, tempDBPath(t), lytecache.WithLogger(discardLogger()))
	var calls atomic.Int32
	_, _ = c3.OnChange(func(lytecache.ChangeRecord) {
		calls.Add(1)
		_, _ = c3.Incr("depth", 1) // re-enters the cache, triggering itself again
	}, lytecache.OpSet)
	_ = c3.Set("start", 1)
	time.Sleep(time.Second)
	if calls.Load() != 8 {
		t.Fatalf("depth guard: %d calls", calls.Load())
	}
	if v, _, _ := c3.GetInt64("depth"); v != 8 {
		t.Fatalf("depth = %d", v)
	}
}

func TestOnChangeUnsubscribeAndDispatcherLifecycle(t *testing.T) {
	c := newEventsCache(t, tempDBPath(t))
	gate := make(chan struct{})
	_, _ = c.OnChange(func(lytecache.ChangeRecord) { <-gate })
	r := &recorder{}
	unsub, _ := c.OnChange(r.add)
	_ = c.Set("a", 1)
	unsub()
	unsub()
	close(gate)
	if got := r.settle(); len(got) != 0 {
		t.Fatalf("called after unsubscribe: %+v", got)
	}
	// The dispatch goroutine is stopped by Close; goleak in TestMain checks it.
}

func TestPublishChanges(t *testing.T) {
	path := tempDBPath(t)
	plain := newEventsCache(t, path)
	_ = plain.Set("a", 1)
	_, _ = plain.Delete("a")
	if tableExists(t, path) {
		t.Fatal("publish_changes unset but events table created")
	}
	c := newEventsCache(t, path, lytecache.WithPublishChanges(lytecache.OpDelete, lytecache.OpFlush))
	s := watch(t, c, []string{lytecache.CacheChannel})
	_ = c.Set("a", 1)
	m := map[string]any{}
	for i := 0; i < 150; i++ {
		m["k"+strconv.Itoa(i)] = i
	}
	_ = c.SetMany(m)
	_, _ = c.Delete("a", "missing")
	_ = c.Flush()
	var d, f lytecache.ChangeRecord
	_ = next(t, s).Decode(&d)
	_ = next(t, s).Decode(&f)
	nothing(t, s, 200*time.Millisecond)
	if d.Op != "delete" || strings.Join(d.Keys, ",") != "a,missing" || d.Count != 1 || d.Truncated {
		t.Fatalf("delete event: %+v", d)
	}
	if f.Op != "flush" || f.Keys != nil || !f.Truncated || f.Count != 150 {
		t.Fatalf("flush event: %+v", f)
	}
	if _, err := lytecache.New(lytecache.WithPath(path), lytecache.WithPublishChanges("expire")); !errors.Is(err, lytecache.ErrInvalidArgument) {
		t.Fatal("unknown publish op accepted")
	}
}

func TestInvalidationRecipe(t *testing.T) {
	path := tempDBPath(t)
	workerA := newEventsCache(t, path)
	workerB := newEventsCache(t, path)
	localMemo := map[string]int{"balance:42": 100, "balance:7": 5}

	sub, err := workerB.Watch([]string{"invalidate"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	// worker A, after committing a DB change
	_, _ = workerA.Delete("balance:42")
	_, _ = workerA.Notify("invalidate", map[string]any{"key": "balance:42"})

	// workers B..N
	ev := <-sub.Events()
	var p struct {
		Key string `json:"key"`
	}
	_ = ev.Decode(&p)
	delete(localMemo, p.Key)
	if len(localMemo) != 1 || localMemo["balance:7"] != 5 {
		t.Fatalf("memo = %v", localMemo)
	}
}
