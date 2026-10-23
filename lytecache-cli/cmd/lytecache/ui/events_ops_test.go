package ui

import (
	"database/sql"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	lytecache "github.com/lytecache/lytecache-go"
	"gopkg.in/yaml.v3"
)

// seedEvents runs fn against a fresh cache at a new path and closes it, so
// the server under test opens the file itself.
func seedEvents(t *testing.T, fn func(c *lytecache.Cache)) string {
	t.Helper()
	path := tempDBPath(t)
	c, err := lytecache.New(lytecache.WithPath(path), lytecache.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	fn(c)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func rawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustNotify(t *testing.T, c *lytecache.Cache, channel string, payload any) int64 {
	t.Helper()
	seq, err := c.Notify(channel, payload)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// tailOf returns the live-tail section of an Events page, so assertions
// are not fooled by the retention panel or the filter box.
func tailOf(t *testing.T, body string) string {
	t.Helper()
	i := strings.Index(body, "Live tail")
	if i < 0 {
		t.Fatalf("no live tail in:\n%s", body)
	}
	return body[i:]
}

func TestEventsChannelFilterMatchesWatchRule(t *testing.T) {
	channels := []string{"orders", "orders:paid", "orders:refunded", "ordersx", "payments"}
	path := seedEvents(t, func(c *lytecache.Cache) {
		for _, ch := range channels {
			mustNotify(t, c, ch, ch+"-payload")
		}
	})
	s := newTestServer(t, DBSource{Name: "svc", Path: path})

	for _, filter := range []string{"orders:*", "orders", "orders*", "payments orders:paid", "*", "nope"} {
		tail := tailOf(t, doGet(t, s, "/db/svc/events?channel="+url.QueryEscape(filter)).Body.String())
		for _, ch := range channels {
			want := false
			for _, p := range strings.Fields(filter) {
				want = want || lytecache.MatchChannel(p, ch)
			}
			got := strings.Contains(tail, ch+"-payload")
			if got != want {
				t.Errorf("filter %q, channel %q: shown=%v, watch would match=%v", filter, ch, got, want)
			}
		}
	}
}

func TestEventPayloadsRenderEveryTypeCode(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) {
		mustNotify(t, c, "t", "plain text")             // 1 string
		mustNotify(t, c, "t", int64(42))                // 2 int
		mustNotify(t, c, "t", 2.5)                      // 3 float
		mustNotify(t, c, "t", map[string]any{"a": 1})   // 4 json
		mustNotify(t, c, "t", []byte{0xde, 0xad, 0xbe}) // 0 bytes
		mustNotify(t, c, "t", nil)                      // null
	})
	db := rawDB(t, path)
	now := time.Now().UnixMilli()
	for _, code := range []int{5, 6} {
		if _, err := db.Exec(`INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES ('t', 'default', x'80049501', ?, ?)`, code, now); err != nil {
			t.Fatal(err)
		}
	}
	s := newTestServer(t, DBSource{Name: "svc", Path: path})

	cases := []struct {
		seq  int
		want []string
	}{
		{1, []string{"string", "plain text"}},
		{2, []string{"int", "42"}},
		{3, []string{"float", "2.5"}},
		{4, []string{"json", "data-json", `&#34;a&#34;: 1`}},
		{5, []string{"bytes", "3q2&#43;", "3 bytes, base64"}},
		{6, []string{"null"}},
		{7, []string{"python-pickle", "written by another language&#39;s native serializer"}},
		{8, []string{"java-serialized", "written by another language&#39;s native serializer"}},
	}
	for _, tc := range cases {
		rec := doGet(t, s, "/db/svc/events/"+strconv.Itoa(tc.seq))
		if rec.Code != http.StatusOK {
			t.Fatalf("seq %d: code %d", tc.seq, rec.Code)
		}
		body := rec.Body.String()
		for _, w := range tc.want {
			if !strings.Contains(body, w) {
				t.Errorf("seq %d: missing %q in:\n%s", tc.seq, w, body)
			}
		}
	}
	tail := tailOf(t, doGet(t, s, "/db/svc/events").Body.String())
	if strings.Count(tail, "native serializer") != 2 {
		t.Error("tail does not show the non-portable message for codes 5 and 6")
	}
	if rec := doGet(t, s, "/db/svc/events/999"); !strings.Contains(rec.Body.String(), "Not found") {
		t.Error("missing event is not reported as not found")
	}
}

func TestSubscriberLagAndLostCursor(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) {
		for i := 0; i < 10; i++ {
			mustNotify(t, c, "jobs", i)
		}
		_ = c.SaveCursor("caught-up", 10)
		_ = c.SaveCursor("behind", 7)
		_ = c.SaveCursor("lost", 1)
	})
	db := rawDB(t, path)
	// seq 8 waited 90 s; seqs 1-3 are purged so "lost" (seq 1 < oldest-1 = 3) missed events.
	if _, err := db.Exec(`UPDATE events SET created_at = ? WHERE seq = 8`, time.Now().Add(-90*time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM events WHERE seq <= 3`); err != nil {
		t.Fatal(err)
	}

	c := c2(t, path)
	info, err := c.EventStats()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := BuildCursorRows(c, info, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]CursorRow{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	if r := byName["caught-up"]; r.LagEvents != 0 || r.LagSeconds != 0 || !r.LagKnown || r.Lost {
		t.Errorf("caught-up = %+v", r)
	}
	if r := byName["behind"]; r.LagEvents != 3 || r.Lost || r.LagSeconds < 89 || r.LagSeconds > 120 {
		t.Errorf("behind = %+v, want lag 3 events / ~90 s", r)
	}
	if r := byName["lost"]; !r.Lost || r.LagEvents != 9 {
		t.Errorf("lost = %+v, want Lost", r)
	}
	// The library agrees: resuming the lost cursor is a gap.
	sub, err := c.Watch([]string{"jobs"}, lytecache.FromSeq(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sub.TryNext(); err == nil {
		t.Error("library did not raise a gap for the cursor the UI flagged")
	}
	_ = sub.Close()

	s := newTestServer(t, DBSource{Name: "svc", Path: path}, DBSource{Name: "healthy", Path: buildFixtureDB(t)})
	body := doGet(t, s, "/db/svc/events").Body.String()
	for _, w := range []string{"lost events", "1 subscriber cursor(s) are outside the retained range", "caught up", "behind", "1m30s"} {
		if !strings.Contains(body, w) {
			t.Errorf("events page missing %q", w)
		}
	}
	dash := BuildDashboard(s.mgr)
	if !dash[0].Unhealthy || dash[0].LostCursors != 1 || !strings.Contains(strings.Join(dash[0].UnhealthyWhy, ";"), "lost events") {
		t.Errorf("dashboard row for svc = %+v", dash[0])
	}
	if dash[1].Unhealthy || dash[1].EventsEnabled {
		t.Errorf("dashboard row for healthy = %+v", dash[1])
	}
	if d := doGet(t, s, "/dashboard").Body.String(); !strings.Contains(d, `it lost events">lost</span>`) {
		i := strings.Index(d, "/db/svc/events")
		t.Errorf("fleet dashboard does not flag the lost cursor: %s", d[max(0, i-200):min(len(d), i+400)])
	}
}

func TestEventPurgeRoutes403WithoutAllowDelete(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) { mustNotify(t, c, "c", 1) })
	s := newTestServer(t, DBSource{Name: "svc", Path: path})
	for _, tc := range []struct {
		path string
		form url.Values
	}{
		{"/db/svc/events/delete-event", url.Values{"seq": {"1"}}},
		{"/db/svc/events/purge", url.Values{"older_than": {"1ms"}}},
		{"/db/svc/events/clear-channel", url.Values{"channel": {"c"}, "confirm_channel": {"c"}}},
	} {
		if rec := doPost(t, s, tc.path, tc.form); rec.Code != http.StatusForbidden {
			t.Errorf("%s: code %d, want 403", tc.path, rec.Code)
		}
	}
	if info, _ := c2(t, path).EventStats(); info.Count != 1 {
		t.Fatalf("an event was deleted despite 403s: %+v", info)
	}
	if strings.Contains(doGet(t, s, "/db/svc/events").Body.String(), "Purge events") {
		t.Error("purge controls shown without --allow-delete")
	}
}

func TestEventPurgesWorkAndAreAudited(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) {
		for _, ch := range []string{"a", "b", "a", "keep"} {
			mustNotify(t, c, ch, ch)
		}
	})
	db := rawDB(t, path)
	if _, err := db.Exec(`UPDATE events SET created_at = ? WHERE seq = 1`, time.Now().Add(-48*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	audit := filepath.Join(t.TempDir(), "audit.log")
	s := newTestServerWithConfig(t, Config{Databases: []DBSource{{Name: "svc", Path: path}}, AllowDelete: true, AuditLogPath: audit})

	if rec := doPost(t, s, "/db/svc/events/purge", url.Values{"older_than": {"24h"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("purge: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doPost(t, s, "/db/svc/events/delete-event", url.Values{"seq": {"2"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("delete-event: %d", rec.Code)
	}
	if rec := doPost(t, s, "/db/svc/events/clear-channel", url.Values{"channel": {"a"}, "confirm_channel": {"x"}}); rec.Code != http.StatusBadRequest {
		t.Fatalf("clear-channel with wrong confirmation: %d, want 400", rec.Code)
	}
	if rec := doPost(t, s, "/db/svc/events/clear-channel", url.Values{"channel": {"a"}, "confirm_channel": {"a"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("clear-channel: %d", rec.Code)
	}
	events, _ := c2(t, path).RecentEvents(10, -1)
	if len(events) != 1 || events[0].Channel != "keep" {
		t.Fatalf("remaining events = %+v, want only 'keep'", events)
	}
	_ = s.Close()
	logged, err := os.ReadFile(audit)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"action=event-purge", `key="older_than=24h0m0s"`, "action=event-delete", `key="seq=2"`, "action=event-clear-channel", `key="channel=a"`} {
		if !strings.Contains(string(logged), w) {
			t.Errorf("audit log missing %q:\n%s", w, logged)
		}
	}
	if strings.Count(string(logged), "\n") != 3 {
		t.Errorf("want exactly 3 audit lines (the rejected confirmation is not a write):\n%s", logged)
	}
}

func TestMaskingRedactsEventPayloads(t *testing.T) {
	const secret = "otp-831442"
	path := seedEvents(t, func(c *lytecache.Cache) {
		mustNotify(t, c, "payments:otp", map[string]string{"code": secret})                 // seq 1: masked channel
		mustNotify(t, c, "invalidate", map[string]string{"key": "user:7:otp", "v": secret}) // seq 2: value names a masked key
		mustNotify(t, c, "invalidate", map[string]string{"otp_code": secret})               // seq 3: masked field name
		mustNotify(t, c, "notes", "user:7:otp "+secret)                                     // seq 4: string payload
		mustNotify(t, c, "invalidate", map[string]string{"key": "balance:42"})              // seq 5: visible
	})
	s := newTestServerWithConfig(t, Config{
		Databases:    []DBSource{{Name: "svc", Path: path}},
		MaskKeys:     []string{"*otp*"},
		MaskChannels: []string{"payments:*"},
	})
	pages := []string{"/db/svc/events", "/db/svc/events?channel=*"}
	for seq := 1; seq <= 4; seq++ {
		pages = append(pages, "/db/svc/events/"+strconv.Itoa(seq))
	}
	for _, p := range pages {
		body := doGet(t, s, p).Body.String()
		if strings.Contains(body, secret) {
			t.Errorf("%s: secret payload reached the response", p)
		}
	}
	tail := tailOf(t, doGet(t, s, "/db/svc/events").Body.String())
	if !strings.Contains(tail, "balance:42") {
		t.Error("an unmasked payload was redacted too")
	}
	if strings.Count(tail, "payload redacted") != 4 {
		t.Errorf("want 4 redaction notices in the tail:\n%s", tail)
	}
	if !strings.Contains(doGet(t, s, "/db/svc/events/1").Body.String(), "--mask-channels") {
		t.Error("detail view does not say why the payload is redacted")
	}
}

func TestEventsTabGreyedOutWithoutEventsTable(t *testing.T) {
	s := newTestServer(t, DBSource{Name: "plain", Path: buildFixtureDB(t)},
		DBSource{Name: "evented", Path: seedEvents(t, func(c *lytecache.Cache) { mustNotify(t, c, "c", 1) })})
	if body := doGet(t, s, "/db/plain/namespaces/default/keys").Body.String(); !strings.Contains(body, "nav-disabled") || !strings.Contains(body, "never used") {
		t.Error("Events tab not greyed out for a database without an events table")
	}
	if body := doGet(t, s, "/db/evented/namespaces/default/keys").Body.String(); strings.Contains(body, "nav-disabled") {
		t.Error("Events tab greyed out for a database that has events")
	}
}

// The UI must never apply an event retention it cannot know: neither in the
// background nor through an explicit maintenance pass.
func TestMaintenancePassNeverDeletesEvents(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) { mustNotify(t, c, "c", 1) })
	db := rawDB(t, path)
	if _, err := db.Exec(`UPDATE events SET created_at = ?`, time.Now().Add(-30*24*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	s := newDeletableTestServer(t, DBSource{Name: "svc", Path: path})
	if rec := doPost(t, s, "/db/svc/namespaces/default/maintain", nil); rec.Code != http.StatusSeeOther {
		t.Fatalf("maintain: %d", rec.Code)
	}
	if info, _ := c2(t, path).EventStats(); info.Count != 1 {
		t.Fatalf("a maintenance pass from the UI deleted a 30-day-old event: %+v", info)
	}
}

func TestEventMetrics(t *testing.T) {
	path := seedEvents(t, func(c *lytecache.Cache) {
		mustNotify(t, c, "invalidate", 1)
		mustNotify(t, c, "invalidate", 2)
		mustNotify(t, c, "jobs", 3)
		_ = c.SaveCursor("worker-b", 1)
	})
	plain := buildFixtureDB(t)
	s := newTestServer(t, DBSource{Name: "svc", Path: path}, DBSource{Name: "plain", Path: plain})
	body := scrapeMetrics(t, s, nil).Body.String()
	for _, w := range []string{
		`lytecache_events_retained{channel="invalidate",database="svc"} 2`,
		`lytecache_events_retained{channel="jobs",database="svc"} 1`,
		"# TYPE lytecache_events_published_total counter",
		`lytecache_events_published_total{database="svc"} 3`,
		`lytecache_event_oldest_age_seconds{database="svc"}`,
		`lytecache_subscriber_lag_events{cursor="worker-b",database="svc",namespace="default"} 2`,
		`lytecache_subscriber_lag_seconds{cursor="worker-b",database="svc",namespace="default"}`,
		`lytecache_subscriber_gap{cursor="worker-b",database="svc",namespace="default"} 0`,
	} {
		if !strings.Contains(body, w) {
			t.Errorf("metrics missing %q", w)
		}
	}
	if strings.Contains(body, `lytecache_events_published_total{database="plain"}`) {
		t.Error("event metrics emitted for a database without events")
	}
	if eventsTableExists(t, plain) {
		t.Error("a scrape created the events table")
	}
}

func TestDeclaredEventRetentionYAMLRoundTrip(t *testing.T) {
	var src DBSource
	if err := yaml.Unmarshal([]byte("name: svc\npath: /x.db\nevent_retention: 24h\n"), &src); err != nil {
		t.Fatal(err)
	}
	if src.EventRetention == nil || time.Duration(*src.EventRetention) != 24*time.Hour {
		t.Fatalf("EventRetention = %v", src.EventRetention)
	}
	out, err := yaml.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "event_retention: 24h0m0s") {
		t.Fatalf("marshalled as:\n%s", out)
	}
	if err := yaml.Unmarshal([]byte("name: svc\npath: /x.db\nevent_retention: -1h\n"), &src); err == nil {
		t.Error("negative retention accepted")
	}

	path := seedEvents(t, func(c *lytecache.Cache) { mustNotify(t, c, "c", 1) })
	d := Duration(24 * time.Hour)
	s := newTestServer(t, DBSource{Name: "svc", Path: path, EventRetention: &d})
	if body := doGet(t, s, "/db/svc/events").Body.String(); !strings.Contains(body, "24h0m0s") || strings.Contains(body, "not declared") {
		t.Error("declared retention not shown")
	}
}
