package ui

import (
	"database/sql"
	"net/http"
	"net/url"
	"strings"
	"testing"

	lytecache "github.com/lytecache/lytecache-go"
)

func eventsTableExists(t *testing.T, path string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var one int
	return db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'`).Scan(&one) == nil
}

func TestEventsTabWithoutEventsTableCreatesNothing(t *testing.T) {
	path := buildFixtureDB(t)
	s := newTestServer(t, DBSource{Name: "svc", Path: path})
	rec := doGet(t, s, "/db/svc/events")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "No events table") {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if eventsTableExists(t, path) {
		t.Fatal("viewing the Events tab created the events table")
	}
}

func TestEventsTabShowsLiveTailAndStatsEscaped(t *testing.T) {
	path := tempDBPath(t)
	c, err := lytecache.New(lytecache.WithPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Notify("invalidate", map[string]string{"key": "balance:42"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Notify("<script>alert(1)</script>", "<b>x</b>"); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()

	s := newTestServer(t, DBSource{Name: "svc", Path: path})
	rec := doGet(t, s, "/db/svc/events")
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	for _, want := range []string{"Retained events", ">2<", "invalidate", "balance:42", `data-refresh-adaptive="1"`, "cannot be published"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "<script>alert(1)</script>") || strings.Contains(body, "<b>x</b>") {
		t.Error("channel/payload rendered unescaped")
	}
	// Newest first.
	tail := tailOf(t, body)
	if strings.Index(tail, "&lt;script&gt;") > strings.Index(tail, "invalidate") {
		t.Error("tail is not newest-first")
	}
	if !strings.Contains(doGet(t, s, "/db/svc/namespaces/default/keys").Body.String(), `/db/svc/events`) {
		t.Error("no Events link in the sidebar")
	}
}

func TestEventsTabCannotPublish(t *testing.T) {
	path := tempDBPath(t)
	c, _ := lytecache.New(lytecache.WithPath(path))
	_, _ = c.Notify("c", 1)
	_ = c.Close()
	s := newDeletableTestServer(t, DBSource{Name: "svc", Path: path})
	for _, p := range []string{"/db/svc/events", "/db/svc/events/notify", "/db/svc/notify"} {
		rec := doPost(t, s, p, url.Values{"channel": {"c"}, "payload": {"x"}})
		if rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
			t.Errorf("POST %s = %d, want 405/404", p, rec.Code)
		}
	}
	c2, _ := lytecache.New(lytecache.WithPath(path))
	defer func() { _ = c2.Close() }()
	if info, _ := c2.EventStats(); info.Count != 1 {
		t.Fatalf("an event was published through the UI: count=%d", info.Count)
	}
}

func TestEventsTabUnknownDatabase404(t *testing.T) {
	s := newTestServer(t)
	if rec := doGet(t, s, "/db/nope/events"); rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}
