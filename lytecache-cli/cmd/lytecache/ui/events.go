package ui

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	lytecache "github.com/lytecache/lytecache-go"
)

// The Events tab: the operational window onto a database's events table
// and its subscribers' cursors -- a live tail, a retention panel, and
// subscriber lag.
//
// It can NEVER publish. There is no compose box, no replay button and no
// route that inserts into events: publishing is writing data, which the
// delete-only capability model (see mutations.go) forbids outright, and
// TestNoRouteCanCreateOrModifyAValue pins the route table to keep it so.
// Purging events IS allowed under --allow-delete, since it is deletion.
// Rendering the tab never creates the events table.

const (
	eventsTailLimit        = 100
	eventPayloadPreviewMax = 200
	// publishRateWindow is the trailing window the events/min rate is
	// averaged over.
	publishRateWindow = 5 * time.Minute
	// defaultEventRetention mirrors the library's default, shown when the
	// operator has not declared the application's real retention.
	defaultEventRetention = time.Hour
)

// eventMasker applies --mask-channels and --mask-keys to event payloads.
// A masked payload is never decoded into the response at all -- not even
// behind a reveal click -- matching how --mask-keys treats cached values.
type eventMasker struct {
	keys     []string // --mask-keys globs
	channels []string // --mask-channels globs
}

// reason returns why e's payload must be redacted, or "" if it may be
// shown. A channel match redacts the whole channel. A --mask-keys match
// redacts the payload if any JSON object field name, or any string value
// at any depth (a "key" field, a __cache__ change record's key list, a
// plain string payload), matches: event payloads carry references and
// codes just as cached values do, and over-redacting is the safe failure.
func (m eventMasker) reason(e lytecache.Event) string {
	for _, p := range m.channels {
		if ok, _ := filepath.Match(p, e.Channel); ok {
			return "payload redacted (channel matches a --mask-channels pattern)"
		}
	}
	if len(m.keys) == 0 || e.Null {
		return ""
	}
	var mentions bool
	switch e.Payload.ValueType() {
	case typeString:
		mentions = MatchesMaskPattern(string(e.Payload.Data), m.keys)
	case typeJSON:
		var v any
		if err := json.Unmarshal(e.Payload.Data, &v); err != nil {
			return "payload redacted (unparseable JSON cannot be checked against --mask-keys)"
		}
		mentions = jsonMentionsMasked(v, m.keys)
	}
	if mentions {
		return "payload redacted (references a key matching a --mask-keys pattern)"
	}
	return ""
}

func jsonMentionsMasked(v any, patterns []string) bool {
	switch t := v.(type) {
	case string:
		return MatchesMaskPattern(t, patterns)
	case []any:
		for _, x := range t {
			if jsonMentionsMasked(x, patterns) {
				return true
			}
		}
	case map[string]any:
		for k, x := range t {
			if MatchesMaskPattern(k, patterns) || jsonMentionsMasked(x, patterns) {
				return true
			}
		}
	}
	return false
}

// EventsView is the Events tab's view model.
type EventsView struct {
	DB      string
	Enabled bool

	// Filter is the channel filter as typed: whitespace-separated watch
	// patterns, matched with exactly Watch's rule (lytecache.MatchChannel).
	Filter string

	Count int64
	// OldestSeq/NewestSeq bound what is retained; NewestSeq is the last seq
	// ever assigned, so NewestSeq-OldestSeq+1 > Count means events have
	// been deleted by retention or a purge.
	OldestSeq, NewestSeq int64
	OldestAge, NewestAge string
	PayloadBytes         int64

	Retention         string
	RetentionDeclared bool
	PublishRate       float64 // events/min over publishRateWindow

	Channels    []ChannelRow
	Cursors     []CursorRow
	LostCursors int
	Rows        []EventRow
}

// PublishRateDisplay formats PublishRate for the retention panel.
func (v EventsView) PublishRateDisplay() string {
	return strconv.FormatFloat(v.PublishRate, 'f', 1, 64)
}

// ChannelRow is one row of the retention panel's per-channel breakdown.
type ChannelRow struct {
	Channel   string
	Count     int64
	OldestAge string
	NewestAge string
	Bytes     int64
}

// EventRow is one row of the live tail, newest first.
type EventRow struct {
	Seq         int64
	Channel     string
	Namespace   string
	Time        string
	Age         string
	SizeBytes   int
	TypeName    string
	Preview     string // one line, hidden until the operator reveals previews
	PreviewNote string // e.g. "truncated", or why there is no preview
	Masked      bool
}

// BuildEventsView reads stats, cursors and the newest matching events.
// Pure reads only.
func BuildEventsView(c *lytecache.Cache, e *dbEntry, filter string, mask eventMasker, now time.Time) (EventsView, error) {
	v := EventsView{DB: e.Name, Filter: strings.TrimSpace(filter)}
	v.Retention, v.RetentionDeclared = retentionFor(e)

	info, err := c.EventStats()
	if err != nil {
		return v, err
	}
	if !info.Enabled {
		return v, nil
	}
	v.Enabled = true
	v.Count, v.OldestSeq, v.NewestSeq, v.PayloadBytes = info.Count, info.OldestSeq, info.NewestSeq, info.PayloadBytes
	if info.Count > 0 {
		v.OldestAge = humanDuration(now.Sub(info.OldestAt))
		v.NewestAge = humanDuration(now.Sub(info.NewestAt))
	}
	if v.PublishRate, err = publishRate(c, now); err != nil {
		return v, err
	}

	channels, err := c.EventChannelStats()
	if err != nil {
		return v, err
	}
	for _, ch := range channels {
		v.Channels = append(v.Channels, ChannelRow{
			Channel: ch.Channel, Count: ch.Count, Bytes: ch.PayloadBytes,
			OldestAge: humanDuration(now.Sub(ch.OldestAt)), NewestAge: humanDuration(now.Sub(ch.NewestAt)),
		})
	}

	if v.Cursors, err = BuildCursorRows(c, info, now); err != nil {
		return v, err
	}
	for _, cr := range v.Cursors {
		if cr.Lost || cr.Ahead {
			v.LostCursors++
		}
	}

	events, err := c.RecentEvents(eventsTailLimit, -1, strings.Fields(v.Filter)...)
	if err != nil {
		return v, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		v.Rows = append(v.Rows, eventRow(events[i], mask, now))
	}
	return v, nil
}

func retentionFor(e *dbEntry) (string, bool) {
	if e.DeclaredEventRetention != nil {
		return time.Duration(*e.DeclaredEventRetention).String(), true
	}
	return defaultEventRetention.String(), false
}

func publishRate(c *lytecache.Cache, now time.Time) (float64, error) {
	n, err := c.EventCountSince(now.Add(-publishRateWindow))
	if err != nil {
		return 0, err
	}
	return float64(n) / publishRateWindow.Minutes(), nil
}

// CursorRow is one named subscriber cursor and its lag.
type CursorRow struct {
	Namespace string
	Name      string
	Seq       int64
	// LagEvents is how far the cursor is behind the newest seq. Seq is a
	// total order across every channel and a cursor does not record which
	// channels its subscriber watches, so this is an upper bound for a
	// subscriber of only some channels.
	LagEvents int64
	// LagSeconds is how long the oldest event after the cursor has been
	// waiting (0 when caught up); LagKnown is false if that event was
	// purged and no later one is retained.
	LagSeconds float64
	LagKnown   bool
	// Lost: the cursor is older than the oldest retained event, so the
	// subscriber permanently missed events and will get a GapError when it
	// resumes. Ahead: the cursor is past the last seq ever assigned (the
	// file was probably recreated) -- also a GapError on resume.
	Lost  bool
	Ahead bool
}

// LagDisplay formats LagSeconds.
func (r CursorRow) LagDisplay() string {
	if !r.LagKnown {
		return "–"
	}
	if r.LagSeconds == 0 {
		return "0s"
	}
	return humanDuration(time.Duration(r.LagSeconds * float64(time.Second)))
}

// BuildCursorRows computes every cursor's lag against info, applying the
// library's own gap rule (SPEC.md): with retained events, a cursor below
// oldest-1 has lost events; with none retained, any cursor below the
// newest seq has.
func BuildCursorRows(c *lytecache.Cache, info lytecache.EventInfo, now time.Time) ([]CursorRow, error) {
	cursors, err := c.Cursors()
	if err != nil {
		return nil, err
	}
	oldest := info.NewestSeq + 1
	if info.Count > 0 {
		oldest = info.OldestSeq
	}
	rows := make([]CursorRow, 0, len(cursors))
	for _, cur := range cursors {
		r := CursorRow{Namespace: cur.Namespace, Name: cur.Name, Seq: cur.Seq}
		switch {
		case cur.Seq > info.NewestSeq:
			r.Ahead = true
		case cur.Seq == info.NewestSeq:
			r.LagKnown = true
		default:
			r.LagEvents = info.NewestSeq - cur.Seq
			r.Lost = cur.Seq < oldest-1
			next, err := c.RecentEvents(1, cur.Seq)
			if err != nil {
				return nil, err
			}
			if len(next) == 1 {
				r.LagKnown = true
				r.LagSeconds = max(0, now.Sub(next[0].CreatedAt).Seconds())
			}
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func eventRow(e lytecache.Event, mask eventMasker, now time.Time) EventRow {
	code := e.Payload.ValueType()
	r := EventRow{
		Seq: e.Seq, Channel: e.Channel, Namespace: e.Namespace,
		Time: e.CreatedAt.UTC().Format("2006-01-02 15:04:05.000"), Age: humanDuration(now.Sub(e.CreatedAt)),
		SizeBytes: len(e.Payload.Data), TypeName: typeCodeName(code),
	}
	if why := mask.reason(e); why != "" {
		r.Masked, r.PreviewNote = true, why
		return r
	}
	switch {
	case e.Null:
		r.Preview = "null"
	case isNonPortable(code):
		r.PreviewNote = nonPortableMessage(code, int64(len(e.Payload.Data)))
	case code == typeBytes:
		r.Preview = base64.StdEncoding.EncodeToString(e.Payload.Data)
		r.PreviewNote = "base64"
	default:
		r.Preview = strings.Join(strings.Fields(string(e.Payload.Data)), " ")
	}
	if len(r.Preview) > eventPayloadPreviewMax {
		cut := eventPayloadPreviewMax
		for cut > 0 && !utf8.RuneStart(r.Preview[cut]) {
			cut--
		}
		r.Preview = r.Preview[:cut] + "…"
		r.PreviewNote = "truncated"
	}
	return r
}

// EventDetailView renders one event's payload in the value viewer's style,
// reusing its JSON pretty-printing and non-portable-code handling.
type EventDetailView struct {
	DB    string
	Found bool
	EventRow
	NonPortable bool
	Message     string
	Rendered    string
	IsJSON      bool
}

// BuildEventDetail fetches event seq. Pure read.
func BuildEventDetail(c *lytecache.Cache, db string, seq int64, mask eventMasker, now time.Time) (EventDetailView, error) {
	v := EventDetailView{DB: db}
	events, err := c.RecentEvents(1, seq-1)
	if err != nil || len(events) == 0 || events[0].Seq != seq {
		return v, err
	}
	e := events[0]
	v.Found = true
	v.EventRow = eventRow(e, mask, now)
	code := e.Payload.ValueType()
	switch {
	case v.Masked:
		v.Message = v.PreviewNote
	case isNonPortable(code):
		v.NonPortable = true
		v.Message = nonPortableMessage(code, int64(len(e.Payload.Data)))
	case e.Null:
		v.Rendered = "null"
	default:
		decoded, err := decodeEventPayload(e)
		if err != nil {
			return v, err
		}
		if v.Rendered, err = renderValue(code, decoded); err != nil {
			return v, err
		}
		v.IsJSON = code == typeJSON
	}
	return v, nil
}

func decodeEventPayload(e lytecache.Event) (any, error) {
	switch e.Payload.ValueType() {
	case typeBytes:
		var b []byte
		err := e.Payload.Decode(&b)
		return b, err
	case typeString:
		var s string
		err := e.Payload.Decode(&s)
		return s, err
	case typeInt:
		var n int64
		err := e.Payload.Decode(&n)
		return n, err
	case typeFloat:
		var f float64
		err := e.Payload.Decode(&f)
		return f, err
	default:
		var v any
		err := e.Payload.Decode(&v)
		return v, err
	}
}

func humanDuration(d time.Duration) string {
	switch {
	case d < 0:
		return "0s"
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%02dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

type eventsPage struct {
	Page
	EventsView
}

type eventDetailPage struct {
	Page
	EventDetailView
}

var (
	eventsTmpl      = parsePage("events.html")
	eventDetailTmpl = parsePage("event.html")
)

func (s *Server) eventMasker() eventMasker {
	return eventMasker{keys: s.maskKeys, channels: s.maskChannels}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	dbName := r.PathValue("db")
	e, ok := s.mgr.entry(dbName)
	if !ok {
		s.writeNotFound(w, r, "unknown database "+dbName)
		return
	}
	c, err := entryCache(s, dbName, "default")
	if err != nil {
		s.writeEntryError(w, r, dbName, err)
		return
	}
	view, err := BuildEventsView(c, e, r.URL.Query().Get("channel"), s.eventMasker(), time.Now())
	if err != nil {
		s.writeServerError(w, err)
		return
	}
	page := s.dbPage(r, dbName, "")
	page.CurrentView = "events"
	page.AdaptiveRefresh = view.Enabled
	s.writeTemplate(w, eventsTmpl, eventsPage{Page: page, EventsView: view})
}

func (s *Server) handleEventDetail(w http.ResponseWriter, r *http.Request) {
	dbName := r.PathValue("db")
	if _, ok := s.mgr.entry(dbName); !ok {
		s.writeNotFound(w, r, "unknown database "+dbName)
		return
	}
	seq, err := strconv.ParseInt(r.PathValue("seq"), 10, 64)
	if err != nil || seq <= 0 {
		s.writeNotFound(w, r, "no such event")
		return
	}
	c, err := entryCache(s, dbName, "default")
	if err != nil {
		s.writeEntryError(w, r, dbName, err)
		return
	}
	view, err := BuildEventDetail(c, dbName, seq, s.eventMasker(), time.Now())
	if err != nil {
		s.writeServerError(w, err)
		return
	}
	page := s.dbPage(r, dbName, "")
	page.CurrentView = "events"
	s.writeTemplate(w, eventDetailTmpl, eventDetailPage{Page: page, EventDetailView: view})
}

// --- Event purges (deletion only; gated behind --allow-delete) ---------

func (s *Server) eventsCacheFor(w http.ResponseWriter, r *http.Request) (string, *lytecache.Cache, bool) {
	dbName := r.PathValue("db")
	if _, ok := s.mgr.entry(dbName); !ok {
		s.writeNotFound(w, r, "unknown database "+dbName)
		return "", nil, false
	}
	if err := r.ParseForm(); err != nil {
		s.writeBadRequest(w, r, err.Error())
		return "", nil, false
	}
	c, err := entryCache(s, dbName, "default")
	if err != nil {
		s.writeEntryError(w, r, dbName, err)
		return "", nil, false
	}
	return dbName, c, true
}

func (s *Server) handleDeleteEvent(w http.ResponseWriter, r *http.Request) {
	dbName, c, ok := s.eventsCacheFor(w, r)
	if !ok {
		return
	}
	seq, err := strconv.ParseInt(r.PostForm.Get("seq"), 10, 64)
	if err != nil {
		s.writeBadRequest(w, r, "missing or invalid seq")
		return
	}
	deleted, err := c.DeleteEvent(seq)
	if err != nil {
		s.writeServerError(w, err)
		return
	}
	s.logMutation(r, "event-delete", dbName, "", fmt.Sprintf("seq=%d", seq), fmt.Sprintf("deleted=%v", deleted))
	http.Redirect(w, r, "/db/"+dbName+"/events", http.StatusSeeOther)
}

func (s *Server) handlePurgeEvents(w http.ResponseWriter, r *http.Request) {
	dbName, c, ok := s.eventsCacheFor(w, r)
	if !ok {
		return
	}
	age, err := time.ParseDuration(r.PostForm.Get("older_than"))
	if err != nil || age <= 0 {
		s.writeBadRequest(w, r, "older_than must be a positive duration such as 30m or 24h")
		return
	}
	n, err := c.DeleteEventsBefore(time.Now().Add(-age))
	if err != nil {
		s.writeServerError(w, err)
		return
	}
	s.logMutation(r, "event-purge", dbName, "", "older_than="+age.String(), fmt.Sprintf("deleted %d event(s)", n))
	http.Redirect(w, r, "/db/"+dbName+"/events", http.StatusSeeOther)
}

// handleClearChannel requires the channel name typed back, like flush's
// typed namespace confirmation.
func (s *Server) handleClearChannel(w http.ResponseWriter, r *http.Request) {
	dbName, c, ok := s.eventsCacheFor(w, r)
	if !ok {
		return
	}
	channel := r.PostForm.Get("channel")
	if channel == "" || r.PostForm.Get("confirm_channel") != channel {
		s.writeBadRequest(w, r, "confirmation text did not match the channel name")
		return
	}
	n, err := c.DeleteChannelEvents(channel)
	if err != nil {
		s.writeServerError(w, err)
		return
	}
	s.logMutation(r, "event-clear-channel", dbName, "", "channel="+channel, fmt.Sprintf("deleted %d event(s)", n))
	http.Redirect(w, r, "/db/"+dbName+"/events", http.StatusSeeOther)
}
