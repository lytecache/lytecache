package lytecache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Local event notification: Notify / Watch.
//
// SINGLE HOST ONLY. POLLED, NOT PUSHED. DURABLE. This is not Redis Pub/Sub:
// events are rows in the `events` table with a monotonic seq; watchers on
// the same machine poll it (every 50 ms by default, backing off to ~1 s
// when idle). Subscribers on another machine never see anything, and
// SQLite locking is unreliable on network filesystems, so this cannot be
// stretched across hosts. See SPEC.md, "Events", and docs/notifications.md.

// eventsDDL is created lazily, the first time a Cache uses Notify or Watch.
// Adding it does NOT bump schema_version (still 1): released libraries
// refuse a newer schema_version, and v1 libraries must keep opening these
// files. The events_version meta row advertises the table instead.
const eventsDDL = `
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  channel    TEXT    NOT NULL,
  namespace  TEXT    NOT NULL DEFAULT 'default',
  payload    BLOB,
  value_type INTEGER NOT NULL DEFAULT 4,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_seq_channel ON events(channel, seq);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
INSERT OR IGNORE INTO meta (k, v) VALUES ('events_version', '1');
`

const (
	eventsBatch       = 500
	maxIdleInterval   = time.Second
	defaultPoll       = 50 * time.Millisecond
	defaultRetention  = time.Hour
	defaultKeyCap     = 100
	cacheChannel      = "__cache__"
	maxDispatchDepth  = 8
	subscriptionQueue = 256
)

// CacheChannel is the reserved channel that [WithPublishChanges] publishes
// mutations to.
const CacheChannel = cacheChannel

// Event is one published event.
type Event struct {
	Seq       int64
	Channel   string
	Namespace string
	CreatedAt time.Time
	// Payload is the raw payload; decode it with [Event.Decode].
	Payload RawValue
	// Null reports a null payload (Notify with a nil payload).
	Null bool
}

// Decode decodes the payload into dest, exactly as [Cache.Get] decodes a
// value. A null payload leaves dest unchanged.
func (e Event) Decode(dest any) error {
	if e.Null {
		return nil
	}
	return e.Payload.Decode(dest)
}

// Notification is one event for [Cache.NotifyMany].
type Notification struct {
	Channel string
	Payload any
}

// GapError reports that events a subscription should have seen are gone:
// retention deleted them before it read them, or its cursor is ahead of the
// file (the file was recreated). It is never silently skipped. To accept
// the loss, watch again with FromSeq(ResumeSeq). errors.Is(err, ErrGap)
// matches it.
type GapError struct {
	RequestedSeq int64
	OldestSeq    int64
	ResumeSeq    int64
	Ahead        bool
}

func (e *GapError) Error() string {
	if e.Ahead {
		return fmt.Sprintf("lytecache: cursor %d is ahead of the last event ever published (%d); the database file was probably recreated",
			e.RequestedSeq, e.OldestSeq-1)
	}
	return fmt.Sprintf("lytecache: events after seq %d are no longer retained (oldest retained is %d); events %d..%d were lost",
		e.RequestedSeq, e.OldestSeq, e.RequestedSeq+1, e.OldestSeq-1)
}

// Is makes errors.Is(err, ErrGap) true.
func (e *GapError) Is(target error) bool { return target == ErrGap }

func newGap(requested, oldest int64, ahead bool) *GapError {
	return &GapError{RequestedSeq: requested, OldestSeq: oldest, ResumeSeq: oldest - 1, Ahead: ahead}
}

func validateChannel(channel string) error {
	switch {
	case channel == "":
		return fmt.Errorf("%w: channel must be non-empty", ErrInvalidArgument)
	case strings.Contains(channel, "*"):
		return fmt.Errorf("%w: channel %q may not contain '*' (reserved for watch patterns)", ErrInvalidArgument, channel)
	case strings.HasPrefix(channel, "__"):
		return fmt.Errorf("%w: channel names starting with '__' are reserved: %q", ErrInvalidArgument, channel)
	}
	return nil
}

// channelFilter implements the matching rule: an exact name, or a trailing
// '*' prefix glob; "*" alone matches everything; '*' elsewhere is literal.
func channelFilter(patterns []string) (string, []any, error) {
	var clauses []string
	var args []any
	for _, p := range patterns {
		if p == "" {
			return "", nil, fmt.Errorf("%w: watch patterns must be non-empty", ErrInvalidArgument)
		}
		if strings.HasSuffix(p, "*") {
			prefix := strings.TrimSuffix(p, "*")
			if prefix == "" {
				return "1", nil, nil
			}
			clauses = append(clauses, "substr(channel, 1, ?) = ?")
			args = append(args, len([]rune(prefix)), prefix)
		} else {
			clauses = append(clauses, "channel = ?")
			args = append(args, p)
		}
	}
	return "(" + strings.Join(clauses, " OR ") + ")", args, nil
}

func (c *Cache) ensureEvents() error {
	if c.eventsReady.Load() {
		return nil
	}
	if _, err := c.writeDB.Exec(eventsDDL); err != nil {
		return fmt.Errorf("lytecache: creating events table: %w", err)
	}
	c.eventsReady.Store(true)
	return nil
}

func (c *Cache) eventsExist() bool {
	if c.eventsReady.Load() {
		return true
	}
	var one int
	err := c.readDB.QueryRow(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'`).Scan(&one)
	if err == nil {
		c.eventsReady.Store(true)
		return true
	}
	return false
}

// Notify publishes an event for watchers ON THIS HOST and returns its seq.
//
// Single host only, delivered by polling (not pushed), durable: the event
// is a committed row that watchers read until retention deletes it
// ([WithEventRetention], default 1 hour). payload is encoded exactly like a
// cached value. Each call is one write under the same lock as Set, so a
// high event rate slows cache writes -- batch with [Cache.NotifyMany]. Call
// it after your own database transaction commits, never inside one you
// might roll back.
func (c *Cache) Notify(channel string, payload any) (int64, error) {
	if err := validateChannel(channel); err != nil {
		return 0, err
	}
	seqs, err := c.insertEvents([]Notification{{channel, payload}})
	if err != nil {
		return 0, err
	}
	return seqs[0], nil
}

// NotifyMany publishes several events in one transaction (one write-lock
// acquisition), with the same semantics as [Cache.Notify]. It returns the
// assigned seqs in order.
func (c *Cache) NotifyMany(events []Notification) ([]int64, error) {
	for _, e := range events {
		if err := validateChannel(e.Channel); err != nil {
			return nil, err
		}
	}
	if len(events) == 0 {
		return []int64{}, nil
	}
	return c.insertEvents(events)
}

func (c *Cache) insertEvents(events []Notification) ([]int64, error) {
	type row struct {
		channel string
		data    []byte
		vt      int
	}
	rows := make([]row, len(events))
	for i, e := range events {
		data, vt, err := encodeValue(e.Payload)
		if err != nil {
			return nil, err
		}
		rows[i] = row{e.Channel, data, vt}
	}
	if err := c.ensureEvents(); err != nil {
		return nil, err
	}
	now := nowMillis()
	const q = `INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES (?, ?, ?, ?, ?)`
	seqs := make([]int64, 0, len(rows))
	insert := func(ex interface {
		Exec(string, ...any) (sql.Result, error)
	}) error {
		for _, r := range rows {
			res, err := ex.Exec(q, r.channel, c.namespace, r.data, r.vt, now)
			if err != nil {
				return err
			}
			id, err := res.LastInsertId()
			if err != nil {
				return err
			}
			seqs = append(seqs, id)
		}
		return nil
	}
	var err error
	if len(rows) == 1 {
		err = insert(c.writeDB)
	} else {
		err = c.inTx(func(tx *sql.Tx) error { return insert(tx) })
	}
	if err != nil {
		return nil, fmt.Errorf("lytecache: notify: %w", err)
	}
	// In-process optimization only: wake this Cache's watchers now instead
	// of at their next poll. The committed row stays the source of truth.
	c.subsMu.Lock()
	for s := range c.subs {
		s.poke()
	}
	c.subsMu.Unlock()
	return seqs, nil
}

// WatchOption configures [Cache.Watch].
type WatchOption func(*watchOptions)

type watchOptions struct {
	fromSeq      *int64
	pollInterval time.Duration
}

// FromSeq resumes after seq n (for example a cursor saved with
// [Cache.SaveCursor]). FromSeq(0) replays everything still retained.
// Without it, a subscription sees only events published from now on.
func FromSeq(n int64) WatchOption { return func(o *watchOptions) { o.fromSeq = &n } }

// PollInterval sets how often the subscription polls while events are
// flowing (default 50 ms). It backs off to ~1 s while idle.
func PollInterval(d time.Duration) WatchOption {
	return func(o *watchOptions) { o.pollInterval = d }
}

// Watch subscribes to events published ON THIS HOST to channels matching
// patterns: exact names, or a trailing '*' for a prefix
// ("cache:invalidate:*"); "*" matches every channel. Only events from this
// Cache's namespace are seen.
//
// Single host only; polled (latency about the poll interval, up to ~1 s
// after an idle spell); durable and resumable via [FromSeq]. Delivery is
// at-least-once from your cursor, in total seq order, to every subscriber
// (fan-out; no consumer groups) -- handlers must be idempotent. If events
// after your cursor were already deleted by retention, the subscription
// ends with a [*GapError] (see [Subscription.Err]) instead of resuming
// elsewhere. Always Close the subscription.
func (c *Cache) Watch(patterns []string, opts ...WatchOption) (*Subscription, error) {
	if len(patterns) == 0 {
		return nil, fmt.Errorf("%w: Watch needs at least one channel or pattern", ErrInvalidArgument)
	}
	filter, filterArgs, err := channelFilter(patterns)
	if err != nil {
		return nil, err
	}
	o := watchOptions{pollInterval: defaultPoll}
	for _, opt := range opts {
		opt(&o)
	}
	if o.pollInterval <= 0 {
		return nil, fmt.Errorf("%w: poll interval must be positive", ErrInvalidArgument)
	}
	if o.fromSeq != nil && *o.fromSeq < 0 {
		return nil, fmt.Errorf("%w: FromSeq must be >= 0", ErrInvalidArgument)
	}
	if err := c.ensureEvents(); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", c.path+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		return nil, fmt.Errorf("lytecache: watch: %w", err)
	}
	db.SetMaxOpenConns(1)

	var start int64
	var gap *GapError
	err = readTx(db, func(tx *sql.Tx) error {
		last, err := lastAssignedSeq(tx)
		if err != nil {
			return err
		}
		switch {
		case o.fromSeq == nil:
			start = last
		case *o.fromSeq == 0:
			var oldest sql.NullInt64
			if err := tx.QueryRow(`SELECT min(seq) FROM events`).Scan(&oldest); err != nil {
				return err
			}
			start = last
			if oldest.Valid {
				start = oldest.Int64 - 1
			}
		default:
			start = *o.fromSeq
			gap, err = gapCheck(tx, start)
			return err
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("lytecache: watch: %w", err)
	}

	s := &Subscription{
		cache:        c,
		db:           db,
		namespace:    c.namespace,
		patterns:     append([]string(nil), patterns...),
		filter:       filter,
		filterArgs:   filterArgs,
		cursor:       start,
		pollInterval: o.pollInterval,
		events:       make(chan Event, subscriptionQueue),
		wake:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	s.lastSeq.Store(start)
	if gap != nil {
		s.err = gap
		close(s.events)
		close(s.done)
	} else {
		go s.run()
	}
	c.subsMu.Lock()
	c.subs[s] = struct{}{}
	c.subsMu.Unlock()
	return s, nil
}

// Subscription is a live, polled view of the events table for some
// channels, returned by [Cache.Watch]. Receive from [Subscription.Events],
// or call [Subscription.Next] / [Subscription.TryNext]. Always Close it.
type Subscription struct {
	cache        *Cache
	db           *sql.DB
	namespace    string
	patterns     []string
	filter       string
	filterArgs   []any
	cursor       int64 // poller goroutine only
	pollInterval time.Duration

	events chan Event
	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}

	lastSeq   atomic.Int64
	errMu     sync.Mutex
	err       error
	closeOnce sync.Once
}

// Events returns the channel events arrive on, in seq order. It is closed
// when the subscription is closed or ends with a gap; check [Subscription.Err].
func (s *Subscription) Events() <-chan Event { return s.events }

// Err returns the [*GapError] that ended the subscription, or nil.
func (s *Subscription) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// LastSeq is the seq of the last event handed to the Events channel (the
// value to persist as a cursor once you have handled it).
func (s *Subscription) LastSeq() int64 { return s.lastSeq.Load() }

// Patterns returns the channel patterns this subscription matches.
func (s *Subscription) Patterns() []string { return append([]string(nil), s.patterns...) }

// Next blocks for the next event. It returns the gap error, ctx's error, or
// [ErrSubscriptionClosed] once the subscription has ended.
func (s *Subscription) Next(ctx context.Context) (Event, error) {
	select {
	case e, ok := <-s.events:
		if !ok {
			return Event{}, s.endErr()
		}
		return e, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

// TryNext returns an already-fetched event without waiting; ok is false if
// there is none yet.
func (s *Subscription) TryNext() (e Event, ok bool, err error) {
	select {
	case e, open := <-s.events:
		if !open {
			return Event{}, false, s.endErr()
		}
		return e, true, nil
	default:
		return Event{}, false, nil
	}
}

func (s *Subscription) endErr() error {
	if err := s.Err(); err != nil {
		return err
	}
	return ErrSubscriptionClosed
}

// Close stops polling and releases the goroutine and connection. It is
// idempotent.
func (s *Subscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		err = s.db.Close()
		s.cache.subsMu.Lock()
		delete(s.cache.subs, s)
		s.cache.subsMu.Unlock()
	})
	return err
}

func (s *Subscription) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Subscription) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

func (s *Subscription) run() {
	defer close(s.done)
	defer close(s.events)
	interval := s.pollInterval
	idleCap := max(maxIdleInterval, s.pollInterval)
	for {
		select {
		case <-s.stop:
			return
		default:
		}
		batch, gap, err := s.fetch()
		switch {
		case err != nil:
			s.cache.logger.Warn("lytecache: watch poll failed, retrying", "error", err)
		case gap != nil:
			s.setErr(gap)
			return
		}
		for _, e := range batch {
			select {
			case s.events <- e:
				s.cursor = e.Seq
				s.lastSeq.Store(e.Seq)
			case <-s.stop:
				return
			}
		}
		if len(batch) > 0 {
			interval = s.pollInterval
			if len(batch) == eventsBatch {
				continue // more are waiting: read on without sleeping
			}
		} else if err == nil {
			interval = min(interval*2, idleCap)
		}
		timer := time.NewTimer(interval)
		select {
		case <-s.stop:
			timer.Stop()
			return
		case <-s.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// fetch runs the gap check and the batch read in one read transaction, so
// the sweeper cannot delete events between the two.
func (s *Subscription) fetch() ([]Event, *GapError, error) {
	var out []Event
	var gap *GapError
	err := readTx(s.db, func(tx *sql.Tx) error {
		var err error
		if gap, err = gapCheck(tx, s.cursor); err != nil || gap != nil {
			return err
		}
		args := append([]any{s.cursor, s.namespace}, s.filterArgs...)
		args = append(args, eventsBatch)
		rows, err := tx.Query(`SELECT seq, channel, namespace, payload, value_type, created_at FROM events
WHERE seq > ? AND namespace = ? AND `+s.filter+` ORDER BY seq LIMIT ?`, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				return err
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, gap, err
}

type rowScanner interface{ Scan(dest ...any) error }

func scanEvent(r rowScanner) (Event, error) {
	var e Event
	var payload []byte
	var vt int
	var created int64
	if err := r.Scan(&e.Seq, &e.Channel, &e.Namespace, &payload, &vt, &created); err != nil {
		return Event{}, err
	}
	e.CreatedAt = time.UnixMilli(created)
	e.Null = payload == nil || (vt == typeJSON && string(payload) == "null")
	e.Payload = RawValue{Data: payload, typeCode: vt}
	return e, nil
}

func readTx(db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // read-only; nothing to keep
	return fn(tx)
}

func lastAssignedSeq(q interface {
	QueryRow(string, ...any) *sql.Row
}) (int64, error) {
	var last int64
	err := q.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'events'`).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return last, err
}

// gapCheck implements SPEC.md's gap rule: a cursor older than the oldest
// retained event minus one has lost events; one ahead of the last seq
// ever assigned means the file was recreated.
func gapCheck(tx *sql.Tx, cursor int64) (*GapError, error) {
	last, err := lastAssignedSeq(tx)
	if err != nil {
		return nil, err
	}
	if cursor > last {
		return newGap(cursor, last+1, true), nil
	}
	var oldest sql.NullInt64
	if err := tx.QueryRow(`SELECT min(seq) FROM events`).Scan(&oldest); err != nil {
		return nil, err
	}
	o := last + 1
	if oldest.Valid {
		o = oldest.Int64
	}
	if cursor < o-1 {
		return newGap(cursor, o, false), nil
	}
	return nil, nil
}

// SaveCursor persists a named subscriber cursor in the meta table. Store
// an event's seq after you have finished handling it, then resume with
// Watch(..., FromSeq(n)). Each save is a write: on a busy stream save every
// N events, accepting that up to N may be redelivered after a crash.
func (c *Cache) SaveCursor(name string, seq int64) error {
	_, err := c.writeDB.Exec(`INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
		"cursor:"+c.namespace+":"+name, fmt.Sprint(seq))
	if err != nil {
		return fmt.Errorf("lytecache: save cursor %q: %w", name, err)
	}
	return nil
}

// LoadCursor returns the cursor last saved under name; found is false if
// there is none.
func (c *Cache) LoadCursor(name string) (seq int64, found bool, err error) {
	var v string
	err = c.readDB.QueryRow(`SELECT v FROM meta WHERE k = ?`, "cursor:"+c.namespace+":"+name).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("lytecache: load cursor %q: %w", name, err)
	}
	if _, err := fmt.Sscan(v, &seq); err != nil {
		return 0, false, fmt.Errorf("lytecache: load cursor %q: %w", name, err)
	}
	return seq, true, nil
}

// sweepEvents applies retention in bounded batches. It always deletes a
// prefix of the seq range (everything up to the newest event past
// retention or beyond the count cap), never a hole, which keeps gap
// detection sound even if clocks disagree.
func (c *Cache) sweepEvents() {
	if !c.eventsExist() {
		return
	}
	var bound sql.NullInt64
	if err := c.readDB.QueryRow(`SELECT max(seq) FROM events WHERE created_at <= ?`,
		nowMillis()-c.eventRetention.Milliseconds()).Scan(&bound); err != nil {
		c.logger.Warn("lytecache: sweep: event retention", "error", err)
		return
	}
	if c.eventMaxCount > 0 {
		var capBound int64
		err := c.readDB.QueryRow(`SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?`, c.eventMaxCount).Scan(&capBound)
		if err == nil && (!bound.Valid || capBound > bound.Int64) {
			bound = sql.NullInt64{Int64: capBound, Valid: true}
		}
	}
	if !bound.Valid {
		return
	}
	for {
		res, err := c.writeDB.Exec(`DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)`,
			bound.Int64, sweepBatchSize)
		if err != nil {
			c.logger.Warn("lytecache: sweep: event retention", "error", err)
			return
		}
		if rowsAffected(res) < sweepBatchSize {
			return
		}
	}
}

// EventInfo summarizes the retained events in a file, for admin tooling
// (the lytecache CLI's `events` command and the UI's Events tab).
type EventInfo struct {
	Enabled      bool // false if the file has no events table
	Count        int64
	OldestSeq    int64
	NewestSeq    int64 // last seq ever assigned
	OldestAt     time.Time
	NewestAt     time.Time
	PayloadBytes int64
}

// EventStats reports on the events retained in this file, across every
// namespace. It is a pure read and never creates the events table.
func (c *Cache) EventStats() (EventInfo, error) {
	if !c.eventsExist() {
		return EventInfo{}, nil
	}
	var info EventInfo
	var oldest, oldestAt, newestAt, bytes sql.NullInt64
	err := c.readDB.QueryRow(`SELECT COUNT(*), min(seq), min(created_at), max(created_at), COALESCE(SUM(length(payload)), 0) FROM events`).
		Scan(&info.Count, &oldest, &oldestAt, &newestAt, &bytes)
	if err != nil {
		return EventInfo{}, fmt.Errorf("lytecache: event stats: %w", err)
	}
	info.Enabled = true
	info.OldestSeq = oldest.Int64
	info.PayloadBytes = bytes.Int64
	if oldestAt.Valid {
		info.OldestAt = time.UnixMilli(oldestAt.Int64)
		info.NewestAt = time.UnixMilli(newestAt.Int64)
	}
	if info.NewestSeq, err = lastAssignedSeq(c.readDB); err != nil {
		return EventInfo{}, fmt.Errorf("lytecache: event stats: %w", err)
	}
	return info, nil
}

// RecentEvents returns up to limit retained events with seq > afterSeq,
// newest last, across every namespace (like [Cache.Namespaces], this is
// for admin tooling, not application code). With afterSeq < 0 it returns
// the newest limit events. Optional patterns restrict it to matching
// channels, using exactly [Cache.Watch]'s rule (see [MatchChannel]); with
// none it returns every channel. It is a pure read and never creates the
// table.
func (c *Cache) RecentEvents(limit int, afterSeq int64, patterns ...string) ([]Event, error) {
	if !c.eventsExist() || limit <= 0 {
		return nil, nil
	}
	where, args := "1", []any{}
	if len(patterns) > 0 {
		var err error
		if where, args, err = channelFilter(patterns); err != nil {
			return nil, err
		}
	}
	var rows *sql.Rows
	var err error
	if afterSeq < 0 {
		rows, err = c.readDB.Query(`SELECT * FROM (SELECT seq, channel, namespace, payload, value_type, created_at
FROM events WHERE `+where+` ORDER BY seq DESC LIMIT ?) ORDER BY seq`, append(args, limit)...)
	} else {
		rows, err = c.readDB.Query(`SELECT seq, channel, namespace, payload, value_type, created_at
FROM events WHERE seq > ? AND `+where+` ORDER BY seq LIMIT ?`, append(append([]any{afterSeq}, args...), limit)...)
	}
	if err != nil {
		return nil, fmt.Errorf("lytecache: recent events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("lytecache: recent events: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
