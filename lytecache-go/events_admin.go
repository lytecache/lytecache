package lytecache

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Admin-tooling reads and purges for the events table (the lytecache CLI's
// web UI uses these). Like [Cache.EventStats] and [Cache.RecentEvents] they
// span every namespace in the file and never create the events table.
//
// There is deliberately no admin method that inserts or rewrites an event:
// publishing goes through [Cache.Notify] only.

// MatchChannel reports whether channel matches a watch pattern, using
// exactly the rule [Cache.Watch] applies in SQL: an exact name, or a
// trailing '*' as a prefix glob. "*" alone matches everything, '*'
// anywhere else is literal, and an empty pattern matches nothing.
func MatchChannel(pattern, channel string) bool {
	if pattern == "" {
		return false
	}
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(channel, prefix)
	}
	return channel == pattern
}

// ChannelInfo summarizes the retained events on one channel.
type ChannelInfo struct {
	Channel      string
	Count        int64
	OldestSeq    int64
	NewestSeq    int64
	OldestAt     time.Time
	NewestAt     time.Time
	PayloadBytes int64
}

// EventChannelStats reports per-channel totals for the retained events,
// ordered by channel name. It is a pure read.
func (c *Cache) EventChannelStats() ([]ChannelInfo, error) {
	if !c.eventsExist() {
		return nil, nil
	}
	rows, err := c.readDB.Query(`SELECT channel, COUNT(*), min(seq), max(seq), min(created_at), max(created_at),
       COALESCE(SUM(length(payload)), 0)
FROM events GROUP BY channel ORDER BY channel`)
	if err != nil {
		return nil, fmt.Errorf("lytecache: event channel stats: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ChannelInfo
	for rows.Next() {
		var ci ChannelInfo
		var oldestAt, newestAt int64
		if err := rows.Scan(&ci.Channel, &ci.Count, &ci.OldestSeq, &ci.NewestSeq, &oldestAt, &newestAt, &ci.PayloadBytes); err != nil {
			return nil, fmt.Errorf("lytecache: event channel stats: %w", err)
		}
		ci.OldestAt, ci.NewestAt = time.UnixMilli(oldestAt), time.UnixMilli(newestAt)
		out = append(out, ci)
	}
	return out, rows.Err()
}

// EventCountSince returns how many retained events were published at or
// after t (for a publish rate). It is a pure read.
func (c *Cache) EventCountSince(t time.Time) (int64, error) {
	if !c.eventsExist() {
		return 0, nil
	}
	var n int64
	if err := c.readDB.QueryRow(`SELECT COUNT(*) FROM events WHERE created_at >= ?`, t.UnixMilli()).Scan(&n); err != nil {
		return 0, fmt.Errorf("lytecache: event count: %w", err)
	}
	return n, nil
}

// CursorInfo is one named subscriber cursor saved with [Cache.SaveCursor].
type CursorInfo struct {
	Namespace string
	Name      string
	Seq       int64
}

// Cursors lists every subscriber cursor saved in this file, across all
// namespaces, ordered by namespace then name. It is a pure read.
//
// Cursors are stored under meta keys "cursor:<namespace>:<name>". Both
// parts may contain ':', so the namespace is resolved against the
// namespaces the file actually uses (the cache table, the events table and
// this handle's own), longest match first; a key matching none splits at
// its first ':'. Entries whose value is not an integer are skipped.
func (c *Cache) Cursors() ([]CursorInfo, error) {
	raw, err := c.cursorRows()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}

	known, err := c.knownNamespaces()
	if err != nil {
		return nil, err
	}
	out := make([]CursorInfo, 0, len(raw))
	for _, e := range raw {
		seq, err := strconv.ParseInt(e.v, 10, 64)
		if err != nil {
			continue
		}
		rest := strings.TrimPrefix(e.k, "cursor:")
		ci := CursorInfo{Seq: seq}
		for _, ns := range known {
			if name, ok := strings.CutPrefix(rest, ns+":"); ok {
				ci.Namespace, ci.Name = ns, name
				break
			}
		}
		if ci.Namespace == "" {
			ci.Namespace, ci.Name, _ = strings.Cut(rest, ":")
		}
		out = append(out, ci)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

type metaRow struct{ k, v string }

// cursorRows reads the raw cursor:* meta rows. It closes its result set
// before Cursors issues its next query, which matters on a single-
// connection reader.
func (c *Cache) cursorRows() ([]metaRow, error) {
	rows, err := c.readDB.Query(`SELECT k, v FROM meta WHERE substr(k, 1, 7) = 'cursor:' ORDER BY k`)
	if err != nil {
		return nil, fmt.Errorf("lytecache: cursors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []metaRow
	for rows.Next() {
		var e metaRow
		if err := rows.Scan(&e.k, &e.v); err != nil {
			return nil, fmt.Errorf("lytecache: cursors: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: cursors: %w", err)
	}
	return out, nil
}

// knownNamespaces returns every namespace in use, longest first.
func (c *Cache) knownNamespaces() ([]string, error) {
	q := `SELECT DISTINCT namespace FROM cache`
	if c.eventsExist() {
		q += ` UNION SELECT DISTINCT namespace FROM events`
	}
	rows, err := c.readDB.Query(q)
	if err != nil {
		return nil, fmt.Errorf("lytecache: cursors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	set := map[string]bool{c.namespace: true}
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			return nil, fmt.Errorf("lytecache: cursors: %w", err)
		}
		set[ns] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lytecache: cursors: %w", err)
	}
	out := sortedKeys(set)
	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out, nil
}

// DeleteEvent deletes one retained event by seq and reports whether it
// existed. Subscribers that have not reached it yet simply never see it;
// it does not trigger a gap (only losing the oldest events does).
func (c *Cache) DeleteEvent(seq int64) (bool, error) {
	if !c.eventsExist() {
		return false, nil
	}
	res, err := c.writeDB.Exec(`DELETE FROM events WHERE seq = ?`, seq)
	if err != nil {
		return false, fmt.Errorf("lytecache: delete event: %w", err)
	}
	return rowsAffected(res) > 0, nil
}

// DeleteEventsBefore deletes every event published before t and returns
// how many were removed. Like the retention sweeper it removes a prefix of
// the seq range (everything up to the newest event older than t), never a
// hole, so a subscriber whose cursor falls inside it gets a [GapError]
// exactly as if retention had removed them. Deletes run in short batches so
// the write lock is never held for long.
func (c *Cache) DeleteEventsBefore(t time.Time) (int64, error) {
	if !c.eventsExist() {
		return 0, nil
	}
	var bound sql.NullInt64
	if err := c.readDB.QueryRow(`SELECT max(seq) FROM events WHERE created_at < ?`, t.UnixMilli()).Scan(&bound); err != nil {
		return 0, fmt.Errorf("lytecache: delete events: %w", err)
	}
	if !bound.Valid {
		return 0, nil
	}
	return c.deleteEventsBatched(`seq <= ?`, bound.Int64)
}

// DeleteChannelEvents deletes every retained event on exactly this channel
// (no pattern matching) and returns how many were removed, in short
// batches. Like [Cache.DeleteEvent], subscribers that had not reached them
// simply never see them.
func (c *Cache) DeleteChannelEvents(channel string) (int64, error) {
	if channel == "" {
		return 0, fmt.Errorf("%w: channel must be non-empty", ErrInvalidArgument)
	}
	if !c.eventsExist() {
		return 0, nil
	}
	return c.deleteEventsBatched(`channel = ?`, channel)
}

func (c *Cache) deleteEventsBatched(where string, arg any) (int64, error) {
	var total int64
	for {
		res, err := c.writeDB.Exec(`DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE `+where+` ORDER BY seq LIMIT ?)`,
			arg, sweepBatchSize)
		if err != nil {
			return total, fmt.Errorf("lytecache: delete events: %w", err)
		}
		n := rowsAffected(res)
		total += n
		if n < sweepBatchSize {
			return total, nil
		}
	}
}
