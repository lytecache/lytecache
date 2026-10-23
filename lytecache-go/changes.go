package lytecache

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Change operation names, shared by [Cache.OnChange] and
// [WithPublishChanges].
const (
	// OpSet covers every write that changes a value: Set, SetMany, Add,
	// Replace, GetSet, counters, and hash writes.
	OpSet = "set"
	// OpDelete is [Cache.Delete].
	OpDelete = "delete"
	// OpFlush is [Cache.Flush].
	OpFlush = "flush"

	opSet    = OpSet
	opDelete = OpDelete
	opFlush  = OpFlush
)

var allOps = []string{OpSet, OpDelete, OpFlush}

func validOps(ops []string) (map[string]bool, error) {
	set := map[string]bool{}
	for _, op := range ops {
		if op != OpSet && op != OpDelete && op != OpFlush {
			return nil, fmt.Errorf("%w: unknown change operation %q (want %q, %q or %q)", ErrInvalidArgument, op, OpSet, OpDelete, OpFlush)
		}
		set[op] = true
	}
	return set, nil
}

// ChangeRecord describes one committed mutation. A bulk operation produces
// ONE record, never one per key: Keys lists the keys the call targeted, or
// is nil with Truncated true when there were more than [WithChangeKeyCap]
// (default 100). Flush always has Keys nil and Truncated true. Count is
// the number of keys written, or for delete/flush the rows removed.
//
// Its JSON form is the payload of the __cache__ events written by
// [WithPublishChanges].
type ChangeRecord struct {
	Op        string   `json:"op"`
	Namespace string   `json:"namespace"`
	Keys      []string `json:"keys"`
	Count     int      `json:"count"`
	Truncated bool     `json:"truncated"`
}

func (c *Cache) newRecord(op string, keys []string, count int) ChangeRecord {
	r := ChangeRecord{Op: op, Namespace: c.namespace, Count: count}
	if keys == nil || len(keys) > c.changeKeyCap {
		r.Truncated = true
	} else {
		r.Keys = append([]string{}, keys...)
	}
	return r
}

// changed is called after a mutation commits. With no listeners and no
// publish_changes it returns immediately: the zero-cost common case.
func (c *Cache) changed(op string, keys []string, count int) {
	d := c.dispatcher.Load()
	toListeners := d != nil && d.wants(op)
	publish := c.publishOps[op]
	if !toListeners && !publish {
		return
	}
	rec := c.newRecord(op, keys, count)
	if toListeners {
		d.submit(rec)
	}
	if publish {
		if _, err := c.insertEvents([]Notification{{cacheChannel, rec}}); err != nil {
			c.logger.Warn("lytecache: failed to publish change event", "op", op, "error", err)
		}
	}
}

// OnChange calls listener after mutations IN THIS PROCESS commit, for the
// given operations ([OpSet], [OpDelete], [OpFlush]; all three if none are
// given). It returns an unsubscribe function; once that returns, listener
// is never called again.
//
// In-process only, with no SQLite involvement and effectively zero latency.
// A record arrives only after its transaction commits (a rolled-back
// SetMany notifies nobody), on a dispatch goroutine that holds no cache
// lock, so a slow listener never stalls writers and a listener may write to
// the cache itself. A panicking listener is recovered and logged; the write
// already committed. Writes made by a listener can trigger listeners again;
// past 8 levels those records are dropped with a warning. TTL expiry and
// eviction are not reported. The dispatch goroutine starts with the first
// listener and stops on [Cache.Close].
func (c *Cache) OnChange(listener func(ChangeRecord), ops ...string) (unsubscribe func(), err error) {
	if len(ops) == 0 {
		ops = allOps
	}
	set, err := validOps(ops)
	if err != nil {
		return nil, err
	}
	c.dispatcherMu.Lock()
	d := c.dispatcher.Load()
	if d == nil {
		d = newDispatcher(c)
		c.dispatcher.Store(d)
	}
	c.dispatcherMu.Unlock()
	return d.add(listener, set), nil
}

type listenerEntry struct {
	fn  func(ChangeRecord)
	ops map[string]bool
}

type queuedRecord struct {
	rec   ChangeRecord
	depth int
}

type dispatcher struct {
	cache     *Cache
	mu        sync.Mutex
	cond      *sync.Cond
	listeners map[int]listenerEntry
	nextID    int
	queue     []queuedRecord
	closed    bool
	done      chan struct{}
	gid       atomic.Int64
	depth     int // only touched by the dispatch goroutine
}

func newDispatcher(c *Cache) *dispatcher {
	d := &dispatcher{cache: c, listeners: map[int]listenerEntry{}, done: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	go d.run()
	return d
}

func (d *dispatcher) add(fn func(ChangeRecord), ops map[string]bool) func() {
	d.mu.Lock()
	id := d.nextID
	d.nextID++
	d.listeners[id] = listenerEntry{fn, ops}
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		delete(d.listeners, id)
		d.mu.Unlock()
	}
}

func (d *dispatcher) wants(op string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, l := range d.listeners {
		if l.ops[op] {
			return true
		}
	}
	return false
}

// submit queues a record. A record produced on the dispatch goroutine
// itself (by a listener's own write) is one level deeper than the record
// being dispatched; Go has no goroutine-locals, so the dispatch goroutine
// is recognized by its id.
func (d *dispatcher) submit(rec ChangeRecord) {
	depth := 1
	if goroutineID() == d.gid.Load() {
		depth = d.depth + 1
	}
	if depth > maxDispatchDepth {
		d.cache.logger.Warn(fmt.Sprintf("lytecache: dropping %s change record: OnChange listeners re-entered the cache %d levels deep",
			rec.Op, maxDispatchDepth))
		return
	}
	d.mu.Lock()
	if !d.closed {
		d.queue = append(d.queue, queuedRecord{rec, depth})
		d.cond.Signal()
	}
	d.mu.Unlock()
}

func (d *dispatcher) run() {
	defer close(d.done)
	d.gid.Store(goroutineID())
	for {
		d.mu.Lock()
		for len(d.queue) == 0 && !d.closed {
			d.cond.Wait()
		}
		if d.closed {
			d.mu.Unlock()
			return
		}
		item := d.queue[0]
		d.queue = d.queue[1:]
		var targets []int
		for id, l := range d.listeners {
			if l.ops[item.rec.Op] {
				targets = append(targets, id)
			}
		}
		d.mu.Unlock()
		sort.Ints(targets) // registration order

		d.depth = item.depth
		for _, id := range targets {
			d.call(id, item.rec)
		}
		d.depth = 0
	}
}

func (d *dispatcher) call(id int, rec ChangeRecord) {
	defer func() {
		if r := recover(); r != nil {
			d.cache.logger.Error("lytecache: OnChange listener panicked", "panic", r)
		}
	}()
	// Re-check: an unsubscribe since the snapshot means "never call again".
	d.mu.Lock()
	l, still := d.listeners[id]
	d.mu.Unlock()
	if still {
		l.fn(rec)
	}
}

func (d *dispatcher) close() {
	d.mu.Lock()
	d.closed = true
	d.listeners = map[int]listenerEntry{}
	d.cond.Broadcast()
	d.mu.Unlock()
	if goroutineID() == d.gid.Load() {
		return // Close called from inside a listener
	}
	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
		d.cache.logger.Warn("lytecache: an OnChange listener is still running at Close")
	}
}

// goroutineID parses the current goroutine's id from its stack header
// ("goroutine 123 [running]:"). Used only by the depth guard, and only
// while listeners are registered.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	field := bytes.Fields(buf[len("goroutine "):n])
	if len(field) == 0 {
		return -1
	}
	id, err := strconv.ParseInt(string(field[0]), 10, 64)
	if err != nil {
		return -1
	}
	return id
}
