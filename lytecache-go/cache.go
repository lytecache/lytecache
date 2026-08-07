// Package lytecache is an embedded, Redis-like cache backed by a SQLite
// file. It gives you the familiar Redis API surface -- Set/Get, TTLs,
// atomic counters, eviction, distributed locks -- with zero infrastructure:
// no server process, no port, no client to configure. Just a file.
//
// The zero-configuration form is the flagship way to use this package:
//
//	cache, err := lytecache.New()
//	if err != nil {
//		// handle err
//	}
//	defer cache.Close()
//
// New(), with no options, creates the database file (and any missing
// parent directories) on first use, at a default, per-project location.
// See [DefaultPath] for exactly where.
package lytecache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/sqlite" // registers the "sqlite" database/sql driver via its init()
)

// Eviction selects the policy used when a namespace exceeds [WithMaxKeys]
// or [WithMaxBytes].
type Eviction int

const (
	// LRU evicts the least-recently-used key first. This is the default.
	LRU Eviction = iota
	// TTLPolicy evicts the soonest-to-expire key first; keys with no TTL
	// are evicted last.
	TTLPolicy
	// Random evicts an arbitrary key.
	Random
	// NoEviction rejects a write that would grow the namespace past the
	// configured limit, returning an error wrapping [ErrCacheFull],
	// instead of evicting. Updating an existing key is always allowed,
	// since it never grows the dataset.
	NoEviction
)

// Cache is an embedded, Redis-like cache backed by a SQLite file.
//
// A *Cache is safe for concurrent use by multiple goroutines, like
// [*http.Client]. It is also safe for concurrent use by multiple OS
// processes sharing the same database file: every read-modify-write
// operation is a single SQL statement or an explicit transaction, so
// correctness holds across processes, not just goroutines.
type Cache struct {
	path          string
	namespace     string
	schemaVersion int

	// readDB allows multiple concurrent connections (safe under WAL mode,
	// where readers never block a writer or each other). writeDB is
	// capped at one connection, serializing this process's writes
	// application-side -- SQLite only has one writer at a time regardless,
	// and doing it this way avoids most in-process SQLITE_BUSY churn.
	readDB  *sql.DB
	writeDB *sql.DB

	maxKeys  *int64
	maxBytes *int64
	eviction Eviction
	strict   bool
	logger   *slog.Logger

	sweepInterval time.Duration
	opsSinceSweep atomic.Int64

	lruMu     sync.Mutex
	lruBuffer map[string]lruUpdate

	hits           atomic.Int64
	misses         atomic.Int64
	evictions      atomic.Int64
	expiredRemoved atomic.Int64

	sweepStop chan struct{}
	sweepDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

type lruUpdate struct {
	lastAccessed int64
	accessCount  int64
}

// opportunisticSweepEvery is how many operations pass between opportunistic
// maintenance sweeps when WithSweepInterval(0) disables the background
// goroutine.
const opportunisticSweepEvery = 100

// lruFlushEvery is how many buffered last_accessed/access_count updates
// accumulate before Get proactively flushes them, independent of the
// sweeper. Keeps memory bounded under a long-running, sweep-disabled cache.
const lruFlushEvery = 200

// Option configures a [Cache] constructed by [New].
type Option func(*options)

type options struct {
	path          string
	namespace     string
	maxKeys       *int64
	maxBytes      *int64
	eviction      Eviction
	sweepInterval time.Duration
	strict        bool
	logger        *slog.Logger
}

// WithPath sets an explicit database file path, overriding the default
// location computed by [DefaultPath]. This is an optional escape hatch --
// most programs should not need it.
func WithPath(path string) Option { return func(o *options) { o.path = path } }

// WithNamespace sets a logical partition within the database file. Two
// Cache instances pointed at the same file but different namespaces never
// see each other's keys.
func WithNamespace(namespace string) Option {
	return func(o *options) { o.namespace = namespace }
}

// WithMaxKeys evicts (per the configured [Eviction] policy) once the
// namespace exceeds this many keys. The default is no limit.
func WithMaxKeys(n int64) Option { return func(o *options) { o.maxKeys = &n } }

// WithMaxBytes evicts (per the configured [Eviction] policy) once the
// namespace exceeds this many bytes of stored value data. The default is
// no limit.
func WithMaxBytes(n int64) Option { return func(o *options) { o.maxBytes = &n } }

// WithEviction sets the eviction policy. The default is [LRU].
func WithEviction(e Eviction) Option { return func(o *options) { o.eviction = e } }

// WithSweepInterval sets how often the background sweeper removes expired
// keys and enforces eviction limits. The default is 60 seconds. A value of
// 0 disables the background goroutine entirely; maintenance then runs
// opportunistically, piggybacked on roughly every 100th operation instead.
func WithSweepInterval(d time.Duration) Option {
	return func(o *options) { o.sweepInterval = d }
}

// WithStrict controls how internal read errors are handled. When false
// (the default), a read that hits an internal error degrades to a miss and
// logs a warning via the configured logger, rather than failing the
// caller's request -- a cache should not be able to crash the host
// application. When true, such errors are returned to the caller. Writes
// always return errors, in both modes.
func WithStrict(strict bool) Option { return func(o *options) { o.strict = strict } }

// WithLogger sets the logger used for non-strict-mode warnings (see
// [WithStrict]). The default is [slog.Default].
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// New opens (creating if necessary) a Cache. With no options, it uses
// [DefaultPath] and sensible defaults for everything else -- this is the
// intended, zero-configuration way to use the package.
//
// If the database file does not exist, New creates it, including any
// missing parent directories, and applies the schema automatically. There
// is no separate init step.
func New(opts ...Option) (*Cache, error) {
	o := options{
		namespace:     "default",
		eviction:      LRU,
		sweepInterval: 60 * time.Second,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	path := o.path
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	} else {
		expanded, err := expandHome(path)
		if err != nil {
			return nil, err
		}
		path = expanded
	}

	if dir := filepath.Dir(path); dir != "." && dir != string(filepath.Separator) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("lytecache: creating database directory %s: %w", dir, err)
		}
	}

	dsn := path + "?" + pragmaDSNParams()

	writeDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("lytecache: opening database: %w", err)
	}
	writeDB.SetMaxOpenConns(1)

	version, err := initSchemaWithRetry(writeDB)
	if err != nil {
		_ = writeDB.Close()
		return nil, err
	}

	readDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("lytecache: opening database: %w", err)
	}
	readConns := runtime.GOMAXPROCS(0)
	if readConns < 4 {
		readConns = 4
	}
	readDB.SetMaxOpenConns(readConns)

	c := &Cache{
		path:          path,
		namespace:     o.namespace,
		schemaVersion: version,
		readDB:        readDB,
		writeDB:       writeDB,
		maxKeys:       o.maxKeys,
		maxBytes:      o.maxBytes,
		eviction:      o.eviction,
		strict:        o.strict,
		logger:        o.logger,
		sweepInterval: o.sweepInterval,
		lruBuffer:     make(map[string]lruUpdate),
	}

	if o.sweepInterval > 0 {
		c.sweepStop = make(chan struct{})
		c.sweepDone = make(chan struct{})
		go c.sweepLoop()
	}

	return c, nil
}

// pragmaDSNParams builds the modernc.org/sqlite DSN query string that
// applies our required PRAGMAs to every new connection the driver opens
// (see newConn/applyQueryParams in that package) -- this is what makes
// synchronous/foreign_keys/busy_timeout apply per-connection without us
// having to hook connection creation ourselves.
func pragmaDSNParams() string {
	return "_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
}

// sqliteBusyPrimary is SQLITE_BUSY from sqlite3.h -- result code 5, stable
// across every SQLite binding and version. Error.Code() may return an
// *extended* result code (e.g. SQLITE_BUSY_SNAPSHOT = 5 | (2<<8)), so
// callers should mask to the low byte before comparing, which
// isSQLiteBusy does.
const sqliteBusyPrimary = 5

func isSQLiteBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code()&0xff == sqliteBusyPrimary
	}
	return false
}

// initSchemaWithRetry applies the DDL and checks/records the schema
// version. Multiple OS processes creating the same brand-new WAL-mode file
// at the same moment can hit SQLITE_BUSY on the very first schema
// statements, before busy_timeout has any effect on that specific
// operation (a well-known SQLite cold-start race, not specific to this
// driver) -- so this retries a handful of times on that specific error
// before giving up.
func initSchemaWithRetry(db *sql.DB) (int, error) {
	const maxAttempts = 25
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var version int
		if version, err = initSchema(db); err == nil {
			return version, nil
		}
		if !isSQLiteBusy(err) {
			return 0, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("lytecache: initializing schema after %d attempts: %w", maxAttempts, err)
}

func initSchema(db *sql.DB) (int, error) {
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return 0, fmt.Errorf("lytecache: applying schema: %w", err)
	}

	var versionText string
	err := db.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'schema_version'`).Scan(&versionText)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := db.ExecContext(ctx,
			`INSERT OR IGNORE INTO meta (k, v) VALUES ('schema_version', '1')`); err != nil {
			return 0, fmt.Errorf("lytecache: recording schema version: %w", err)
		}
		return 1, nil
	case err != nil:
		return 0, fmt.Errorf("lytecache: reading schema version: %w", err)
	}

	var version int
	if _, err := fmt.Sscanf(versionText, "%d", &version); err != nil {
		return 0, fmt.Errorf("lytecache: parsing schema version %q: %w", versionText, err)
	}
	if version > schemaVersion {
		return 0, fmt.Errorf("%w: file has schema_version=%d, this version of lytecache supports up to %d",
			ErrSchemaVersion, version, schemaVersion)
	}
	return version, nil
}

// Path returns this instance's actual database file path.
func (c *Cache) Path() string { return c.path }

// SchemaVersion returns the on-disk schema_version this file was opened
// with. It is always <= the version this build understands -- New refuses
// to open a file with a newer schema_version than it supports (see
// [ErrSchemaVersion]).
func (c *Cache) SchemaVersion() int { return c.schemaVersion }

// Limits reports the capacity limits this Cache was constructed with (see
// [WithMaxKeys], [WithMaxBytes]). A nil field means that limit was not set
// (unbounded). Intended for introspection/admin tooling; ordinary
// applications already know their own configured limits.
type Limits struct {
	MaxKeys  *int64
	MaxBytes *int64
}

// Limits returns this Cache's configured capacity limits.
func (c *Cache) Limits() Limits {
	l := Limits{}
	if c.maxKeys != nil {
		v := *c.maxKeys
		l.MaxKeys = &v
	}
	if c.maxBytes != nil {
		v := *c.maxBytes
		l.MaxBytes = &v
	}
	return l
}

// Close flushes any buffered state, stops the background sweeper if one is
// running, and closes the underlying database connections. Close is
// idempotent and safe to call more than once.
func (c *Cache) Close() error {
	c.closeOnce.Do(func() {
		if c.sweepStop != nil {
			close(c.sweepStop)
			<-c.sweepDone
		}
		c.flushLRUBuffer()

		var errs []error
		if err := c.writeDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing write connection: %w", err))
		}
		if err := c.readDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing read connection: %w", err))
		}
		if len(errs) > 0 {
			c.closeErr = fmt.Errorf("lytecache: %w", errors.Join(errs...))
		}
	})
	return c.closeErr
}

func nowMillis() int64 { return time.Now().UnixMilli() }

// maybeOpportunisticSweep runs a lightweight maintenance pass every
// opportunisticSweepEvery operations when the background sweeper goroutine
// is disabled (WithSweepInterval(0)).
func (c *Cache) maybeOpportunisticSweep() {
	if c.sweepInterval > 0 {
		return
	}
	if n := c.opsSinceSweep.Add(1); n%opportunisticSweepEvery == 0 {
		c.sweepOnce()
	}
}
