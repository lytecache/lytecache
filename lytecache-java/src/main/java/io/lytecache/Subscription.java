package io.lytecache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A live, polled view of the {@code events} table for some channels, returned by
 * {@link LyteCache#watch(String...)}.
 *
 * <p><b>Single host only, polled, durable.</b> This is not Redis Pub/Sub: one background thread
 * per subscription polls the cache file on its own read-only connection (never taking the write
 * lock), every 50 ms by default while events flow and backing off to about 1 s while idle.
 * Delivery is at-least-once from your stored cursor, in {@code seq} order, to every subscriber;
 * handlers must be idempotent.
 *
 * <p>Consume it by pulling -- iterate it ({@code for (Event e : sub)}, blocking), or call
 * {@link #poll(Duration)} / {@link #tryNext()} -- or by pushing, with {@link #onEvent(Consumer)},
 * which delivers on the subscription's thread instead of queueing. Lost events surface as a
 * {@link GapException}: thrown from the pull methods, or passed to {@link #onGap(Consumer)}.
 * Always {@link #close()} it (it is {@link AutoCloseable}).
 */
public final class Subscription implements AutoCloseable, Iterable<Event> {
    private static final Logger logger = Logger.getLogger(Subscription.class.getName());
    private static final int BATCH = 500;
    private static final long MAX_IDLE_MS = 1000;
    private static final int QUEUE_CAPACITY = 10_000;
    private static final Object CLOSED = new Object();

    private final String dbUrl;
    private final String namespace;
    private final List<String> patterns;
    private final String filterSql;
    private final Map<String, Object> filterParams;
    private final long pollMs;
    private final Serializer serializer;
    private final Consumer<Subscription> onClose;
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread thread;

    private volatile boolean stopped;
    private volatile boolean closed;
    private volatile long lastSeq;
    private volatile Consumer<Event> pushHandler;
    private volatile Consumer<GapException> gapHandler;
    private long cursor; // poller thread only

    Subscription(String dbUrl, String namespace, List<String> patterns, long startSeq, GapException gap,
            Duration pollInterval, Serializer serializer, Consumer<Subscription> onClose) {
        this.dbUrl = dbUrl;
        this.namespace = namespace;
        this.patterns = List.copyOf(patterns);
        this.filterParams = new HashMap<>();
        this.filterSql = channelFilter(patterns, filterParams);
        this.cursor = startSeq;
        this.lastSeq = startSeq;
        this.pollMs = Math.max(1, pollInterval.toMillis());
        this.serializer = serializer;
        this.onClose = onClose;
        if (gap != null) {
            queue.add(gap);
            this.thread = null;
        } else {
            this.thread = new Thread(this::run, "lytecache-watch");
            this.thread.setDaemon(true);
            this.thread.start();
        }
    }

    /**
     * SQL for the channel-matching rule: an exact name, or a trailing {@code *} prefix glob;
     * {@code *} alone matches everything; {@code *} anywhere else is literal.
     */
    static String channelFilter(List<String> patterns, Map<String, Object> params) {
        List<String> clauses = new ArrayList<>();
        for (int i = 0; i < patterns.size(); i++) {
            String p = patterns.get(i);
            if (p == null || p.isEmpty()) {
                throw new IllegalArgumentException("watch patterns must be non-empty");
            }
            if (p.endsWith("*")) {
                String prefix = p.substring(0, p.length() - 1);
                if (prefix.isEmpty()) {
                    return "1";
                }
                clauses.add("substr(channel, 1, :n" + i + ") = :c" + i);
                params.put("n" + i, (long) prefix.codePointCount(0, prefix.length()));
                params.put("c" + i, prefix);
            } else {
                clauses.add("channel = :c" + i);
                params.put("c" + i, p);
            }
        }
        return "(" + String.join(" OR ", clauses) + ")";
    }

    /**
     * The channel patterns this subscription matches.
     *
     * @return the patterns
     */
    public List<String> patterns() {
        return patterns;
    }

    /**
     * The seq of the last event handed to you: the value to persist as a cursor once handled.
     *
     * @return the last delivered seq
     */
    public long lastSeq() {
        return lastSeq;
    }

    /**
     * Switches to push delivery: {@code handler} is called on the subscription's thread for each
     * event, instead of events being queued for the pull methods. Exceptions it throws are logged.
     *
     * @param handler the event handler
     * @return this subscription
     */
    public Subscription onEvent(Consumer<Event> handler) {
        this.pushHandler = handler;
        return this;
    }

    /**
     * Registers the handler for a {@link GapException} in push mode (the pull methods throw it
     * instead). Without one, a gap is logged at SEVERE.
     *
     * @param handler the gap handler
     * @return this subscription
     */
    public Subscription onGap(Consumer<GapException> handler) {
        this.gapHandler = handler;
        return this;
    }

    /**
     * Waits up to {@code timeout} for the next event.
     *
     * @param timeout how long to wait
     * @return the next event, or null on timeout or once closed
     * @throws GapException if events were lost to retention (the subscription then closes)
     */
    public Event poll(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (closed && queue.isEmpty()) {
                return null;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return null;
            }
            Object item;
            try {
                item = queue.poll(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(250)), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (item == null) {
                continue;
            }
            Event e = take(item);
            if (e != null || item == CLOSED) {
                return e;
            }
        }
    }

    /**
     * The next already-fetched event, without waiting.
     *
     * @return the event, or null if none is ready
     * @throws GapException if events were lost to retention (the subscription then closes)
     */
    public Event tryNext() {
        Object item = queue.poll();
        return item == null ? null : take(item);
    }

    private Event take(Object item) {
        if (item instanceof GapException gap) {
            close();
            throw gap;
        }
        if (item == CLOSED) {
            closed = true;
            return null;
        }
        Event e = (Event) item;
        lastSeq = e.seq();
        return e;
    }

    /**
     * A blocking iterator: {@code hasNext()} waits for the next event and returns false once the
     * subscription is closed. It throws {@link GapException} if events were lost to retention.
     *
     * @return the iterator
     */
    @Override
    public Iterator<Event> iterator() {
        return new Iterator<>() {
            private Event next;

            @Override
            public boolean hasNext() {
                while (next == null) {
                    if (closed && queue.isEmpty()) {
                        return false;
                    }
                    next = poll(Duration.ofMillis(250));
                }
                return true;
            }

            @Override
            public Event next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Event e = next;
                next = null;
                return e;
            }
        };
    }

    /**
     * Whether the subscription has been closed (by you, by the cache's close, or by a gap).
     *
     * @return true once closed
     */
    public boolean isClosed() {
        return closed;
    }

    /** Wakes the poller now (an in-process notify); the table remains the source of truth. */
    void wake() {
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

    /** Stops polling and releases the thread and connection. Idempotent. */
    @Override
    public void close() {
        synchronized (this) {
            if (stopped) {
                closed = true;
                return;
            }
            stopped = true;
        }
        closed = true;
        if (thread != null && thread != Thread.currentThread()) {
            LockSupport.unpark(thread);
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        queue.offer(CLOSED);
        onClose.accept(this);
    }

    private void run() {
        long interval = pollMs;
        NamedSql fetch = NamedSql.of("SELECT seq, channel, namespace, payload, value_type, created_at FROM events "
                + "WHERE seq > :cursor AND namespace = :ns AND " + filterSql + " ORDER BY seq LIMIT " + BATCH);
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            try (Statement s = conn.createStatement()) {
                s.execute("PRAGMA busy_timeout=5000");
                s.execute("PRAGMA query_only=1"); // a subscription never writes
            }
            while (!stopped) {
                List<Event> batch = new ArrayList<>();
                GapException gap = null;
                try {
                    gap = fetch(conn, fetch, batch);
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "lytecache: watch poll failed, retrying: " + e.getMessage());
                }
                if (gap != null) {
                    deliverGap(gap);
                    return;
                }
                for (Event e : batch) {
                    if (!deliver(e)) {
                        return;
                    }
                    cursor = e.seq();
                }
                if (!batch.isEmpty()) {
                    interval = pollMs;
                    if (batch.size() == BATCH) {
                        continue; // more are waiting: read on without sleeping
                    }
                } else {
                    interval = Math.min(interval * 2, Math.max(MAX_IDLE_MS, pollMs));
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(interval));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "lytecache: watch thread could not open its connection", e);
        } finally {
            closed = true;
            queue.offer(CLOSED);
        }
    }

    /** The gap check and the batch read run in one read transaction: one consistent snapshot. */
    private GapException fetch(Connection conn, NamedSql fetch, List<Event> out) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");
        }
        try {
            GapException gap = gapCheck(conn, cursor);
            if (gap != null) {
                return gap;
            }
            Map<String, Object> params = new HashMap<>(filterParams);
            params.put("cursor", cursor);
            params.put("ns", namespace);
            try (PreparedStatement ps = fetch.prepare(conn, params); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Event(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(6), rs.getBytes(4),
                            rs.getInt(5), serializer));
                }
            }
            return null;
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("COMMIT");
            }
        }
    }

    private boolean deliver(Event e) {
        Consumer<Event> handler = pushHandler;
        if (handler != null) {
            lastSeq = e.seq();
            try {
                handler.accept(e);
            } catch (RuntimeException ex) {
                logger.log(Level.WARNING, "lytecache: watch onEvent handler threw", ex);
            }
            return !stopped;
        }
        while (!stopped) {
            try {
                if (queue.offer(e, 100, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void deliverGap(GapException gap) {
        Consumer<GapException> handler = gapHandler;
        if (pushHandler != null) {
            if (handler != null) {
                handler.accept(gap);
            } else {
                logger.log(Level.SEVERE, "lytecache: watch subscription ended", gap);
            }
        }
        queue.offer(gap);
        stopped = true;
    }

    static long lastAssignedSeq(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("SELECT seq FROM sqlite_sequence WHERE name = 'events'")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    static Long oldestSeq(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT min(seq) FROM events")) {
            rs.next();
            long v = rs.getLong(1);
            return rs.wasNull() ? null : v;
        }
    }

    /**
     * SPEC.md's gap rule: a cursor older than the oldest retained event minus one has lost
     * events; one ahead of the last seq ever assigned means the file was recreated.
     */
    static GapException gapCheck(Connection conn, long cursor) throws SQLException {
        long last = lastAssignedSeq(conn);
        if (cursor > last) {
            return new GapException(cursor, last + 1, true);
        }
        Long oldest = oldestSeq(conn);
        long o = oldest == null ? last + 1 : oldest;
        return cursor < o - 1 ? new GapException(cursor, o, false) : null;
    }
}
