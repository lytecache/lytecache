package io.lytecache;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internal: in-process {@link LyteCache#onChange} delivery. One daemon thread, started with the
 * first listener; records are queued after their mutation commits, so a slow listener never holds
 * up a writer, and the thread holds no cache lock while calling listeners, so a listener may write
 * to the cache.
 */
final class ChangeDispatcher {
    static final int MAX_DEPTH = 8;
    private static final Logger logger = Logger.getLogger(ChangeDispatcher.class.getName());
    private static final Object STOP = new Object();

    private record Listener(Consumer<ChangeRecord> fn, Set<ChangeOp> ops) {}

    private record Queued(ChangeRecord record, int depth) {}

    private final Map<Integer, Listener> listeners = new ConcurrentSkipListMap<>();
    private final AtomicInteger nextId = new AtomicInteger();
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final ThreadLocal<Integer> depth = ThreadLocal.withInitial(() -> 0);
    private final Thread thread;
    private volatile boolean closed;

    ChangeDispatcher() {
        thread = new Thread(this::run, "lytecache-dispatch");
        thread.setDaemon(true);
        thread.start();
    }

    Runnable add(Consumer<ChangeRecord> fn, Set<ChangeOp> ops) {
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("onChange needs at least one ChangeOp");
        }
        int id = nextId.getAndIncrement();
        listeners.put(id, new Listener(fn, EnumSet.copyOf(ops)));
        return () -> listeners.remove(id);
    }

    boolean wants(ChangeOp op) {
        for (Listener l : listeners.values()) {
            if (l.ops().contains(op)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Queues a record. A record produced on the dispatch thread itself (by a listener's own write)
     * is one level deeper than the record being dispatched; past {@link #MAX_DEPTH} it is dropped.
     */
    void submit(ChangeRecord record) {
        if (closed) {
            return;
        }
        int d = depth.get() + 1;
        if (d > MAX_DEPTH) {
            logger.warning("lytecache: dropping " + record.op().wireName() + " change record: onChange listeners "
                    + "re-entered the cache " + MAX_DEPTH + " levels deep");
            return;
        }
        queue.add(new Queued(record, d));
    }

    private void run() {
        while (true) {
            Object item;
            try {
                item = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            if (item == STOP) {
                return;
            }
            Queued q = (Queued) item;
            depth.set(q.depth());
            try {
                for (Map.Entry<Integer, Listener> e : listeners.entrySet()) {
                    Listener l = listeners.get(e.getKey()); // re-check: unsubscribed means never again
                    if (l == null || !l.ops().contains(q.record().op())) {
                        continue;
                    }
                    try {
                        l.fn().accept(q.record());
                    } catch (RuntimeException ex) {
                        logger.log(Level.WARNING, "lytecache: onChange listener threw", ex);
                    }
                }
            } finally {
                depth.set(0);
            }
        }
    }

    void close() {
        closed = true;
        listeners.clear();
        queue.clear();
        queue.add(STOP);
        if (thread != Thread.currentThread()) {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                logger.warning("lytecache: an onChange listener is still running at close()");
            }
        }
    }
}
