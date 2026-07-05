package io.litecache;

import java.time.Duration;
import java.util.UUID;

/**
 * Process-safe distributed lock built on cache {@code add()} semantics: acquiring the lock is an
 * atomic {@code SET NX} on a {@code "lock:" + name} key, so at most one holder (across threads and
 * processes) can hold it at a time. Implements {@link AutoCloseable} so it can be used in
 * try-with-resources.
 *
 * <p>The lock's TTL equals the acquisition {@code timeout} passed to
 * {@link LiteCache#lock(String, Duration)}: if the holder crashes without releasing, the lock
 * expires and is available again rather than being stuck forever. Each acquisition writes a unique
 * random token as the lock's value, so release only ever deletes a lock this instance still owns --
 * never one that expired and was already re-acquired by someone else.
 */
public class CacheLock implements AutoCloseable {
    private static final long POLL_INTERVAL_MS = 50;

    private final LiteCache cache;
    private final String lockKey;
    private final Duration ttl;
    private final String token = UUID.randomUUID().toString();
    private volatile boolean acquired = false;

    CacheLock(LiteCache cache, String lockName, Duration timeout) {
        this.cache = cache;
        this.lockKey = "lock:" + lockName;
        this.ttl = timeout;
        acquire(timeout);
    }

    private void acquire(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            if (cache.tryAcquireLock(lockKey, token, ttl)) {
                acquired = true;
                return;
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new LockTimeoutException("Failed to acquire lock '" + lockKey + "' within " + timeout);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LockTimeoutException("Lock acquisition interrupted", e);
            }
        }
    }

    /**
     * Releases the lock, if this instance still holds it. Safe to call more than once.
     */
    public void release() {
        if (acquired) {
            acquired = false;
            cache.releaseLock(lockKey, token);
        }
    }

    @Override
    public void close() {
        release();
    }
}
