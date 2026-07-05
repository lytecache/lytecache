package io.litecache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the distributed cache lock (in-process; see ConcurrencyProcessTest for the
 * cross-process exclusivity guarantee).
 */
public class LockTest {
    @TempDir
    Path tempDir;
    private Path dbPath;
    private LiteCache cache;

    @BeforeEach
    public void setUp() {
        dbPath = tempDir.resolve("cache.db");
        cache = LiteCache.builder().path(dbPath).build();
    }

    @AfterEach
    public void tearDown() {
        if (cache != null) {
            cache.close();
        }
    }

    @Test
    public void testLockAcquisition() {
        try (CacheLock lock = cache.lock("mylock", Duration.ofSeconds(5))) {
            // Should have acquired; the lock is stored under a "lock:" prefixed key
            assertThat(cache.exists("lock:mylock")).isTrue();
        }
        // Should be released
        assertThat(cache.exists("lock:mylock")).isFalse();
    }

    @Test
    public void testLockTimeout() {
        try (CacheLock lock1 = cache.lock("shared", Duration.ofSeconds(5))) {
            // First lock acquired
            assertThat(cache.exists("lock:shared")).isTrue();

            // Second lock should timeout
            assertThatThrownBy(() -> cache.lock("shared", Duration.ofMillis(100)))
                    .isInstanceOf(LockTimeoutException.class);
        }
    }

    @Test
    public void testLockReacquisitionAfterRelease() {
        cache.lock("reentrant", Duration.ofSeconds(5)).close();
        // Once released, a new acquisition should succeed immediately.
        try (CacheLock lock = cache.lock("reentrant", Duration.ofSeconds(1))) {
            assertThat(cache.exists("lock:reentrant")).isTrue();
        }
    }

    @Test
    public void testReleaseOnlyRemovesOwnLock() {
        CacheLock lock1 = cache.lock("owned", Duration.ofMillis(50));
        // Simulate lock1's TTL expiring and someone else acquiring it in the meantime.
        cache.delete("lock:owned");
        try (CacheLock lock2 = cache.lock("owned", Duration.ofSeconds(1))) {
            lock1.close(); // must be a no-op: lock1's token no longer matches the stored value
            assertThat(cache.exists("lock:owned"))
                    .as("lock1.close() must not delete lock2's active lock")
                    .isTrue();
        }
    }

    @Test
    public void testLockAutoRelease() {
        String lockName = "autolock";
        {
            CacheLock lock = cache.lock(lockName, Duration.ofSeconds(5));
            assertThat(cache.exists("lock:" + lockName)).isTrue();
            lock.close();
        }
        // Lock should be released after close
        assertThat(cache.exists("lock:" + lockName)).isFalse();
    }

    @Test
    public void testConcurrentLockContention() throws InterruptedException {
        String lockName = "contended";
        int threadCount = 5;
        int[] acquired = new int[threadCount];

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            threads[i] = new Thread(() -> {
                try (CacheLock lock = cache.lock(lockName, Duration.ofSeconds(10))) {
                    acquired[idx] = 1;
                    Thread.sleep(50); // Hold lock
                } catch (Exception e) {
                    acquired[idx] = 0;
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join(10_000);
        }

        int acquiredCount = 0;
        for (int a : acquired) {
            if (a == 1) acquiredCount++;
        }
        assertThat(acquiredCount).isEqualTo(threadCount);
        assertThat(cache.exists("lock:" + lockName)).isFalse();
    }
}
