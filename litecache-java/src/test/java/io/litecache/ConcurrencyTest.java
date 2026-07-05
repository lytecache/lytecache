package io.litecache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * Concurrency stress tests for LiteCache.
 */
public class ConcurrencyTest {
    @TempDir
    Path tempDir;
    private Path dbPath;

    @BeforeEach
    public void setUp() {
        dbPath = tempDir.resolve("cache.db");
    }

    @Test
    public void testThreadSafeIncrementAcrossThreads() throws Exception {
        try (LiteCache cache = LiteCache.builder().path(dbPath).build()) {
            int threadCount = 10;
            int incrementsPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < incrementsPerThread; j++) {
                            cache.incr("counter");
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            long finalValue = cache.getLong("counter");
            assertThat(finalValue).isEqualTo((long) threadCount * incrementsPerThread);
        }
    }

    @Test
    public void testThreadSafeMixedOperations() throws Exception {
        try (LiteCache cache = LiteCache.builder().path(dbPath).build()) {
            int threadCount = 20;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int op = 0; op < 50; op++) {
                            cache.set("key_" + threadId, "value_" + op);
                            cache.get("key_" + threadId, String.class);
                            cache.incr("global_counter");
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            long counter = cache.getLong("global_counter");
            assertThat(counter).isEqualTo((long) threadCount * 50);
        }
    }

    @Test
    public void testConcurrentExpirationAndAccess() throws Exception {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .sweepInterval(Duration.ofMillis(100))
                .build()) {
            
            ExecutorService executor = Executors.newFixedThreadPool(5);
            
            // Writer thread: adds keys with TTL
            executor.submit(() -> {
                for (int i = 0; i < 50; i++) {
                    cache.set("ttl_" + i, "value_" + i, Duration.ofMillis(300));
                    try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            });
            
            // Reader thread: attempts to read
            executor.submit(() -> {
                for (int i = 0; i < 100; i++) {
                    cache.get("ttl_" + (i % 50), String.class);
                    try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            });
            
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testHighContention() throws Exception {
        try (LiteCache cache = LiteCache.builder().path(dbPath).build()) {
            int threadCount = 50;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicLong errors = new AtomicLong(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        for (int op = 0; op < 100; op++) {
                            try {
                                cache.incr("hotkey", 1);
                                cache.get("hotkey", Long.class);
                            } catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            // Should complete with no errors
            assertThat(errors.get()).isEqualTo(0);
            assertThat(cache.getLong("hotkey")).isEqualTo((long) threadCount * 100);
        }
    }
}
