package io.litecache;

import java.nio.file.Path;

/**
 * Test helper (not a JUnit test): launched as a separate JVM process by ConcurrencyProcessTest to
 * verify that {@link LiteCache#incr} is atomic across processes sharing one SQLite file, not just
 * threads within one JVM.
 *
 * <p>Args: {@code <dbPath> <key> <iterations>}. Increments {@code key} by 1, {@code iterations}
 * times, then exits.
 */
public final class IncrWorker {
    private IncrWorker() {}

    public static void main(String[] args) throws Exception {
        Path dbPath = Path.of(args[0]);
        String key = args[1];
        int iterations = Integer.parseInt(args[2]);

        try (LiteCache cache = LiteCache.builder().path(dbPath).sweepInterval(null).build()) {
            for (int i = 0; i < iterations; i++) {
                cache.incr(key);
            }
        }
    }
}
