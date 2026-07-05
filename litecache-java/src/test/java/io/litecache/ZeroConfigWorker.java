package io.litecache;

/**
 * Test helper (not a JUnit test): launched as a separate JVM process by ZeroConfigTest to verify
 * that the {@code LITECACHE_PATH} environment variable override -- which can only be exercised by
 * actually setting an environment variable for a process, not by mutating a running JVM's own
 * environment -- is honored by the zero-argument {@code new LiteCache()} constructor.
 *
 * <p>Prints the resolved {@link LiteCache#defaultPath()}, writes a key, and exits.
 */
public final class ZeroConfigWorker {
    private ZeroConfigWorker() {}

    public static void main(String[] args) {
        System.out.println(LiteCache.defaultPath());
        try (LiteCache cache = new LiteCache()) {
            cache.set("from-worker", "hello");
        }
    }
}
