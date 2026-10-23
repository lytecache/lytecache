package io.lytecache;

import java.nio.file.Path;

/**
 * Test helper (not a JUnit test): {@code <dbPath> <hset|hincrby> <key> <field> <iterations>}.
 */
public final class HashWorker {
    private HashWorker() {}

    public static void main(String[] args) {
        try (LyteCache cache = LyteCache.builder().path(Path.of(args[0])).sweepInterval(null).build()) {
            int n = Integer.parseInt(args[4]);
            for (int i = 0; i < n; i++) {
                if (args[1].equals("hset")) {
                    cache.hset(args[2], args[3], i);
                } else {
                    cache.hincrBy(args[2], args[3], 1);
                }
            }
        }
    }
}
