package io.lytecache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Test helper (not a JUnit test). Modes:
 * {@code subscribe <pattern> <count> <outFile> <readyFile>}, {@code publish <count>},
 * {@code cursorsub <readyFile>} (handles "jobs" forever, saving its cursor; killed by the test).
 */
public final class EventWorker {
    private EventWorker() {}

    public static void main(String[] args) throws Exception {
        try (LyteCache cache = LyteCache.builder().path(Path.of(args[0])).sweepInterval(null).build()) {
            switch (args[1]) {
                case "subscribe" -> {
                    int count = Integer.parseInt(args[3]);
                    List<String> lines = new ArrayList<>();
                    try (Subscription sub = cache.watch(args[2])) {
                        Files.writeString(Path.of(args[5]), "ready");
                        while (lines.size() < count) {
                            Event e = sub.poll(Duration.ofSeconds(30));
                            if (e == null) {
                                System.exit(2);
                            }
                            lines.add(e.seq() + " " + e.channel() + " " + e.payload());
                        }
                    }
                    Files.write(Path.of(args[4]), lines);
                }
                case "publish" -> {
                    int n = Integer.parseInt(args[2]);
                    for (int i = 0; i < n; i++) {
                        cache.notify("orders:new", i);
                        cache.notify("unrelated", i);
                    }
                }
                case "cursorsub" -> {
                    OptionalLong from = cache.loadCursor("worker");
                    WatchOptions opts = from.isPresent() ? WatchOptions.defaults().fromSeq(from.getAsLong()) : WatchOptions.defaults();
                    try (Subscription sub = cache.watch(opts, "jobs")) {
                        Files.writeString(Path.of(args[2]), "ready");
                        for (Event e : sub) {
                            cache.saveCursor("worker", e.seq());
                        }
                    }
                }
                default -> System.exit(3);
            }
        }
    }
}
