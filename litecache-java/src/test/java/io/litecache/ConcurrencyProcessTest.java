package io.litecache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies atomicity across separate OS processes sharing one SQLite file -- not just threads
 * within a single JVM. Each test launches real {@code java} subprocesses via {@link ProcessBuilder}
 * against a common database file, which is the only way to actually exercise SQLite's
 * cross-process locking rather than this library's in-process Java monitor locks.
 */
public class ConcurrencyProcessTest {
    @TempDir
    Path tempDir;

    @Test
    public void incrIsAtomicAcrossProcesses() throws Exception {
        Path dbPath = tempDir.resolve("cross-process.db");
        int processCount = 4;
        int iterationsPerProcess = 200;

        List<Process> processes = new ArrayList<>();
        for (int i = 0; i < processCount; i++) {
            processes.add(startJava(IncrWorker.class, dbPath.toString(), "counter",
                    String.valueOf(iterationsPerProcess)));
        }
        for (Process p : processes) {
            assertThat(p.waitFor(60, TimeUnit.SECONDS)).as("worker process should finish").isTrue();
            assertThat(p.exitValue()).as("worker process exit code").isZero();
        }

        try (LiteCache cache = LiteCache.builder().path(dbPath).sweepInterval(null).build()) {
            assertThat(cache.getLong("counter")).isEqualTo((long) processCount * iterationsPerProcess);
        }
    }

    @Test
    public void lockExcludesConcurrentHoldersAcrossProcesses() throws Exception {
        Path dbPath = tempDir.resolve("cross-process-lock.db");
        Path logFile = tempDir.resolve("lock-log.txt");
        Files.createFile(logFile);
        int processCount = 4;
        long holdMillis = 200;

        List<Process> processes = new ArrayList<>();
        for (int i = 0; i < processCount; i++) {
            processes.add(startJava(LockWorker.class, dbPath.toString(), "shared-lock",
                    logFile.toString(), String.valueOf(holdMillis)));
        }
        for (Process p : processes) {
            assertThat(p.waitFor(60, TimeUnit.SECONDS)).as("worker process should finish").isTrue();
            assertThat(p.exitValue()).as("worker process exit code").isZero();
        }

        List<String> lines = Files.readAllLines(logFile);
        assertThat(lines).hasSize(processCount * 2);

        // Pair up START/END lines per marker and assert no two holding windows overlap.
        record Window(long start, long end) {}
        List<Window> windows = new ArrayList<>();
        for (int i = 0; i < lines.size(); i += 2) {
            String[] startParts = lines.get(i).split(" ");
            String[] endParts = lines.get(i + 1).split(" ");
            assertThat(startParts[0]).isEqualTo("START");
            assertThat(endParts[0]).isEqualTo("END");
            assertThat(startParts[1]).as("START/END markers must pair up in log order").isEqualTo(endParts[1]);
            windows.add(new Window(Long.parseLong(startParts[2]), Long.parseLong(endParts[2])));
        }

        windows.sort((a, b) -> Long.compare(a.start(), b.start()));
        for (int i = 1; i < windows.size(); i++) {
            assertThat(windows.get(i).start())
                    .as("holder %d must not start before holder %d released the lock", i, i - 1)
                    .isGreaterThanOrEqualTo(windows.get(i - 1).end());
        }
    }

    private Process startJava(Class<?> mainClass, String... args) throws IOException {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass.getName());
        command.addAll(List.of(args));
        return new ProcessBuilder(command).inheritIO().start();
    }
}
