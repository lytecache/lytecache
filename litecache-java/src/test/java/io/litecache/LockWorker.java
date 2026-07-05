package io.litecache;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Test helper (not a JUnit test): launched as a separate JVM process by ConcurrencyProcessTest to
 * verify that {@link CacheLock} excludes concurrent holders across processes, not just threads
 * within one JVM.
 *
 * <p>Args: {@code <dbPath> <lockName> <logFile> <holdMillis>}. Acquires the named lock, appends a
 * "START" line (with a distinct marker for this process) to {@code logFile} guarded by an OS file
 * lock (so the append itself is atomic even though {@code logFile} is outside LiteCache), sleeps
 * {@code holdMillis} while holding the LiteCache lock, appends "END", then releases.
 */
public final class LockWorker {
    private LockWorker() {}

    public static void main(String[] args) throws Exception {
        Path dbPath = Path.of(args[0]);
        String lockName = args[1];
        Path logFile = Path.of(args[2]);
        long holdMillis = Long.parseLong(args[3]);
        String marker = ProcessHandle.current().pid() + "-" + System.nanoTime();

        try (LiteCache cache = LiteCache.builder().path(dbPath).sweepInterval(null).build()) {
            try (CacheLock lock = cache.lock(lockName, Duration.ofSeconds(30))) {
                appendLine(logFile, "START " + marker + " " + System.currentTimeMillis());
                Thread.sleep(holdMillis);
                appendLine(logFile, "END " + marker + " " + System.currentTimeMillis());
            }
        }
    }

    private static void appendLine(Path file, String line) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
                FileChannel channel = raf.getChannel()) {
            FileLock fileLock = channel.lock();
            try {
                raf.seek(raf.length());
                raf.write((line + System.lineSeparator()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } finally {
                fileLock.release();
            }
        }
    }
}
