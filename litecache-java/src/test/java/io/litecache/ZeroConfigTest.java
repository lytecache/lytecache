package io.litecache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the "zero configuration" contract: {@code new LiteCache()} must work with no prior
 * file, no explicit path, and no init step -- auto-creating missing parent directories, honoring
 * {@code LITECACHE_PATH}, and deriving a distinct default file per working directory.
 */
public class ZeroConfigTest {
    @TempDir
    Path tempDir;

    @Test
    public void firstSetOnAFreshPathAutoCreatesMissingParentDirectories() {
        Path nested = tempDir.resolve("does/not/exist/yet/cache.db");
        assertThat(Files.exists(nested.getParent())).isFalse();

        try (LiteCache cache = LiteCache.builder().path(nested).build()) {
            cache.set("k", "v");
            assertThat(cache.getString("k")).isEqualTo("v");
        }

        assertThat(Files.exists(nested)).isTrue();
    }

    @Test
    public void differentWorkingDirectoriesResolveToDifferentDefaultPaths() {
        String originalUserDir = System.getProperty("user.dir");
        try {
            Path dirA = tempDir.resolve("project-a");
            Path dirB = tempDir.resolve("project-b");
            Files.createDirectories(dirA);
            Files.createDirectories(dirB);

            System.setProperty("user.dir", dirA.toString());
            Path pathA = LiteCache.defaultPath();

            System.setProperty("user.dir", dirB.toString());
            Path pathB = LiteCache.defaultPath();

            assertThat(pathA).isNotEqualTo(pathB);
            // Same directory queried twice must be stable.
            System.setProperty("user.dir", dirA.toString());
            assertThat(LiteCache.defaultPath()).isEqualTo(pathA);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.setProperty("user.dir", originalUserDir);
        }
    }

    @Test
    public void litecachePathEnvVarOverridesTheDefaultLocation() throws Exception {
        // The env var can only be exercised by actually setting a process environment variable,
        // which a running JVM cannot do to itself -- so this spawns a real subprocess.
        Path envPath = tempDir.resolve("env-override").resolve("cache.db");
        assertThat(Files.exists(envPath)).isFalse();

        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ZeroConfigWorker.class.getName());

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("LITECACHE_PATH", envPath.toString());
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = builder.start();

        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().reduce("", (a, b) -> a + b + "\n");
        }
        assertThat(p.waitFor(30, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).as("worker exit code; output was:%n%s", output).isZero();
        assertThat(output.trim()).isEqualTo(envPath.toString());

        assertThat(Files.exists(envPath)).as("LITECACHE_PATH should have been created").isTrue();
        try (LiteCache cache = LiteCache.builder().path(envPath).build()) {
            assertThat(cache.getString("from-worker")).isEqualTo("hello");
        }
    }
}
