package io.litecache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for eviction policies.
 */
public class EvictionTest {
    @TempDir
    Path tempDir;
    private Path dbPath;

    @BeforeEach
    public void setUp() {
        dbPath = tempDir.resolve("cache.db");
    }

    @Test
    public void testLRUEviction() {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .maxKeys(3)
                .eviction(Eviction.LRU)
                .sweepInterval(Duration.ofMillis(100))
                .build()) {
            
            // last_accessed has millisecond resolution (part of the cross-language wire format,
            // see SPEC.md); sleep between operations so ordering isn't decided by tie-breaking.
            cache.set("a", "1");
            Thread.sleep(5);
            cache.set("b", "2");
            Thread.sleep(5);
            cache.set("c", "3");
            Thread.sleep(5);

            // Access 'a' to make it most recently used
            cache.getString("a");
            Thread.sleep(5);

            // Set 'd' - should evict 'b' (least recently used)
            cache.set("d", "4");
            
            Thread.sleep(200); // Let sweep run
            
            assertThat(cache.exists("a")).isTrue();
            assertThat(cache.exists("b")).isFalse(); // Evicted
            assertThat(cache.exists("c")).isTrue();
            assertThat(cache.exists("d")).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testTTLEviction() {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .maxKeys(10)
                .eviction(Eviction.TTL)
                .sweepInterval(Duration.ofMillis(100))
                .build()) {
            
            cache.set("short", "1", Duration.ofMillis(100));
            cache.set("long", "2", Duration.ofSeconds(10));
            
            Thread.sleep(200); // Wait for short TTL to expire
            
            assertThat(cache.exists("short")).isFalse();
            assertThat(cache.exists("long")).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testRandomEviction() {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .maxKeys(3)
                .eviction(Eviction.RANDOM)
                .sweepInterval(Duration.ofMillis(100))
                .build()) {
            
            cache.set("a", "1");
            cache.set("b", "2");
            cache.set("c", "3");
            
            // Add 'd' - should randomly evict one
            cache.set("d", "4");
            
            Thread.sleep(200);
            
            // Should have exactly 3 keys
            long count = cache.keys("*").count();
            assertThat(count).isEqualTo(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testNoEvictionThrows() {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .maxKeys(3)
                .eviction(Eviction.NOEVICTION)
                .build()) {
            
            cache.set("a", "1");
            cache.set("b", "2");
            cache.set("c", "3");
            
            assertThatThrownBy(() -> cache.set("d", "4"))
                    .isInstanceOf(CacheFullException.class);
        }
    }

    @Test
    public void testMaxBytesEviction() {
        try (LiteCache cache = LiteCache.builder()
                .path(dbPath)
                .maxBytes(100) // 100 bytes
                .eviction(Eviction.LRU)
                .sweepInterval(Duration.ofMillis(100))
                .build()) {
            
            cache.set("small", "x"); // ~1 byte
            Thread.sleep(50);
            cache.set("large", "y".repeat(50)); // 50 bytes
            Thread.sleep(50);
            cache.set("xlarge", "z".repeat(60)); // 60 bytes
            Thread.sleep(200);
            
            // Should have evicted something
            long sizeBytes = cache.stats().sizeBytes();
            assertThat(sizeBytes).isLessThan(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
