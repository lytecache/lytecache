package io.litecache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for LiteCache.
 */
public class LiteCacheTest {
    @TempDir
    Path tempDir;
    private Path dbPath;
    private LiteCache cache;

    @BeforeEach
    public void setUp() {
        dbPath = tempDir.resolve("cache.db");
        cache = LiteCache.builder()
                .path(dbPath)
                .sweepInterval(Duration.ofMillis(500))
                .build();
    }

    @AfterEach
    public void tearDown() {
        if (cache != null) {
            cache.close();
        }
    }

    // ============ Basic Set/Get Tests ============

    @Test
    public void testZeroConfigCreation() {
        // Should work with no arguments
        try (LiteCache c = new LiteCache()) {
            c.set("key", "value");
            assertThat(c.getString("key")).isEqualTo("value");
        }
    }

    @Test
    public void testStringSet() {
        cache.set("key", "value");
        assertThat(cache.getString("key")).isEqualTo("value");
    }

    @Test
    public void testLongSet() {
        cache.set("counter", 42L);
        assertThat(cache.getLong("counter")).isEqualTo(42L);
    }

    @Test
    public void testDoubleSet() {
        cache.set("pi", 3.14159);
        assertThat(cache.getDouble("pi")).isCloseTo(3.14159, within(0.00001));
    }

    @Test
    public void testBytesSet() {
        byte[] data = {1, 2, 3, 4, 5};
        cache.set("bytes", data);
        assertThat(cache.getBytes("bytes")).isEqualTo(data);
    }

    @Test
    public void testObjectSerialization() {
        Map<String, Object> obj = Map.of("name", "Alice", "age", 30);
        cache.set("user", obj);
        Map<String, Object> retrieved = cache.get("user", new TypeReference<Map<String, Object>>() {});
        assertThat(retrieved).containsEntry("name", "Alice");
    }

    @Test
    public void testTypeReferenceGenericList() {
        List<Map<String, Object>> obj = List.of(Map.of("id", 1), Map.of("id", 2));
        cache.set("items", obj);
        List<Map<String, Object>> retrieved = cache.get("items", new TypeReference<List<Map<String, Object>>>() {});
        assertThat(retrieved).hasSize(2);
        assertThat(retrieved.get(0)).containsEntry("id", 1);
    }

    @Test
    public void testNullReturn() {
        assertThat(cache.getString("nonexistent")).isNull();
        assertThat(cache.getLong("nonexistent")).isNull();
    }

    @Test
    public void testOptionalVariant() {
        cache.set("opt", "value");
        assertThat(cache.findString("opt")).contains("value");
        assertThat(cache.findString("missing")).isEmpty();
    }

    // ============ Delete Tests ============

    @Test
    public void testDelete() {
        cache.set("a", "1");
        cache.set("b", "2");
        int deleted = cache.delete("a", "b");
        assertThat(deleted).isEqualTo(2);
        assertThat(cache.exists("a")).isFalse();
    }

    @Test
    public void testExists() {
        cache.set("key", "value");
        assertThat(cache.exists("key")).isTrue();
        assertThat(cache.exists("missing")).isFalse();
    }

    // ============ Add/Replace Tests ============

    @Test
    public void testAdd() {
        assertThat(cache.add("key", "value", null)).isTrue();
        assertThat(cache.add("key", "newvalue", null)).isFalse();
        assertThat(cache.getString("key")).isEqualTo("value");
    }

    @Test
    public void testReplace() {
        assertThat(cache.replace("key", "value", null)).isFalse();
        cache.set("key", "old");
        assertThat(cache.replace("key", "new", null)).isTrue();
        assertThat(cache.getString("key")).isEqualTo("new");
    }

    @Test
    public void testGetSet() {
        cache.set("key", "old");
        String prev = cache.getSet("key", "new");
        assertThat(prev).isEqualTo("old");
        assertThat(cache.getString("key")).isEqualTo("new");
    }

    // ============ TTL Tests ============

    @Test
    public void testTTLSet() throws InterruptedException {
        cache.set("ttl_key", "value", Duration.ofMillis(200));
        assertThat(cache.getString("ttl_key")).isEqualTo("value");
        Thread.sleep(300);
        assertThat(cache.getString("ttl_key")).isNull();
    }

    @Test
    public void testExpire() {
        cache.set("key", "value");
        assertThat(cache.expire("key", Duration.ofMillis(200))).isTrue();
        assertThat(cache.ttl("key")).isNotNull();
    }

    @Test
    public void testPersist() {
        cache.set("key", "value", Duration.ofSeconds(10));
        assertThat(cache.persist("key")).isTrue();
        Duration ttl = cache.ttl("key");
        assertThat(ttl).isEqualTo(Duration.ofSeconds(-1));
    }

    @Test
    public void testTTLReturnsCorrectValues() {
        cache.set("key", "value");
        assertThat(cache.ttl("key")).isEqualTo(Duration.ofSeconds(-1)); // No expiry
        assertThat(cache.ttl("missing")).isNull(); // Key doesn't exist
    }

    @Test
    public void testTouch() {
        cache.set("key", "value", Duration.ofMillis(100));
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(cache.touch("key", Duration.ofMillis(200))).isTrue();
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Should still be there because we refreshed
        assertThat(cache.getString("key")).isEqualTo("value");
    }

    // ============ Atomic Counter Tests ============

    @Test
    public void testIncr() {
        assertThat(cache.incr("counter")).isEqualTo(1);
        assertThat(cache.incr("counter")).isEqualTo(2);
        assertThat(cache.incr("counter", 5)).isEqualTo(7);
    }

    @Test
    public void testDecr() {
        cache.set("counter", 10);
        assertThat(cache.decr("counter")).isEqualTo(9);
        assertThat(cache.decr("counter", 3)).isEqualTo(6);
    }

    @Test
    public void testIncrDouble() {
        assertThat(cache.incrDouble("price", 1.5)).isCloseTo(1.5, within(0.001));
        assertThat(cache.incrDouble("price", 2.5)).isCloseTo(4.0, within(0.001));
    }

    @Test
    public void testIncrOnNonNumeric() {
        cache.set("key", "not_a_number");
        assertThatThrownBy(() -> cache.incr("key"))
                .isInstanceOf(IllegalStateException.class);
    }

    // ============ SetAll/GetAll Tests ============

    @Test
    public void testSetAll() {
        Map<String, String> entries = Map.of("a", "1", "b", "2", "c", "3");
        cache.setAll(entries, Duration.ofHours(1));
        assertThat(cache.getString("a")).isEqualTo("1");
        assertThat(cache.getString("b")).isEqualTo("2");
    }

    @Test
    public void testGetAll() {
        cache.set("a", "1");
        cache.set("b", "2");
        cache.set("c", "3");
        Map<String, String> result = cache.getAll(Arrays.asList("a", "b", "c", "missing"));
        assertThat(result).containsEntry("a", "1").containsEntry("b", "2");
        assertThat(result).doesNotContainKey("missing");
    }

    // ============ Key Scanning Tests ============

    @Test
    public void testKeysPattern() {
        cache.set("user:1", "Alice");
        cache.set("user:2", "Bob");
        cache.set("post:1", "Post1");
        
        List<String> keys = cache.keys("user:*").toList();
        assertThat(keys).containsExactlyInAnyOrder("user:1", "user:2");
    }

    // ============ Flush Tests ============

    @Test
    public void testFlush() {
        cache.set("a", "1");
        cache.set("b", "2");
        cache.flush();
        assertThat(cache.getString("a")).isNull();
        assertThat(cache.getString("b")).isNull();
    }

    // ============ Stats Tests ============

    @Test
    public void testStats() {
        cache.set("key1", "value1");
        cache.set("key2", "value2");
        cache.getString("key1");
        cache.getString("key1");
        cache.getString("missing");

        CacheStats stats = cache.stats();
        assertThat(stats.keyCount()).isEqualTo(2);
        assertThat(stats.hits()).isEqualTo(2);
        assertThat(stats.misses()).isEqualTo(1);
        assertThat(stats.hitRate()).isCloseTo(66.67, within(1.0));
        assertThat(stats.path()).isEqualTo(dbPath);
    }

    // ============ Persistence Tests ============

    @Test
    public void testPersistence() {
        cache.set("persistent", "value");
        cache.close();

        try (LiteCache cache2 = LiteCache.builder().path(dbPath).build()) {
            assertThat(cache2.getString("persistent")).isEqualTo("value");
        }
    }

    // ============ Serialization Tests ============

    @Test
    public void testJsonSerializationWithTypes() {
        Map<String, Object> complexObj = new LinkedHashMap<>();
        complexObj.put("name", "Alice");
        complexObj.put("age", 30);
        complexObj.put("active", true);

        cache.set("obj", complexObj);
        JsonNode retrieved = cache.get("obj", JsonNode.class);
        assertThat(retrieved.get("name").asText()).isEqualTo("Alice");
        assertThat(retrieved.get("age").asInt()).isEqualTo(30);
    }

    @Test
    public void testListSerialization() {
        List<String> list = Arrays.asList("a", "b", "c");
        cache.set("list", list);
        List<Object> retrieved = cache.get("list", new TypeReference<List<Object>>() {});
        assertThat(retrieved).containsExactly("a", "b", "c");
    }

    // ============ Concurrency Tests ============

    @Test
    public void testConcurrentReads() throws InterruptedException {
        cache.set("key", "value");
        
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>();
        List<String> results = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                String val = cache.getString("key");
                results.add(val);
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertThat(results).hasSize(threadCount).allMatch(v -> v.equals("value"));
    }

    @Test
    public void testConcurrentWrites() throws InterruptedException {
        int threadCount = 10;
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                cache.set("key_" + idx, "value_" + idx);
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < threadCount; i++) {
            assertThat(cache.getString("key_" + i)).isEqualTo("value_" + i);
        }
    }

    // ============ Vacuum Test ============

    @Test
    public void testVacuum() {
        cache.set("key", "value");
        cache.delete("key");
        cache.vacuum();
        assertThat(Files.exists(dbPath)).isTrue();
    }

    // ============ Namespace Isolation ============

    @Test
    public void testNamespaceIsolation() {
        try (LiteCache ns1 = LiteCache.builder().path(dbPath).namespace("ns1").build();
             LiteCache ns2 = LiteCache.builder().path(dbPath).namespace("ns2").build()) {
            
            ns1.set("key", "value1");
            ns2.set("key", "value2");
            
            assertThat(ns1.getString("key")).isEqualTo("value1");
            assertThat(ns2.getString("key")).isEqualTo("value2");
        }
    }
}
