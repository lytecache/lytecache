package io.lytecache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Hash operations, run once on the JSON1 path and once with the fallback forced. */
public class HashTest {
    private static final List<String> WEIRD = List.of("a.b", "a[0]", "a]b", "q\"uote", "back\\slash", "\\\"", "sp ace",
            "ünï 字", "", "$", "*");

    @TempDir
    Path dir;

    private final List<LyteCache> opened = new ArrayList<>();

    @AfterEach
    void tearDown() {
        opened.forEach(LyteCache::close);
        System.clearProperty("lytecache.forceJsonFallback");
    }

    private LyteCache open(String mode) {
        if (mode.equals("fallback")) {
            System.setProperty("lytecache.forceJsonFallback", "1");
        } else {
            System.clearProperty("lytecache.forceJsonFallback");
        }
        LyteCache c = LyteCache.builder().path(dir.resolve("cache.db")).sweepInterval(null).build();
        opened.add(c);
        assertThat(c.hashMode()).isEqualTo(mode);
        assertThat(c.stats().hashMode()).isEqualTo(mode);
        return c;
    }

    private record Raw(byte[] value, int vt, Long exp, long size, long len, long la, long ac) {}

    private Raw raw(String key) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("cache.db"));
                PreparedStatement ps = conn.prepareStatement("SELECT value, value_type, expires_at, size_bytes, length(value), "
                        + "last_accessed, access_count FROM cache WHERE namespace='default' AND key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                long exp = rs.getLong(3);
                Long expBox = rs.wasNull() ? null : exp;
                return new Raw(rs.getBytes(1), rs.getInt(2), expBox, rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7));
            }
        }
    }

    private void insertRaw(String key, byte[] blob, int vt) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("cache.db"));
                PreparedStatement ps = conn.prepareStatement("INSERT INTO cache (namespace, key, value, value_type, created_at, "
                        + "expires_at, last_accessed, access_count, size_bytes) VALUES ('default', ?, ?, ?, 1, NULL, 1, 0, ?)")) {
            ps.setString(1, key);
            ps.setBytes(2, blob);
            ps.setInt(3, vt);
            ps.setInt(4, blob.length);
            ps.executeUpdate();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void roundTripsNaturalTypesAndReportsCreated(String mode) {
        LyteCache c = open(mode);
        assertThat(c.hset("h", "s", "text")).isTrue();
        assertThat(c.hset("h", "i", 42)).isTrue();
        assertThat(c.hset("h", "f", 1.5)).isTrue();
        assertThat(c.hset("h", "t", true)).isTrue();
        assertThat(c.hset("h", "n", null)).isTrue();
        assertThat(c.hset("h", "o", Map.of("x", List.of(1, 2)))).isTrue();
        assertThat(c.hset("h", "s", "again")).isFalse();
        assertThat(c.hget("h", "s")).isEqualTo("again");
        assertThat(c.hget("h", "i")).isEqualTo(42L);
        assertThat(c.hget("h", "f")).isEqualTo(1.5);
        assertThat(c.hget("h", "t")).isEqualTo(true);
        assertThat(c.hget("h", "n")).isNull();
        assertThat(c.hget("h", "o")).isEqualTo(Map.of("x", List.of(1L, 2L)));
        assertThat(c.hgetString("h", "s")).isEqualTo("again");
        assertThat(c.hgetLong("h", "i")).isEqualTo(42L);
        assertThat(c.hexists("h", "n")).isTrue();
        assertThat(c.hget("h", "missing")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void isAViewOverOrdinaryJsonValues(String mode) {
        LyteCache c = open(mode);
        c.set("u", Map.of("name", "Ada"));
        assertThat(c.hgetString("u", "name")).isEqualTo("Ada");
        c.hset("u", "status", "active");
        assertThat(c.get("u", Map.class)).isEqualTo(Map.of("name", "Ada", "status", "active"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void hsetManyHmgetHdelHkeysHvalsHlen(String mode) {
        LyteCache c = open(mode);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", 1);
        m.put("b", "two");
        m.put("c", null);
        c.hsetMany("h", m);
        assertThat(c.hmget("h", List.of("b", "nope", "a"))).containsExactly("two", null, 1L);
        assertThat(c.hkeys("h")).containsExactly("a", "b", "c");
        assertThat(c.hvals("h")).containsExactly(1L, "two", null);
        assertThat(c.hlen("h")).isEqualTo(3);
        assertThat(c.hdel("h", "a", "nope", "a")).isEqualTo(1);
        assertThat(c.hdel("h", "b", "c")).isEqualTo(2);
        assertThat(c.get("h", Map.class)).isEmpty();
        assertThat(c.exists("h")).isTrue();

        Map<String, Object> big = new LinkedHashMap<>();
        for (int i = 0; i < 1234; i++) {
            big.put("f" + i, i);
        }
        c.hsetMany("big", big);
        assertThat(c.hlen("big")).isEqualTo(1234);
        assertThat(c.hgetLong("big", "f777")).isEqualTo(777L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void hsetnxAndMissesCreateNothing(String mode) throws Exception {
        LyteCache c = open(mode);
        assertThat(c.hsetnx("h", "a", 1)).isTrue();
        assertThat(c.hsetnx("h", "a", 2)).isFalse();
        assertThat(c.hgetLong("h", "a")).isEqualTo(1L);
        assertThat(c.hget("missing", "f")).isNull();
        assertThat(c.hgetAll("missing")).isEmpty();
        assertThat(c.hkeys("missing")).isEmpty();
        assertThat(c.hlen("missing")).isZero();
        assertThat(c.hdel("missing", "f")).isZero();
        assertThat(raw("missing")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void missingKeysAreCreatedWithoutExpiryAndTtlIsIgnoredForExistingKeys(String mode) throws Exception {
        LyteCache c = open(mode);
        c.hset("plain", "f", 1);
        assertThat(raw("plain").exp()).isNull();
        c.hset("t", "f", 1, Duration.ofSeconds(100));
        Long exp = raw("t").exp();
        assertThat(exp).isNotNull();
        c.hset("t", "g", 2, Duration.ofSeconds(5));
        assertThat(raw("t").exp()).isEqualTo(exp);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void neverResurrectsAnExpiredKey(String mode) throws Exception {
        LyteCache c = open(mode);
        c.set("h", Map.of("old", 1), Duration.ofMillis(10));
        Thread.sleep(40);
        assertThat(c.hget("h", "old")).isNull();
        assertThat(c.hsetnx("h", "new", 1)).isTrue();
        assertThat(c.hgetAll("h")).isEqualTo(Map.of("new", 1L));
        assertThat(raw("h").exp()).isNull();
        c.set("n", Map.of("n", 100), Duration.ofMillis(10));
        Thread.sleep(40);
        assertThat(c.hincrBy("n", "n", 1)).isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void rejectsEveryNonJsonTypeCodeWithoutTouchingTheRow(String mode) throws Exception {
        LyteCache c = open(mode);
        Object[][] cases = {{new byte[] {0, 1}, 0}, {"hello".getBytes(), 1}, {"42".getBytes(), 2}, {"3.14".getBytes(), 3},
            {new byte[] {(byte) 0x80, 4}, 5}, {new byte[] {(byte) 0xac, (byte) 0xed}, 6}};
        for (Object[] tc : cases) {
            byte[] blob = (byte[]) tc[0];
            int vt = (int) tc[1];
            String key = "k" + vt;
            insertRaw(key, blob, vt);
            List<Runnable> ops = List.of(() -> c.hset(key, "f", 1), () -> c.hsetMany(key, Map.of("f", 1)),
                    () -> c.hsetnx(key, "f", 1), () -> c.hincrBy(key, "f", 1), () -> c.hincrByFloat(key, "f", 1),
                    () -> c.hdel(key, "f"), () -> c.hget(key, "f"), () -> c.hgetAll(key), () -> c.hkeys(key), () -> c.hvals(key), () -> c.hlen(key));
            for (Runnable op : ops) {
                assertThatThrownBy(op::run).isInstanceOf(SerializationException.class).hasMessageContaining("value_type=" + vt);
            }
            assertThat(raw(key).value()).isEqualTo(blob);
            assertThat(raw(key).vt()).isEqualTo(vt);
        }
        c.set("arr", List.of(1, 2));
        assertThatThrownBy(() -> c.hset("arr", "f", 1)).isInstanceOf(SerializationException.class)
                .hasMessageContaining("not a JSON object");
        insertRaw("bad", "{not json".getBytes(), 4);
        assertThatThrownBy(() -> c.hset("bad", "f", 1)).hasMessageContaining("malformed JSON");
        assertThatThrownBy(() -> c.hset("h", "f", new byte[] {1})).isInstanceOf(SerializationException.class);
        assertThatThrownBy(() -> c.hset("h", "f", Double.NaN)).isInstanceOf(SerializationException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void awkwardFieldNamesAddressExactlyOneTopLevelKey(String mode) {
        LyteCache c = open(mode);
        for (String f : WEIRD) {
            Map<String, Object> base = new LinkedHashMap<>();
            base.put("a", Map.of("b", "decoy"));
            base.put("other", 0);
            c.set("h", base);
            assertThat(c.hset("h", f, "v")).as(f).isTrue();
            assertThat(c.hgetString("h", f)).as(f).isEqualTo("v");
            Map<?, ?> doc = c.get("h", Map.class);
            assertThat(doc.get(f)).as(f).isEqualTo("v");
            assertThat(doc).as(f).hasSize(3);
            assertThat(c.hkeys("h")).as(f).contains(f);
            assertThat(c.hincrBy("h", f + "#n", 2)).as(f).isEqualTo(2L);
            assertThat(c.hdel("h", f)).as(f).isEqualTo(1);
        }
        Map<String, Object> all = new LinkedHashMap<>();
        for (int i = 0; i < WEIRD.size(); i++) {
            all.put(WEIRD.get(i), i);
        }
        c.set("all", Map.of());
        c.hsetMany("all", all);
        assertThat(c.hkeys("all")).containsExactlyElementsOf(WEIRD);
        assertThat(c.hdel("all", WEIRD.toArray(new String[0]))).isEqualTo(WEIRD.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void preservesTtlTracksSizeAndCountsOneAccessPerCall(String mode) throws Exception {
        LyteCache c = open(mode);
        c.set("h", Map.of("a", 1), Duration.ofMinutes(1));
        Raw r0 = raw("h");
        Thread.sleep(5);
        List<Runnable> ops = List.of(() -> c.hset("h", "b", "字字字字"), () -> c.hsetMany("h", Map.of("c", 3)),
                () -> c.hsetnx("h", "e", 4), () -> c.hincrBy("h", "n", 7), () -> c.hincrByFloat("h", "x", 1.25),
                () -> c.hdel("h", "a", "c"));
        for (Runnable op : ops) {
            op.run();
            Raw r = raw("h");
            assertThat(r.exp()).isEqualTo(r0.exp());
            assertThat(r.size()).isEqualTo(r.len()).isEqualTo(r.value().length);
        }
        Raw r = raw("h");
        assertThat(r.ac()).isEqualTo(r0.ac() + ops.size());
        assertThat(r.la()).isGreaterThan(r0.la());
        c.hget("h", "b");
        c.hgetAll("h");
        assertThat(raw("h").ac()).isEqualTo(r.ac()); // reads are buffered, not written
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void hincrBy(String mode) throws Exception {
        LyteCache c = open(mode);
        assertThat(c.hincrBy("h", "n", 1)).isEqualTo(1);
        assertThat(c.hincrBy("h", "n", 4)).isEqualTo(5);
        assertThat(c.hincrBy("h", "n", -10)).isEqualTo(-5);
        assertThat(new String(raw("h").value(), StandardCharsets.UTF_8)).isEqualTo("{\"n\":-5}");
        for (Object bad : new Object[] {"5", true, null, Map.of("a", 1), List.of(1), 1.5}) {
            c.hset("h", "bad", bad);
            assertThatThrownBy(() -> c.hincrBy("h", "bad", 1)).isInstanceOf(IllegalStateException.class);
        }
        c.hset("h", "max", Long.MAX_VALUE - 1);
        assertThat(c.hincrBy("h", "max", 1)).isEqualTo(Long.MAX_VALUE);
        byte[] before = raw("h").value();
        assertThatThrownBy(() -> c.hincrBy("h", "max", 1)).isInstanceOf(ArithmeticException.class);
        assertThat(raw("h").value()).isEqualTo(before);
        c.hset("h", "min", Long.MIN_VALUE + 1);
        assertThat(c.hincrBy("h", "min", -1)).isEqualTo(Long.MIN_VALUE);
        assertThatThrownBy(() -> c.hincrBy("h", "min", -1)).isInstanceOf(ArithmeticException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void hincrByFloatKeepsFullPrecision(String mode) {
        LyteCache c = open(mode);
        double a = 0.1;
        double b = 0.2;
        assertThat(c.hincrByFloat("h", "x", a)).isEqualTo(a);
        assertThat(c.hincrByFloat("h", "x", b)).isEqualTo(a + b);
        c.hset("h", "i", 2);
        assertThat(c.hincrByFloat("h", "i", 0.5)).isEqualTo(2.5);
        c.hset("h", "s", "str");
        assertThatThrownBy(() -> c.hincrByFloat("h", "s", 1)).isInstanceOf(IllegalStateException.class);
        c.hset("h", "huge", 1.7e308);
        assertThatThrownBy(() -> c.hincrByFloat("h", "huge", 1.7e308)).isInstanceOf(ArithmeticException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void storesCanonicalJsonAndReadsOtherEncoders(String mode) throws Exception {
        LyteCache c = open(mode);
        c.hset("h", "name", "Adé");
        c.hset("h", "n", 1);
        assertThat(new String(raw("h").value(), StandardCharsets.UTF_8)).isEqualTo("{\"name\":\"Adé\",\"n\":1}");
        insertRaw("foreign", "{ \"a\" : 1,\n \"b.c\": [true, null], \"u\": \"\\u00e9\" }".getBytes(StandardCharsets.UTF_8), 4);
        assertThat(c.hincrBy("foreign", "a", 1)).isEqualTo(2);
        assertThat(c.hgetString("foreign", "u")).isEqualTo("é");
        assertThat(c.hkeys("foreign")).containsExactly("a", "b.c", "u");
    }

    @ParameterizedTest
    @ValueSource(strings = {"json1", "fallback"})
    void losesNoUpdatesAcrossProcesses(String mode) throws Exception {
        LyteCache c = open(mode);
        c.set("shared", Map.of("other", "untouched"));
        List<Process> procs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            procs.add(startJava(mode, HashWorker.class, dir.resolve("cache.db").toString(), "hset", "shared", "f" + i, "60"));
            procs.add(startJava(mode, HashWorker.class, dir.resolve("cache.db").toString(), "hincrby", "shared", "n", "60"));
        }
        for (Process p : procs) {
            assertThat(p.waitFor(120, TimeUnit.SECONDS)).isTrue();
            assertThat(p.exitValue()).isZero();
        }
        assertThat(c.hgetAll("shared")).isEqualTo(Map.of("other", "untouched", "f0", 59L, "f1", 59L, "f2", 59L, "n", 180L));
    }

    static Process startJava(String mode, Class<?> main, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), main.getName()));
        cmd.addAll(List.of(args));
        ProcessBuilder pb = new ProcessBuilder(cmd).inheritIO();
        if ("fallback".equals(mode)) {
            pb.environment().put("LYTECACHE_FORCE_JSON_FALLBACK", "1");
        } else {
            pb.environment().remove("LYTECACHE_FORCE_JSON_FALLBACK");
        }
        return pb.start();
    }
}
