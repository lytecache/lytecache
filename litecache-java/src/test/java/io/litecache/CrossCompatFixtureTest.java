package io.litecache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Guards the cross-language storage contract described in SPEC.md: a SQLite file built by hand
 * (standing in for one written by the Python reference implementation, since a live Python runtime
 * isn't available at Java test time) must be readable by LiteCache exactly as documented -- byte
 * for byte per type code, with no silent misinterpretation.
 */
public class CrossCompatFixtureTest {
    @TempDir
    Path tempDir;

    /** A record used to verify JSON (type code 4) decodes into a Java type, dates included. */
    public record Person(String name, LocalDateTime createdAt) {}

    @Test
    public void readsAHandBuiltFixtureCoveringEveryPortableTypeCode() throws Exception {
        Path fixture = tempDir.resolve("fixture.db");
        buildFixture(fixture);

        try (LiteCache cache = LiteCache.builder().path(fixture).build()) {
            assertThat(cache.getString("greeting")).isEqualTo("hello from another language");
            assertThat(cache.getBytes("raw")).isEqualTo(new byte[] {1, 2, 3, 4, 5});

            // INT64/FLOAT64 are UTF-8 decimal text on disk (see SPEC.md), not binary -- this is
            // what lets a Python-written counter be both read AND atomically incr()'d from Java.
            assertThat(cache.getLong("count")).isEqualTo(42L);
            assertThat(cache.getDouble("ratio")).isCloseTo(3.14, within(0.0001));

            Person person = cache.get("person", Person.class);
            assertThat(person.name()).isEqualTo("Ada");
            assertThat(person.createdAt()).isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 0));

            // A value already expired in the fixture must read as a miss, not stale data.
            assertThat(cache.getString("expired")).isNull();
        }
    }

    @Test
    public void writesTheSameWireFormatItReads() throws Exception {
        Path dbPath = tempDir.resolve("roundtrip.db");
        try (LiteCache cache = LiteCache.builder().path(dbPath).build()) {
            cache.set("counter", 42L);
            cache.set("pi", 3.5);
        }

        // Read the raw bytes back with plain JDBC, bypassing LiteCache's own deserialization, to
        // confirm what Java actually put on disk matches the documented (and Python-compatible)
        // decimal-text wire format -- not just that LiteCache can read its own binary format back.
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath())) {
            assertThat(rawValue(conn, "counter")).isEqualTo("42".getBytes(StandardCharsets.UTF_8));
            assertThat(rawValue(conn, "pi")).isEqualTo("3.5".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static byte[] rawValue(Connection conn, String key) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT value FROM cache WHERE namespace = 'default' AND key = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("row for key %s", key).isTrue();
                return rs.getBytes("value");
            }
        }
    }

    /**
     * Builds a SQLite file by hand, using nothing but raw JDBC and the schema/type-code table from
     * SPEC.md -- standing in for a file written by another language's LiteCache implementation.
     */
    private static void buildFixture(Path path) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath())) {
            try (var stmt = conn.createStatement()) {
                stmt.executeUpdate("""
                        CREATE TABLE cache (
                          key            TEXT    NOT NULL,
                          namespace      TEXT    NOT NULL DEFAULT 'default',
                          value          BLOB    NOT NULL,
                          value_type     INTEGER NOT NULL DEFAULT 0,
                          created_at     INTEGER NOT NULL,
                          expires_at     INTEGER,
                          last_accessed  INTEGER NOT NULL,
                          access_count   INTEGER NOT NULL DEFAULT 0,
                          size_bytes     INTEGER NOT NULL,
                          PRIMARY KEY (namespace, key)
                        ) WITHOUT ROWID
                        """);
                stmt.executeUpdate("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
                stmt.executeUpdate("INSERT INTO meta (k, v) VALUES ('schema_version', '1')");
            }

            long now = System.currentTimeMillis();
            insert(conn, "greeting", 1, "hello from another language".getBytes(StandardCharsets.UTF_8), now, null);
            insert(conn, "raw", 0, new byte[] {1, 2, 3, 4, 5}, now, null);
            insert(conn, "count", 2, "42".getBytes(StandardCharsets.UTF_8), now, null);
            insert(conn, "ratio", 3, "3.14".getBytes(StandardCharsets.UTF_8), now, null);
            insert(conn, "person", 4,
                    "{\"name\":\"Ada\",\"createdAt\":\"2024-01-15T10:30:00\"}".getBytes(StandardCharsets.UTF_8),
                    now, null);
            insert(conn, "expired", 1, "gone".getBytes(StandardCharsets.UTF_8), now - 10_000, now - 5_000);
        }
    }

    private static void insert(Connection conn, String key, int typeCode, byte[] value, long createdAt, Long expiresAt)
            throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO cache
                    (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                VALUES (?, 'default', ?, ?, ?, ?, ?, 0, ?)
                """)) {
            ps.setString(1, key);
            ps.setBytes(2, value);
            ps.setInt(3, typeCode);
            ps.setLong(4, createdAt);
            ps.setObject(5, expiresAt);
            ps.setLong(6, createdAt);
            ps.setLong(7, value.length);
            ps.executeUpdate();
        }
    }
}
