package io.litecache;

import com.fasterxml.jackson.core.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * LiteCache: Redis-like embedded caching library backed by SQLite.
 * Zero-config, portable JSON values, thread-safe, production-grade.
 *
 * <p>Usage (no configuration):
 * <pre>
 * try (LiteCache cache = new LiteCache()) {
 *     cache.set("key", "value", Duration.ofMinutes(5));
 *     String value = cache.getString("key");
 * }
 * </pre>
 *
 * <p>Implements {@link AutoCloseable} for resource management.
 */
public class LiteCache implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(LiteCache.class.getName());
    private static final int SCHEMA_VERSION = 1;
    private static final int BATCH_DELETE_LIMIT = 500;
    private static final long BUFFER_FLUSH_INTERVAL_MS = 5000;

    private final Path dbPath;
    private final String namespace;
    private final long maxKeys;
    private final long maxBytes;
    private final Eviction eviction;
    private final Duration sweepInterval;
    private final boolean strict;
    private final Serializer serializer;

    // Concurrency control
    private final DataSource dataSource;
    private final ScheduledExecutorService sweeper;
    private final Object statsLock = new Object();

    // Stats
    private long hits = 0;
    private long misses = 0;
    private long evictions = 0;
    private long expiredRemoved = 0;

    // LRU buffering
    private final ConcurrentHashMap<String, Long> lruBuffer = new ConcurrentHashMap<>();
    private final ScheduledExecutorService bufferFlusher;

    private volatile boolean closed = false;

    private LiteCache(Builder builder) {
        this.dbPath = builder.dbPath;
        this.namespace = builder.namespace;
        this.maxKeys = builder.maxKeys;
        this.maxBytes = builder.maxBytes;
        this.eviction = builder.eviction;
        this.sweepInterval = builder.sweepInterval;
        this.strict = builder.strict;
        this.serializer = builder.serializer;

        this.dataSource = new DataSource(dbPath);
        initializeDatabase();

        // Start sweeper if interval is not null
        if (sweepInterval != null) {
            this.sweeper = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "litecache-sweeper");
                t.setDaemon(true);
                return t;
            });
            sweeper.scheduleAtFixedRate(this::sweep,
                    sweepInterval.toMillis(), sweepInterval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            this.sweeper = null;
        }

        // Buffer flusher for LRU updates
        this.bufferFlusher = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "litecache-buffer-flusher");
            t.setDaemon(true);
            return t;
        });
        bufferFlusher.scheduleAtFixedRate(this::flushLruBuffer,
                BUFFER_FLUSH_INTERVAL_MS, BUFFER_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new LiteCache with zero-config defaults.
     * Equivalent to {@code LiteCache.builder().build()}.
     *
     * @throws LiteCacheException if initialization fails
     */
    public LiteCache() {
        this(new Builder());
    }

    /**
     * Creates a new builder for configuring LiteCache.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default database path for this environment.
     * Linux: $XDG_CACHE_HOME/litecache or ~/.cache/litecache
     * macOS: ~/Library/Caches/litecache
     * Windows: %LOCALAPPDATA%\litecache
     *
     * <p>Can be overridden via LITECACHE_PATH environment variable.
     *
     * @return the default path
     */
    public static Path defaultPath() {
        String envPath = System.getenv("LITECACHE_PATH");
        if (envPath != null && !envPath.isEmpty()) {
            return expandUser(envPath);
        }

        String home = System.getProperty("user.home");
        String os = System.getProperty("os.name").toLowerCase();

        Path basePath;
        if (os.contains("win")) {
            String localAppData = System.getenv("LOCALAPPDATA");
            basePath = Paths.get(localAppData != null ? localAppData : home, "litecache");
        } else if (os.contains("mac")) {
            basePath = Paths.get(home, "Library", "Caches", "litecache");
        } else {
            // Linux and others: use XDG_CACHE_HOME
            String xdgCache = System.getenv("XDG_CACHE_HOME");
            if (xdgCache != null && !xdgCache.isEmpty()) {
                basePath = Paths.get(xdgCache, "litecache");
            } else {
                basePath = Paths.get(home, ".cache", "litecache");
            }
        }

        // Append project ID (hash of working directory)
        String projectId = deriveProjectId();
        return basePath.resolve(projectId + ".db");
    }

    private static Path expandUser(String rawPath) {
        if (rawPath.equals("~") || rawPath.startsWith("~/") || rawPath.startsWith("~" + File.separator)) {
            String home = System.getProperty("user.home");
            return Paths.get(home, rawPath.substring(Math.min(2, rawPath.length())));
        }
        return Paths.get(rawPath);
    }

    /**
     * Derives a project ID identifying the current working directory: the first 12 hex characters
     * (6 bytes) of the SHA-256 digest of the directory's resolved (symlink-free, absolute) path,
     * UTF-8 encoded. This must match the Python reference implementation's
     * {@code hashlib.sha256(str(cwd.resolve()).encode("utf-8")).hexdigest()[:12]} exactly, so that
     * the same project directory resolves to the same cache file regardless of which language's
     * LiteCache created it.
     */
    private static String deriveProjectId() {
        try {
            Path cwd = Paths.get(System.getProperty("user.dir"));
            Path resolved;
            try {
                resolved = cwd.toRealPath();
            } catch (IOException e) {
                resolved = cwd.toAbsolutePath().normalize();
            }
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(resolved.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is a required MessageDigest algorithm", e);
        }
    }

    /**
     * Returns the path of this cache's database file.
     *
     * @return the database path
     */
    public Path path() {
        return dbPath;
    }

    /**
     * Sets a value with an optional TTL.
     *
     * @param key the key
     * @param value the value (any serializable object)
     * @param ttl the time-to-live duration, or null for no expiry
     * @throws LiteCacheException on database error
     */
    public void set(String key, Object value, Duration ttl) {
        checkNotClosed();
        Serializer.SerializedValue sv = serializer.serialize(value);
        long now = System.currentTimeMillis();
        Long expiresAt = ttl != null ? now + ttl.toMillis() : null;

        if (eviction == Eviction.NOEVICTION) {
            // NOEVICTION must reject an overflowing write outright, never accept it and evict
            // later -- there's nothing to evict. Checked before the write so a rejected set() has
            // no side effect.
            checkCapacityBeforeWrite(key);
        }

        try {
            dataSource.executeUpdate(conn -> {
                String sql = """
                        INSERT OR REPLACE INTO cache
                        (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """;
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, key);
                    ps.setString(2, namespace);
                    ps.setBytes(3, sv.bytes());
                    ps.setInt(4, sv.typeCode());
                    ps.setLong(5, now);
                    ps.setObject(6, expiresAt);
                    ps.setLong(7, now);
                    ps.setInt(8, 0);
                    ps.setLong(9, sv.bytes().length);
                    ps.executeUpdate();
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("Failed to set key: " + e.getMessage(), e);
        }

        // Buffer for sweeper
        enforceCapacity();
    }

    /**
     * Sets a value with no expiry.
     *
     * @param key the key
     * @param value the value
     */
    public void set(String key, Object value) {
        set(key, value, null);
    }

    /**
     * Convenience: sets a string value.
     *
     * @param key the key
     * @param value the string value
     * @param ttl the time-to-live duration, or null for no expiry
     */
    public void set(String key, String value, Duration ttl) {
        set(key, (Object) value, ttl);
    }

    /**
     * Convenience: sets a string value with no expiry.
     *
     * @param key the key
     * @param value the string value
     */
    public void set(String key, String value) {
        set(key, value, null);
    }

    /**
     * Convenience: sets a long value.
     *
     * @param key the key
     * @param value the long value
     * @param ttl the time-to-live duration, or null for no expiry
     */
    public void set(String key, long value, Duration ttl) {
        set(key, (Object) value, ttl);
    }

    /**
     * Convenience: sets a double value.
     *
     * @param key the key
     * @param value the double value
     * @param ttl the time-to-live duration, or null for no expiry
     */
    public void set(String key, double value, Duration ttl) {
        set(key, (Object) value, ttl);
    }

    /**
     * Gets a typed value, or null if not found or expired.
     *
     * @param <T> the target type
     * @param key the key
     * @param type the target type
     * @return the value or null
     * @throws SerializationException on type mismatch or deserialization failure
     */
    public <T> T get(String key, Class<T> type) {
        checkNotClosed();
        try {
            RawValue raw = fetchRawValue(key);
            if (raw == null) {
                return null;
            }
            return serializer.deserialize(raw.bytes(), raw.typeCode(), type);
        } catch (SQLException e) {
            if (strict) {
                throw new LiteCacheException("Query failed: " + e.getMessage(), e);
            } else {
                logger.log(Level.WARNING, "Query failed, returning null: " + e.getMessage());
                recordMiss();
                return null;
            }
        }
    }

    /**
     * Gets a value into a fully-parameterized generic type (e.g. {@code Map<String, MyRecord>},
     * {@code List<MyRecord>}) that a raw {@link Class} can't express due to type erasure. Only
     * meaningful for JSON-encoded values; use {@link #get(String, Class)} for native types.
     *
     * @param <T> the target type
     * @param key the key
     * @param typeRef the target generic type, e.g. {@code new TypeReference<Map<String, Long>>() {}}
     * @return the value or null
     * @throws SerializationException on type mismatch or deserialization failure
     */
    public <T> T get(String key, TypeReference<T> typeRef) {
        checkNotClosed();
        try {
            RawValue raw = fetchRawValue(key);
            if (raw == null) {
                return null;
            }
            return serializer.deserialize(raw.bytes(), raw.typeCode(), typeRef);
        } catch (SQLException e) {
            if (strict) {
                throw new LiteCacheException("Query failed: " + e.getMessage(), e);
            } else {
                logger.log(Level.WARNING, "Query failed, returning null: " + e.getMessage());
                recordMiss();
                return null;
            }
        }
    }

    /**
     * Reads the value's raw bytes and stored type code, or null if missing/expired.
     * Handles lazy expiration and LRU/hit/miss bookkeeping; shared by {@link #get(String, Class)}
     * and {@link #memoize(String, Duration, java.util.function.Supplier)}.
     */
    private record RawValue(byte[] bytes, int typeCode) {}

    private RawValue fetchRawValue(String key) throws SQLException {
        long now = System.currentTimeMillis();
        return dataSource.executeQuery(conn -> {
            String sql = "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, namespace);
                ps.setString(2, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        recordMiss();
                        return null;
                    }

                    Long expiresAt = readNullableLong(rs, "expires_at");
                    if (expiresAt != null && expiresAt <= now) {
                        // Expired; lazy delete
                        recordMiss();
                        deleteExpired(conn, key);
                        return null;
                    }

                    byte[] value = rs.getBytes("value");
                    int typeCode = rs.getInt("value_type");

                    // Buffer LRU update instead of immediate write
                    lruBuffer.put(key, now);

                    recordHit();
                    return new RawValue(value, typeCode);
                }
            }
        });
    }

    /**
     * Convenience: gets a string value.
     *
     * @param key the key
     * @return the string value, or null if missing or expired
     */
    public String getString(String key) {
        return get(key, String.class);
    }

    /**
     * Convenience: gets a long value.
     *
     * @param key the key
     * @return the long value, or null if missing or expired
     */
    public Long getLong(String key) {
        return get(key, Long.class);
    }

    /**
     * Convenience: gets a double value.
     *
     * @param key the key
     * @return the double value, or null if missing or expired
     */
    public Double getDouble(String key) {
        return get(key, Double.class);
    }

    /**
     * Convenience: gets bytes value.
     *
     * @param key the key
     * @return the byte array value, or null if missing or expired
     */
    public byte[] getBytes(String key) {
        return get(key, byte[].class);
    }

    /**
     * Optional variant of getString.
     *
     * @param key the key
     * @return an Optional containing the string value, or empty if missing or expired
     */
    public Optional<String> findString(String key) {
        return Optional.ofNullable(getString(key));
    }

    /**
     * Deletes one or more keys.
     *
     * @param keys the keys to delete
     * @return the number of keys actually deleted
     */
    public int delete(String... keys) {
        checkNotClosed();
        if (keys.length == 0) return 0;

        try {
            return dataSource.executeUpdate(conn -> {
                int deleted = 0;
                for (String key : keys) {
                    String sql = "DELETE FROM cache WHERE namespace = ? AND key = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, namespace);
                        ps.setString(2, key);
                        deleted += ps.executeUpdate();
                    }
                }
                return deleted;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("Delete failed: " + e.getMessage(), e);
        }
    }

    /**
     * Checks if a key exists and is not expired.
     *
     * @param key the key
     * @return true if the key exists and is not expired
     */
    public boolean exists(String key) {
        return get(key, String.class) != null;
    }

    /**
     * Sets a value only if the key does not exist (Redis SET NX). Atomic.
     *
     * @param key the key
     * @param value the value
     * @param ttl the TTL, or null
     * @return true if set, false if key already existed
     */
    public boolean add(String key, Object value, Duration ttl) {
        checkNotClosed();
        Serializer.SerializedValue sv = serializer.serialize(value);
        long now = System.currentTimeMillis();
        Long expiresAt = ttl != null ? now + ttl.toMillis() : null;

        if (eviction == Eviction.NOEVICTION) {
            checkCapacityBeforeWrite(key);
        }

        // Single atomic UPSERT: the DO UPDATE branch only fires (and only "wins") when the
        // existing row is already expired, so a fresh key and an expired key both insert/overwrite
        // in one statement -- no separate check-then-write race between processes.
        String sql = """
                INSERT INTO cache
                    (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value = excluded.value,
                    value_type = excluded.value_type,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at,
                    last_accessed = excluded.last_accessed,
                    access_count = 0,
                    size_bytes = excluded.size_bytes
                WHERE cache.expires_at IS NOT NULL AND cache.expires_at <= ?
                """;
        try {
            boolean won = dataSource.executeUpdate(conn -> {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, key);
                    ps.setString(2, namespace);
                    ps.setBytes(3, sv.bytes());
                    ps.setInt(4, sv.typeCode());
                    ps.setLong(5, now);
                    ps.setObject(6, expiresAt);
                    ps.setLong(7, now);
                    ps.setLong(8, sv.bytes().length);
                    ps.setLong(9, now);
                    return ps.executeUpdate() == 1;
                }
            });
            if (won) {
                enforceCapacity();
            }
            return won;
        } catch (SQLException e) {
            throw new LiteCacheException("Add failed: " + e.getMessage(), e);
        }
    }

    /**
     * Sets a value only if the key already exists (Redis SET XX). Atomic.
     *
     * @param key the key
     * @param value the value
     * @param ttl the TTL, or null
     * @return true if replaced, false if key did not exist
     */
    public boolean replace(String key, Object value, Duration ttl) {
        checkNotClosed();
        Serializer.SerializedValue sv = serializer.serialize(value);
        long now = System.currentTimeMillis();
        Long expiresAt = ttl != null ? now + ttl.toMillis() : null;

        // A single UPDATE with the existence/expiry check in the WHERE clause is already atomic;
        // no separate check-then-write is needed.
        String sql = """
                UPDATE cache SET value = ?, value_type = ?, created_at = ?, expires_at = ?, last_accessed = ?, access_count = 0
                WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)
                """;
        try {
            return dataSource.executeUpdate(conn -> {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setBytes(1, sv.bytes());
                    ps.setInt(2, sv.typeCode());
                    ps.setLong(3, now);
                    ps.setObject(4, expiresAt);
                    ps.setLong(5, now);
                    ps.setString(6, namespace);
                    ps.setString(7, key);
                    ps.setLong(8, now);
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("Replace failed: " + e.getMessage(), e);
        }
    }

    /**
     * Atomically reads and replaces a value.
     *
     * @param key the key
     * @param value the new value
     * @return the old value, or null if not found
     */
    public String getSet(String key, String value) {
        checkNotClosed();
        Serializer.SerializedValue sv = serializer.serialize(value);
        long now = System.currentTimeMillis();

        try {
            return dataSource.executeUpdate(conn -> {
                try (Statement begin = conn.createStatement()) {
                    begin.execute("BEGIN IMMEDIATE");
                }
                try {
                    String oldValue = null;
                    try (PreparedStatement select = conn.prepareStatement(
                            "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?")) {
                        select.setString(1, namespace);
                        select.setString(2, key);
                        try (ResultSet rs = select.executeQuery()) {
                            if (rs.next()) {
                                Long expiresAtDb = readNullableLong(rs, "expires_at");
                                if (expiresAtDb == null || expiresAtDb > now) {
                                    oldValue = serializer.deserialize(rs.getBytes("value"), rs.getInt("value_type"), String.class);
                                }
                            }
                        }
                    }

                    String upsertSql = """
                            INSERT INTO cache
                                (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                            VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?)
                            ON CONFLICT(namespace, key) DO UPDATE SET
                                value = excluded.value,
                                value_type = excluded.value_type,
                                created_at = excluded.created_at,
                                expires_at = NULL,
                                last_accessed = excluded.last_accessed,
                                access_count = 0,
                                size_bytes = excluded.size_bytes
                            """;
                    try (PreparedStatement upsert = conn.prepareStatement(upsertSql)) {
                        upsert.setString(1, key);
                        upsert.setString(2, namespace);
                        upsert.setBytes(3, sv.bytes());
                        upsert.setInt(4, sv.typeCode());
                        upsert.setLong(5, now);
                        upsert.setLong(6, now);
                        upsert.setLong(7, sv.bytes().length);
                        upsert.executeUpdate();
                    }

                    try (Statement commit = conn.createStatement()) {
                        commit.execute("COMMIT");
                    }
                    return oldValue;
                } catch (RuntimeException | SQLException e) {
                    try (Statement rollback = conn.createStatement()) {
                        rollback.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                        // best-effort rollback
                    }
                    if (e instanceof SQLException se) throw se;
                    throw (RuntimeException) e;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("getSet failed: " + e.getMessage(), e);
        }
    }

    /**
     * Sets all entries from a map with an optional TTL. Single transaction.
     *
     * @param entries the entries to set
     * @param ttl the TTL, or null
     */
    public void setAll(Map<String, String> entries, Duration ttl) {
        checkNotClosed();
        if (entries.isEmpty()) return;

        long now = System.currentTimeMillis();
        Long expiresAt = ttl != null ? now + ttl.toMillis() : null;

        String sql = """
                INSERT INTO cache
                    (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value = excluded.value,
                    value_type = excluded.value_type,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at,
                    last_accessed = excluded.last_accessed,
                    access_count = 0,
                    size_bytes = excluded.size_bytes
                """;
        try {
            dataSource.executeUpdate(conn -> {
                try (Statement begin = conn.createStatement()) {
                    begin.execute("BEGIN IMMEDIATE");
                }
                try {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        for (Map.Entry<String, String> e : entries.entrySet()) {
                            byte[] bytes = e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            ps.setString(1, e.getKey());
                            ps.setString(2, namespace);
                            ps.setBytes(3, bytes);
                            ps.setInt(4, TypeCode.STRING);
                            ps.setLong(5, now);
                            ps.setObject(6, expiresAt);
                            ps.setLong(7, now);
                            ps.setLong(8, bytes.length);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                    try (Statement commit = conn.createStatement()) {
                        commit.execute("COMMIT");
                    }
                } catch (RuntimeException | SQLException e) {
                    try (Statement rollback = conn.createStatement()) {
                        rollback.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                        // best-effort rollback
                    }
                    if (e instanceof SQLException se) throw se;
                    throw (RuntimeException) e;
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("setAll failed: " + e.getMessage(), e);
        }
    }

    /**
     * Gets multiple values. Single transaction.
     *
     * @param keys the keys to retrieve
     * @return a map of found keys to their string values
     */
    public Map<String, String> getAll(Collection<String> keys) {
        checkNotClosed();
        Map<String, String> result = new LinkedHashMap<>();
        if (keys.isEmpty()) return result;

        try {
            dataSource.executeQuery(conn -> {
                for (String key : keys) {
                    String sql = "SELECT value, value_type, expires_at FROM cache WHERE namespace = ? AND key = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, namespace);
                        ps.setString(2, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                Long expiresAt = readNullableLong(rs, "expires_at");
                                if (expiresAt == null || expiresAt > System.currentTimeMillis()) {
                                    String value = new String(rs.getBytes("value"));
                                    result.put(key, value);
                                }
                            }
                        }
                    }
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("getAll failed: " + e.getMessage(), e);
        }
        return result;
    }

    /**
     * Sets an expiration TTL on an existing key.
     *
     * @param key the key
     * @param ttl the new TTL
     * @return true if the key exists, false otherwise
     */
    public boolean expire(String key, Duration ttl) {
        checkNotClosed();
        long now = System.currentTimeMillis();
        long expiresAt = now + ttl.toMillis();

        try {
            return dataSource.executeUpdate(conn -> {
                String sql = "UPDATE cache SET expires_at = ? WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, expiresAt);
                    ps.setString(2, namespace);
                    ps.setString(3, key);
                    ps.setLong(4, now);
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("expire failed: " + e.getMessage(), e);
        }
    }

    /**
     * Removes expiration from a key (sets TTL to infinite).
     *
     * @param key the key
     * @return true if the key exists, false otherwise
     */
    public boolean persist(String key) {
        checkNotClosed();
        try {
            return dataSource.executeUpdate(conn -> {
                String sql = "UPDATE cache SET expires_at = NULL WHERE namespace = ? AND key = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, key);
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("persist failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the remaining TTL for a key.
     *
     * @param key the key
     * @return Duration of remaining time, Duration.ofSeconds(-1) if no expiry, null if key not found
     */
    public Duration ttl(String key) {
        checkNotClosed();
        try {
            return dataSource.executeQuery(conn -> {
                String sql = "SELECT expires_at FROM cache WHERE namespace = ? AND key = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, key);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            return null;
                        }
                        Long expiresAt = readNullableLong(rs, "expires_at");
                        if (expiresAt == null) {
                            return Duration.ofSeconds(-1);
                        }
                        long now = System.currentTimeMillis();
                        long remaining = expiresAt - now;
                        return Duration.ofMillis(Math.max(0, remaining));
                    }
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("ttl failed: " + e.getMessage(), e);
        }
    }

    /**
     * Updates the TTL for a key (sliding expiration, like Redis TOUCH).
     *
     * @param key the key
     * @param ttl the new TTL
     * @return true if key exists and is not expired, false otherwise
     */
    public boolean touch(String key, Duration ttl) {
        checkNotClosed();
        long now = System.currentTimeMillis();
        long expiresAt = now + ttl.toMillis();

        try {
            return dataSource.executeUpdate(conn -> {
                String sql = "UPDATE cache SET expires_at = ?, last_accessed = ? WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, expiresAt);
                    ps.setLong(2, now);
                    ps.setString(3, namespace);
                    ps.setString(4, key);
                    ps.setLong(5, now);
                    return ps.executeUpdate() > 0;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("touch failed: " + e.getMessage(), e);
        }
    }

    /**
     * Atomically increments a numeric value. Missing keys start at 0.
     *
     * @param key the key
     * @return the new value
     * @throws IllegalStateException if value is not numeric
     */
    public long incr(String key) {
        return incr(key, 1L);
    }

    /**
     * Atomically increments a numeric value by amount.
     *
     * @param key the key
     * @param amount the amount to add
     * @return the new value
     * @throws IllegalStateException if value is not numeric
     */
    public long incr(String key, long amount) {
        checkNotClosed();
        String text = atomicIncr(key, amount, TypeCode.INT64, new int[] {TypeCode.INT64});
        return Long.parseLong(text);
    }

    /**
     * Atomically decrements a numeric value by 1.
     *
     * @param key the key
     * @return the new value
     */
    public long decr(String key) {
        return incr(key, -1L);
    }

    /**
     * Atomically decrements a numeric value by amount.
     *
     * @param key the key
     * @param amount the amount to subtract
     * @return the new value
     */
    public long decr(String key, long amount) {
        return incr(key, -amount);
    }

    /**
     * Atomically increments a double value.
     *
     * @param key the key
     * @param amount the amount to add
     * @return the new value
     */
    public double incrDouble(String key, double amount) {
        checkNotClosed();
        String text = atomicIncr(key, amount, TypeCode.FLOAT64, new int[] {TypeCode.INT64, TypeCode.FLOAT64});
        return Double.parseDouble(text);
    }

    /**
     * Performs an atomic single-statement UPSERT that adds {@code amount} to the existing numeric
     * value (or 0 if absent/expired), storing the result as UTF-8 decimal text (matching the
     * cross-language wire format). This mirrors the reference Python implementation's SQL exactly so
     * that a single SQLite statement -- not a Java-side read-modify-write -- is the unit of atomicity
     * shared across processes.
     *
     * @param key the key
     * @param amount the amount to add (bound as its native numeric JDBC type)
     * @param resultType the value_type to store the result as (INT64 or FLOAT64)
     * @param allowedTypes the existing value_type(s) permitted to be added to
     * @return the new value, as decimal text
     * @throws IllegalStateException if the existing value is not numeric
     */
    private String atomicIncr(String key, Number amount, int resultType, int[] allowedTypes) {
        long now = System.currentTimeMillis();
        byte[] initialBlob = (resultType == TypeCode.INT64
                        ? Long.toString(amount.longValue())
                        : Double.toString(amount.doubleValue()))
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        StringBuilder inClause = new StringBuilder("(");
        for (int i = 0; i < allowedTypes.length; i++) {
            if (i > 0) inClause.append(",");
            inClause.append(allowedTypes[i]);
        }
        inClause.append(")");

        String sql = """
                INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, access_count, size_bytes)
                VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?)
                ON CONFLICT(namespace, key) DO UPDATE SET
                    value = CAST(CAST(
                        (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= ? THEN 0
                              ELSE CAST(cache.value AS TEXT) END) + ?
                        AS TEXT) AS BLOB),
                    value_type = ?,
                    expires_at = CASE
                        WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= ? THEN NULL
                        ELSE cache.expires_at
                    END,
                    last_accessed = ?,
                    size_bytes = LENGTH(CAST(
                        (CASE WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= ? THEN 0
                              ELSE CAST(cache.value AS TEXT) END) + ?
                        AS TEXT))
                WHERE (cache.expires_at IS NOT NULL AND cache.expires_at <= ?)
                   OR cache.value_type IN %s
                """.formatted(inClause);

        try {
            return dataSource.executeUpdate(conn -> {
                try (Statement begin = conn.createStatement()) {
                    begin.execute("BEGIN IMMEDIATE");
                }
                try {
                    int rowsAffected;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        int i = 1;
                        ps.setString(i++, key);
                        ps.setString(i++, namespace);
                        ps.setBytes(i++, initialBlob);
                        ps.setInt(i++, resultType);
                        ps.setLong(i++, now);
                        ps.setLong(i++, now);
                        ps.setLong(i++, initialBlob.length);
                        ps.setLong(i++, now);
                        ps.setObject(i++, amount);
                        ps.setInt(i++, resultType);
                        ps.setLong(i++, now);
                        ps.setLong(i++, now);
                        ps.setLong(i++, now);
                        ps.setObject(i++, amount);
                        ps.setLong(i, now);
                        rowsAffected = ps.executeUpdate();
                    }
                    if (rowsAffected == 0) {
                        throw new IllegalStateException(
                                "Value for key '" + key + "' is not " + (resultType == TypeCode.FLOAT64 ? "a number" : "an integer"));
                    }
                    String result;
                    try (PreparedStatement select = conn.prepareStatement(
                            "SELECT value FROM cache WHERE namespace = ? AND key = ?")) {
                        select.setString(1, namespace);
                        select.setString(2, key);
                        try (ResultSet rs = select.executeQuery()) {
                            if (!rs.next()) {
                                throw new IllegalStateException("Unexpected: key not found after incr upsert");
                            }
                            result = new String(rs.getBytes("value"), java.nio.charset.StandardCharsets.UTF_8);
                        }
                    }
                    try (Statement commit = conn.createStatement()) {
                        commit.execute("COMMIT");
                    }
                    return result;
                } catch (RuntimeException | SQLException e) {
                    try (Statement rollback = conn.createStatement()) {
                        rollback.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                        // best-effort rollback
                    }
                    if (e instanceof SQLException se) throw se;
                    throw (RuntimeException) e;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("incr failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns a lazy stream of keys matching the glob pattern (e.g. {@code "user:*"}), backed by a
     * keyset cursor that fetches in batches of 500 rather than materializing all matches up front.
     * Expired keys are excluded. Consume the stream fully or close it (e.g. via try-with-resources)
     * to release the underlying cursor state.
     *
     * @param globPattern a SQLite {@code GLOB} pattern ({@code *}, {@code ?}, and {@code [...]} wildcards)
     * @return a Stream of matching, non-expired keys in ascending key order
     */
    public Stream<String> keys(String globPattern) {
        checkNotClosed();
        int batchSize = 500;
        Iterator<String> iterator = new Iterator<>() {
            private String lastKey = "";
            private Deque<String> buffer = new ArrayDeque<>();
            private boolean exhausted = false;

            private void fetchNextBatch() {
                try {
                    List<String> batch = dataSource.executeQuery(conn -> {
                        List<String> result = new ArrayList<>();
                        String sql = """
                                SELECT key FROM cache
                                WHERE namespace = ? AND key GLOB ? AND key > ?
                                  AND (expires_at IS NULL OR expires_at > ?)
                                ORDER BY key LIMIT ?
                                """;
                        try (PreparedStatement ps = conn.prepareStatement(sql)) {
                            ps.setString(1, namespace);
                            ps.setString(2, globPattern);
                            ps.setString(3, lastKey);
                            ps.setLong(4, System.currentTimeMillis());
                            ps.setInt(5, batchSize);
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    result.add(rs.getString("key"));
                                }
                            }
                        }
                        return result;
                    });
                    if (batch.isEmpty()) {
                        exhausted = true;
                    } else {
                        buffer.addAll(batch);
                        lastKey = batch.get(batch.size() - 1);
                        if (batch.size() < batchSize) {
                            exhausted = true;
                        }
                    }
                } catch (SQLException e) {
                    throw new LiteCacheException("keys query failed: " + e.getMessage(), e);
                }
            }

            @Override
            public boolean hasNext() {
                if (buffer.isEmpty() && !exhausted) {
                    fetchNextBatch();
                }
                return !buffer.isEmpty();
            }

            @Override
            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return buffer.removeFirst();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    /**
     * Clears all entries in this namespace.
     */
    public void flush() {
        checkNotClosed();
        try {
            dataSource.executeUpdate(conn -> {
                String sql = "DELETE FROM cache WHERE namespace = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.executeUpdate();
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("flush failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns current cache statistics.
     *
     * @return a CacheStats snapshot
     */
    public CacheStats stats() {
        checkNotClosed();
        synchronized (statsLock) {
            try {
                return dataSource.executeQuery(conn -> {
                    long keyCount = 0;
                    long sizeBytes = 0;

                    String sql = "SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, namespace);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                keyCount = rs.getLong("cnt");
                                sizeBytes = rs.getLong("sz");
                            }
                        }
                    }

                    return new CacheStats(hits, misses, keyCount, sizeBytes, evictions, expiredRemoved, dbPath);
                });
            } catch (SQLException e) {
                throw new LiteCacheException("stats query failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Acquires a distributed lock (process-safe) on the given name.
     *
     * @param lockName the lock name
     * @param timeout the acquisition timeout, also used as the lock's hold TTL so a crashed
     *     holder's lock is automatically released rather than stuck forever
     * @return a CacheLock that can be used in try-with-resources
     * @throws LockTimeoutException if acquisition times out
     */
    public CacheLock lock(String lockName, Duration timeout) {
        checkNotClosed();
        return new CacheLock(this, lockName, timeout);
    }

    /**
     * Attempts a single, non-blocking acquisition of the lock key with the given token and TTL.
     *
     * @param lockKey the fully-qualified lock key (e.g. {@code "lock:" + name})
     * @param token a unique value identifying this holder, so release only removes our own lock
     * @param ttl how long the lock is held before auto-expiring
     * @return true if acquired
     */
    boolean tryAcquireLock(String lockKey, String token, Duration ttl) {
        return add(lockKey, token, ttl);
    }

    /**
     * Releases a lock previously acquired with {@link #tryAcquireLock}, but only if it is still
     * held by {@code token} -- so a lock that already expired and was re-acquired by someone else
     * is never deleted out from under them. The check-and-delete happens atomically in a single
     * transaction.
     *
     * @param lockKey the fully-qualified lock key
     * @param token the token this holder acquired the lock with
     * @return true if this call deleted the lock
     */
    boolean releaseLock(String lockKey, String token) {
        checkNotClosed();
        try {
            return dataSource.executeUpdate(conn -> {
                try (Statement begin = conn.createStatement()) {
                    begin.execute("BEGIN IMMEDIATE");
                }
                try {
                    boolean deleted = false;
                    try (PreparedStatement select = conn.prepareStatement(
                            "SELECT value FROM cache WHERE namespace = ? AND key = ?")) {
                        select.setString(1, namespace);
                        select.setString(2, lockKey);
                        try (ResultSet rs = select.executeQuery()) {
                            if (rs.next() && token.equals(new String(rs.getBytes("value"), java.nio.charset.StandardCharsets.UTF_8))) {
                                try (PreparedStatement del = conn.prepareStatement(
                                        "DELETE FROM cache WHERE namespace = ? AND key = ?")) {
                                    del.setString(1, namespace);
                                    del.setString(2, lockKey);
                                    deleted = del.executeUpdate() > 0;
                                }
                            }
                        }
                    }
                    try (Statement commit = conn.createStatement()) {
                        commit.execute("COMMIT");
                    }
                    return deleted;
                } catch (RuntimeException | SQLException e) {
                    try (Statement rollback = conn.createStatement()) {
                        rollback.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                        // best-effort rollback
                    }
                    if (e instanceof SQLException se) throw se;
                    throw (RuntimeException) e;
                }
            });
        } catch (SQLException e) {
            throw new LiteCacheException("lock release failed: " + e.getMessage(), e);
        }
    }

    /**
     * Runs a memoized computation: returns cached value if present, otherwise computes via loader, stores, and returns.
     *
     * @param <T> the value type
     * @param key the key
     * @param ttl the TTL for the computed value
     * @param loader the supplier that computes the value
     * @return the cached or computed value
     */
    @SuppressWarnings("unchecked")
    public <T> T memoize(String key, Duration ttl, java.util.function.Supplier<T> loader) {
        checkNotClosed();
        T cached = (T) getNatural(key);
        if (cached != null) {
            return cached;
        }
        T computed = loader.get();
        set(key, computed, ttl);
        return computed;
    }

    /**
     * Reads a value using the Java type its stored type code naturally maps to (String, byte[],
     * Long, Double, or a JSON-decoded Object), without the caller having to specify a target class.
     * Used by {@link #memoize}, where the loader's return type determines what was stored.
     */
    private Object getNatural(String key) {
        checkNotClosed();
        try {
            RawValue raw = fetchRawValue(key);
            if (raw == null) {
                return null;
            }
            Class<?> targetType = switch (raw.typeCode()) {
                case TypeCode.BYTES -> byte[].class;
                case TypeCode.STRING -> String.class;
                case TypeCode.INT64 -> Long.class;
                case TypeCode.FLOAT64 -> Double.class;
                default -> Object.class;
            };
            return serializer.deserialize(raw.bytes(), raw.typeCode(), targetType);
        } catch (SQLException e) {
            if (strict) {
                throw new LiteCacheException("Query failed: " + e.getMessage(), e);
            } else {
                logger.log(Level.WARNING, "Query failed, returning null: " + e.getMessage());
                recordMiss();
                return null;
            }
        }
    }

    /**
     * Optimizes the database (vacuum).
     */
    public void vacuum() {
        checkNotClosed();
        try {
            dataSource.executeUpdate(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("VACUUM");
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("vacuum failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        // Flush remaining LRU updates
        flushLruBuffer();

        // Stop sweeper
        if (sweeper != null) {
            sweeper.shutdown();
            try {
                if (!sweeper.awaitTermination(5, TimeUnit.SECONDS)) {
                    sweeper.shutdownNow();
                }
            } catch (InterruptedException e) {
                sweeper.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Stop buffer flusher
        if (bufferFlusher != null) {
            bufferFlusher.shutdown();
            try {
                if (!bufferFlusher.awaitTermination(5, TimeUnit.SECONDS)) {
                    bufferFlusher.shutdownNow();
                }
            } catch (InterruptedException e) {
                bufferFlusher.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Close data source
        dataSource.close();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new LiteCacheException("Cache is closed");
        }
    }

    private void recordHit() {
        synchronized (statsLock) {
            hits++;
        }
    }

    private void recordMiss() {
        synchronized (statsLock) {
            misses++;
        }
    }

    private void flushLruBuffer() {
        if (lruBuffer.isEmpty()) return;

        try {
            dataSource.executeUpdate(conn -> {
                String sql = "UPDATE cache SET last_accessed = ?, access_count = access_count + 1 WHERE namespace = ? AND key = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (Map.Entry<String, Long> e : lruBuffer.entrySet()) {
                        ps.setLong(1, e.getValue());
                        ps.setString(2, namespace);
                        ps.setString(3, e.getKey());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                lruBuffer.clear();
                return null;
            });
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to flush LRU buffer: " + e.getMessage());
        }
    }

    private void sweep() {
        try {
            flushLruBuffer();

            // Delete expired entries in bounded batches. Plain SQLite doesn't support DELETE ... LIMIT,
            // so the batch is bounded via a subquery selecting the rowids/keys to remove.
            long now = System.currentTimeMillis();
            long removedCount = dataSource.executeUpdate(conn -> {
                String sql = """
                        DELETE FROM cache WHERE namespace = ? AND key IN (
                            SELECT key FROM cache
                            WHERE namespace = ? AND expires_at IS NOT NULL AND expires_at <= ?
                            LIMIT ?
                        )
                        """;
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, namespace);
                    ps.setLong(3, now);
                    ps.setInt(4, BATCH_DELETE_LIMIT);
                    return (long) ps.executeUpdate();
                }
            });

            if (removedCount > 0) {
                synchronized (statsLock) {
                    expiredRemoved += removedCount;
                }
            }

            // Enforce capacity
            enforceCapacity();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Sweep failed: " + e.getMessage());
        }
    }

    /**
     * Rejects a write outright if it would grow the dataset past {@code maxKeys}/{@code maxBytes}
     * under {@link Eviction#NOEVICTION} -- there is nothing to evict, so the write itself must
     * fail rather than being accepted and evicted after the fact. Updating an existing,
     * non-expired key never grows the dataset, so it is always allowed.
     */
    private void checkCapacityBeforeWrite(String key) {
        try {
            dataSource.executeQuery(conn -> {
                String existsSql = "SELECT 1 FROM cache WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)";
                try (PreparedStatement ps = conn.prepareStatement(existsSql)) {
                    ps.setString(1, namespace);
                    ps.setString(2, key);
                    ps.setLong(3, System.currentTimeMillis());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return null;
                        }
                    }
                }
                String statsSql = "SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?";
                try (PreparedStatement ps = conn.prepareStatement(statsSql)) {
                    ps.setString(1, namespace);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) return null;
                        long keyCount = rs.getLong("cnt");
                        long sizeBytes = rs.getLong("sz");
                        if (keyCount >= maxKeys) {
                            throw new CacheFullException("Cache is full (keys=" + keyCount + ", max=" + maxKeys + ")");
                        }
                        if (sizeBytes >= maxBytes) {
                            throw new CacheFullException("Cache is full (bytes=" + sizeBytes + ", max=" + maxBytes + ")");
                        }
                    }
                }
                return null;
            });
        } catch (SQLException e) {
            if (strict) {
                throw new LiteCacheException("capacity check failed: " + e.getMessage(), e);
            }
            logger.log(Level.WARNING, "capacity check failed: " + e.getMessage());
        }
    }

    private void enforceCapacity() {
        // Flush buffered last_accessed updates first so LRU eviction order reflects the access
        // that just happened, not stale on-disk timestamps.
        if (eviction == Eviction.LRU) {
            flushLruBuffer();
        }
        try {
            dataSource.executeUpdate(conn -> {
                // Check current stats
                String statsSql = "SELECT COUNT(*) as cnt, COALESCE(SUM(size_bytes), 0) as sz FROM cache WHERE namespace = ?";
                try (PreparedStatement ps = conn.prepareStatement(statsSql)) {
                    ps.setString(1, namespace);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) return null;
                        long keyCount = rs.getLong("cnt");
                        long sizeBytes = rs.getLong("sz");

                        if (eviction != Eviction.NOEVICTION && (keyCount > maxKeys || sizeBytes > maxBytes)) {
                            // Determine eviction order
                            String orderBy = switch (eviction) {
                                case LRU -> "last_accessed ASC";
                                case TTL -> "expires_at ASC NULLS LAST";
                                case RANDOM -> "RANDOM()";
                                case NOEVICTION -> throw new AssertionError();
                            };

                            // Delete oldest entries until under limit
                            long toDelete = Math.max(1, (keyCount - maxKeys) + (sizeBytes > maxBytes ? 100 : 0));
                            String deleteSql = "DELETE FROM cache WHERE namespace = ? AND key IN (SELECT key FROM cache WHERE namespace = ? ORDER BY " + orderBy + " LIMIT ?)";
                            try (PreparedStatement delPs = conn.prepareStatement(deleteSql)) {
                                delPs.setString(1, namespace);
                                delPs.setString(2, namespace);
                                delPs.setLong(3, toDelete);
                                int deleted = delPs.executeUpdate();
                                synchronized (statsLock) {
                                    evictions += deleted;
                                }
                            }
                        }
                    }
                }
                return null;
            });
        } catch (CacheFullException e) {
            throw e;
        } catch (SQLException e) {
            if (strict) {
                throw new LiteCacheException("enforceCapacity failed: " + e.getMessage(), e);
            } else {
                logger.log(Level.WARNING, "enforceCapacity failed: " + e.getMessage());
            }
        }
    }

    /**
     * Reads a nullable INTEGER column as a Long, safely. Some JDBC drivers'
     * {@code ResultSet.getObject(String, Class)} throw rather than return null for a SQL NULL
     * column when the requested class is a boxed numeric type, so {@code getLong} + {@code wasNull}
     * is used instead.
     */
    private static Long readNullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private void deleteExpired(Connection conn, String key) {
        try {
            String sql = "DELETE FROM cache WHERE namespace = ? AND key = ? AND expires_at IS NOT NULL AND expires_at <= ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, namespace);
                ps.setString(2, key);
                ps.setLong(3, System.currentTimeMillis());
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to delete expired key: " + e.getMessage());
        }
    }

    private void initializeDatabase() {
        try {
            dataSource.executeUpdate(conn -> {
                // Create tables
                String schema = """
                        CREATE TABLE IF NOT EXISTS cache (
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
                        ) WITHOUT ROWID;
                        
                        CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at) WHERE expires_at IS NOT NULL;
                        CREATE INDEX IF NOT EXISTS idx_cache_lru ON cache(namespace, last_accessed);
                        
                        CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
                        """;
                try (Statement stmt = conn.createStatement()) {
                    for (String sql : schema.split(";")) {
                        if (!sql.trim().isEmpty()) {
                            stmt.executeUpdate(sql);
                        }
                    }
                }

                // Check schema version
                String versionCheck = "SELECT v FROM meta WHERE k = 'schema_version'";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(versionCheck)) {
                    if (rs.next()) {
                        int version = Integer.parseInt(rs.getString("v"));
                        if (version > SCHEMA_VERSION) {
                            throw new SchemaVersionException("Database schema version " + version + " is not supported");
                        }
                    } else {
                        // Insert schema version
                        String insertVersion = "INSERT INTO meta (k, v) VALUES ('schema_version', '1')";
                        try (Statement insertStmt = conn.createStatement()) {
                            insertStmt.executeUpdate(insertVersion);
                        }
                    }
                }

                return null;
            });
        } catch (SQLException e) {
            throw new LiteCacheException("Database initialization failed: " + e.getMessage(), e);
        }
    }

    /**
     * Builder for LiteCache configuration.
     */
    @SuppressWarnings("missing-explicit-ctor")
    public static class Builder {
        private Path dbPath = defaultPath();
        private String namespace = "default";
        private long maxKeys = 1_000_000;
        private long maxBytes = 1024L * 1024 * 1024; // 1 GB
        private Eviction eviction = Eviction.LRU;
        private Duration sweepInterval = Duration.ofSeconds(60);
        private boolean strict = false;
        private Serializer serializer = new DefaultSerializer();

        /**
         * Sets the database file path. Optional escape hatch; omit to use the zero-config default
         * location (see {@link LiteCache#defaultPath()}).
         *
         * @param path the database file path
         * @return this builder
         */
        public Builder path(Path path) {
            this.dbPath = path;
            return this;
        }

        /**
         * Sets the namespace (default: "default").
         *
         * @param namespace the namespace, used to isolate multiple caches in one database file
         * @return this builder
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Sets the maximum number of keys.
         *
         * @param maxKeys the maximum key count before eviction kicks in
         * @return this builder
         */
        public Builder maxKeys(long maxKeys) {
            this.maxKeys = maxKeys;
            return this;
        }

        /**
         * Sets the maximum size in bytes.
         *
         * @param maxBytes the maximum total value size in bytes before eviction kicks in
         * @return this builder
         */
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        /**
         * Sets the eviction policy (default: LRU).
         *
         * @param eviction the eviction policy
         * @return this builder
         */
        public Builder eviction(Eviction eviction) {
            this.eviction = eviction;
            return this;
        }

        /**
         * Sets the sweep interval (default: 60s); null to disable background sweeper.
         *
         * @param sweepInterval how often the background sweeper runs, or null to disable it
         * @return this builder
         */
        public Builder sweepInterval(Duration sweepInterval) {
            this.sweepInterval = sweepInterval;
            return this;
        }

        /**
         * Sets strict mode (default: false). When true, errors throw; when false, errors log and degrade.
         *
         * @param strict whether read errors should throw instead of degrading to a miss
         * @return this builder
         */
        public Builder strict(boolean strict) {
            this.strict = strict;
            return this;
        }

        /**
         * Sets a custom serializer.
         *
         * @param serializer the serializer implementation to use in place of the Jackson-based default
         * @return this builder
         */
        public Builder serializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Builds the LiteCache instance.
         *
         * @return a new, ready-to-use LiteCache
         */
        public LiteCache build() {
            return new LiteCache(this);
        }
    }
}
