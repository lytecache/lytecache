package io.lytecache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * LyteCache: Redis-like embedded caching library backed by SQLite.
 * Zero-config, portable JSON values, thread-safe, production-grade.
 *
 * <p>Usage (no configuration):
 * <pre>
 * try (LyteCache cache = new LyteCache()) {
 *     cache.set("key", "value", Duration.ofMinutes(5));
 *     String value = cache.getString("key");
 * }
 * </pre>
 *
 * <p>Implements {@link AutoCloseable} for resource management.
 */
public class LyteCache implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(LyteCache.class.getName());
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

    // JSON1 support for hash operations, detected once at open.
    private final boolean json1;
    // Events and change listeners are opt-in: none of this costs anything (no table, no thread)
    // until notify/watch/onChange is first used.
    private final Set<ChangeOp> publishOps;
    private final Duration eventRetention;
    private final long eventMaxCount;
    private final int changeKeyCap;
    private volatile boolean eventsReady = false;
    private final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
    private volatile ChangeDispatcher dispatcher;

    private LyteCache(Builder builder) {
        this.dbPath = builder.dbPath;
        this.namespace = builder.namespace;
        this.maxKeys = builder.maxKeys;
        this.maxBytes = builder.maxBytes;
        this.eviction = builder.eviction;
        this.sweepInterval = builder.sweepInterval;
        this.strict = builder.strict;
        this.serializer = builder.serializer;
        this.publishOps = builder.publishOps.isEmpty() ? EnumSet.noneOf(ChangeOp.class) : EnumSet.copyOf(builder.publishOps);
        this.eventRetention = builder.eventRetention;
        this.eventMaxCount = builder.eventMaxCount;
        this.changeKeyCap = builder.changeKeyCap;

        this.dataSource = new DataSource(dbPath);
        initializeDatabase();
        this.json1 = detectJson1();

        // Start sweeper if interval is not null
        if (sweepInterval != null) {
            this.sweeper = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "lytecache-sweeper");
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
            Thread t = new Thread(r, "lytecache-buffer-flusher");
            t.setDaemon(true);
            return t;
        });
        bufferFlusher.scheduleAtFixedRate(this::flushLruBuffer,
                BUFFER_FLUSH_INTERVAL_MS, BUFFER_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new LyteCache with zero-config defaults.
     * Equivalent to {@code LyteCache.builder().build()}.
     *
     * @throws LyteCacheException if initialization fails
     */
    public LyteCache() {
        this(new Builder());
    }

    /**
     * Creates a new builder for configuring LyteCache.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default database path for this environment.
     * Linux: $XDG_CACHE_HOME/lytecache or ~/.cache/lytecache
     * macOS: ~/Library/Caches/lytecache
     * Windows: %LOCALAPPDATA%\lytecache
     *
     * <p>Can be overridden via LYTECACHE_PATH environment variable.
     *
     * @return the default path
     */
    public static Path defaultPath() {
        String envPath = System.getenv("LYTECACHE_PATH");
        if (envPath != null && !envPath.isEmpty()) {
            return expandUser(envPath);
        }

        String home = System.getProperty("user.home");
        String os = System.getProperty("os.name").toLowerCase();

        Path basePath;
        if (os.contains("win")) {
            String localAppData = System.getenv("LOCALAPPDATA");
            basePath = Paths.get(localAppData != null ? localAppData : home, "lytecache");
        } else if (os.contains("mac")) {
            basePath = Paths.get(home, "Library", "Caches", "lytecache");
        } else {
            // Linux and others: use XDG_CACHE_HOME
            String xdgCache = System.getenv("XDG_CACHE_HOME");
            if (xdgCache != null && !xdgCache.isEmpty()) {
                basePath = Paths.get(xdgCache, "lytecache");
            } else {
                basePath = Paths.get(home, ".cache", "lytecache");
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
     * LyteCache created it.
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
     * @throws LyteCacheException on database error
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
            throw new LyteCacheException("Failed to set key: " + e.getMessage(), e);
        }

        changed(ChangeOp.SET, List.of(key), 1);
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
                throw new LyteCacheException("Query failed: " + e.getMessage(), e);
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
                throw new LyteCacheException("Query failed: " + e.getMessage(), e);
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

        int removed;
        try {
            removed = dataSource.executeUpdate(conn -> {
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
            throw new LyteCacheException("Delete failed: " + e.getMessage(), e);
        }
        if (removed > 0) {
            changed(ChangeOp.DELETE, new ArrayList<>(new LinkedHashSet<>(Arrays.asList(keys))), removed);
        }
        return removed;
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
        return addInternal(key, value, ttl, true);
    }

    private boolean addInternal(String key, Object value, Duration ttl, boolean emit) {
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
                if (emit) {
                    changed(ChangeOp.SET, List.of(key), 1);
                }
                enforceCapacity();
            }
            return won;
        } catch (SQLException e) {
            throw new LyteCacheException("Add failed: " + e.getMessage(), e);
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
        boolean replaced;
        try {
            replaced = dataSource.executeUpdate(conn -> {
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
            throw new LyteCacheException("Replace failed: " + e.getMessage(), e);
        }
        if (replaced) {
            changed(ChangeOp.SET, List.of(key), 1);
        }
        return replaced;
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

        String old;
        try {
            old = dataSource.executeUpdate(conn -> {
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
            throw new LyteCacheException("getSet failed: " + e.getMessage(), e);
        }
        changed(ChangeOp.SET, List.of(key), 1);
        return old;
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
            throw new LyteCacheException("setAll failed: " + e.getMessage(), e);
        }
        // ONE change record for the whole batch, never one per key.
        changed(ChangeOp.SET, new ArrayList<>(entries.keySet()), entries.size());
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
            throw new LyteCacheException("getAll failed: " + e.getMessage(), e);
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
            throw new LyteCacheException("expire failed: " + e.getMessage(), e);
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
            throw new LyteCacheException("persist failed: " + e.getMessage(), e);
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
            throw new LyteCacheException("ttl failed: " + e.getMessage(), e);
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
            throw new LyteCacheException("touch failed: " + e.getMessage(), e);
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

        String incrResult;
        try {
            incrResult = dataSource.executeUpdate(conn -> {
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
            throw new LyteCacheException("incr failed: " + e.getMessage(), e);
        }
        changed(ChangeOp.SET, List.of(key), 1);
        return incrResult;
    }

    // -- hashes: field-level access to JSON object values ------------------------------------
    //
    // A hash is not a separate type: it is a view over an ordinary value_type-4 row whose JSON
    // root is an object. Anything written with set(key, map) is usable with hget, and get() on a
    // hash returns the whole document. The reason these exist is atomicity: on the JSON1 path
    // every write is computed inside SQLite from the row's *current* value (json_set /
    // json_remove), so two processes updating different fields never lose each other's write.
    // Where an exact return value needs more than one statement (hset's "created", hdel's count),
    // guarded writes run inside one BEGIN IMMEDIATE transaction; no write depends on data read
    // into Java. The read-modify-write fallback (SQLite without JSON1, or
    // LYTECACHE_FORCE_JSON_FALLBACK=1) holds the write lock throughout.

    private static final String FORCE_JSON_FALLBACK_ENV = "LYTECACHE_FORCE_JSON_FALLBACK";
    private static final int HSET_MANY_PER_STATEMENT = 500;
    private static final int JSON_SET_PAIRS = 30; // stays under builds with a 100-argument limit
    private static final String DOC = "CAST(cache.value AS TEXT)";
    private static final String LIVE = "(cache.expires_at IS NULL OR cache.expires_at > :now)";
    private static final String EXPIRED = "(cache.expires_at IS NOT NULL AND cache.expires_at <= :now)";
    private static final String ROOT_TYPE = "(CASE WHEN json_valid(" + DOC + ") THEN json_type(" + DOC + ") END)";
    private static final String IS_OBJ = "(cache.value_type = 4 AND CASE WHEN json_valid(" + DOC + ") THEN json_type("
            + DOC + ") END = 'object')";
    private static final String[] TYPE_NAMES = {"bytes", "str", "int", "float", "json", "pickle", "java-native"};

    /** Field names are always bound, never interpolated: $."field" with \ and " escaped, in SQL. */
    private static String path(String param) {
        return "('$.\"' || replace(replace(:" + param + ", '\\', '\\\\'), '\"', '\\\"') || '\"')";
    }

    private static String fieldType(String param) {
        return "(CASE WHEN " + IS_OBJ + " THEN json_type(" + DOC + ", " + path(param) + ") END)";
    }

    /**
     * JSON text for a REAL with the fewest digits that round-trip exactly. SQLite before ~3.5x
     * renders a REAL into JSON (and CAST(x AS TEXT)) with 15 significant digits, which would store
     * 0.1 + 0.2 as 0.3; this picks the shortest of 15/16/17 digits that reads back to the same
     * double, on every SQLite version.
     */
    private static String floatText(String expr) {
        String p15 = "printf('%!.15g', " + expr + ")";
        String p16 = "printf('%!.16g', " + expr + ")";
        String p17 = "printf('%!.17g', " + expr + ")";
        return "(CASE WHEN CAST(" + p15 + " AS REAL) = " + expr + " THEN " + p15 + " WHEN CAST(" + p16 + " AS REAL) = "
                + expr + " THEN " + p16 + " ELSE " + p17 + " END)";
    }

    /**
     * An UPSERT that creates {@code freshDoc} for a missing/expired key, or rewrites a live JSON
     * object to {@code liveDoc} when {@code where} holds. expires_at is preserved on a live row
     * and size_bytes is recomputed from the new value in the same statement.
     */
    private static String upsertHashSql(String liveDoc, String freshDoc, String where) {
        String newDoc = "CAST(CASE WHEN " + EXPIRED + " THEN " + freshDoc + " ELSE " + liveDoc + " END AS BLOB)";
        return "INSERT INTO cache (key, namespace, value, value_type, created_at, expires_at, last_accessed, "
                + "access_count, size_bytes) VALUES (:key, :ns, CAST(" + freshDoc + " AS BLOB), 4, :now, :exp, :now, 0, "
                + "length(CAST(" + freshDoc + " AS BLOB))) ON CONFLICT(namespace, key) DO UPDATE SET "
                + "value = " + newDoc + ", value_type = 4, "
                + "created_at = CASE WHEN " + EXPIRED + " THEN :now ELSE cache.created_at END, "
                + "expires_at = CASE WHEN " + EXPIRED + " THEN :exp ELSE cache.expires_at END, "
                + "last_accessed = :now, "
                + "access_count = CASE WHEN " + EXPIRED + " THEN 0 ELSE cache.access_count + 1 END, "
                + "size_bytes = length(" + newDoc + ") WHERE " + EXPIRED + " OR " + where;
    }

    private boolean detectJson1() {
        String forced = System.getenv(FORCE_JSON_FALLBACK_ENV);
        if (forced == null || forced.isEmpty()) {
            forced = System.getProperty("lytecache.forceJsonFallback"); // same knob, settable in-process
        }
        if (forced != null && !forced.isEmpty()) {
            logger.warning("lytecache: " + FORCE_JSON_FALLBACK_ENV
                    + " is set; hash operations use the read-modify-write fallback");
            return false;
        }
        try {
            return dataSource.executeUpdate(conn -> {
                try (Statement s = conn.createStatement();
                        ResultSet rs = s.executeQuery("SELECT json_valid('{}'), '{\"a\\\"b\":[1]}' -> '$.\"a\\\"b\"', "
                                + "sqlite_version()")) {
                    rs.next();
                    boolean ok = rs.getInt(1) == 1 && "[1]".equals(rs.getString(2));
                    if (!ok) {
                        logger.warning("lytecache: SQLite " + rs.getString(3) + " lacks the JSON1 support hash "
                                + "operations need (JSON functions, the -> operator and escaped path labels); falling "
                                + "back to a slower read-modify-write under BEGIN IMMEDIATE");
                    }
                    return ok;
                } catch (SQLException e) {
                    logger.warning("lytecache: SQLite lacks JSON1 (" + e.getMessage()
                            + "); hash operations fall back to read-modify-write under BEGIN IMMEDIATE");
                    return false;
                }
            });
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Which hash implementation this cache uses: {@code "json1"} (single SQL statements) or
     * {@code "fallback"} (read-modify-write under BEGIN IMMEDIATE, when the linked SQLite lacks
     * JSON1). Also reported by {@link CacheStats#hashMode()}.
     *
     * @return the hash mode
     */
    public String hashMode() {
        return json1 ? "json1" : "fallback";
    }

    private Map<String, Object> hashParams(String key, long now, Duration ttl) {
        Map<String, Object> p = new HashMap<>();
        p.put("ns", namespace);
        p.put("key", key);
        p.put("now", now);
        p.put("exp", ttl == null ? null : now + ttl.toMillis());
        return p;
    }

    private static SerializationException wrongType(String key, int valueType, String rootType) {
        if (valueType != TypeCode.JSON) {
            String name = valueType >= 0 && valueType < TYPE_NAMES.length ? TYPE_NAMES[valueType] : "unknown";
            return new SerializationException("key '" + key + "' holds a value of type " + name + " (value_type="
                    + valueType + "), not a JSON object; hash operations apply only to JSON objects (value_type=4)");
        }
        if (rootType == null) {
            return new SerializationException("key '" + key + "' holds malformed JSON (value_type=4)");
        }
        return new SerializationException("key '" + key + "' holds a JSON " + rootType
                + ", not a JSON object; hash operations apply only to JSON objects");
    }

    private void checkHashType(Connection conn, String key, long now) throws SQLException {
        Map<String, Object> p = hashParams(key, now, null);
        try (PreparedStatement ps = NamedSql.of("SELECT cache.value_type, " + ROOT_TYPE
                + " FROM cache WHERE namespace = :ns AND key = :key AND " + LIVE).prepare(conn, p);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next() && !(rs.getInt(1) == TypeCode.JSON && "object".equals(rs.getString(2)))) {
                throw wrongType(key, rs.getInt(1), rs.getString(2));
            }
        }
    }

    private static int run(Connection conn, String sql, Map<String, Object> params) throws SQLException {
        try (PreparedStatement ps = NamedSql.of(sql).prepare(conn, params)) {
            return ps.executeUpdate();
        }
    }

    /** Runs {@code body} inside BEGIN IMMEDIATE on the write connection; rolls back on any error. */
    private <T> T inImmediate(SqlFunction<T> body) {
        try {
            return dataSource.executeUpdate(conn -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("BEGIN IMMEDIATE");
                }
                try {
                    T result = body.apply(conn);
                    try (Statement s = conn.createStatement()) {
                        s.execute("COMMIT");
                    }
                    return result;
                } catch (RuntimeException | SQLException e) {
                    try (Statement s = conn.createStatement()) {
                        s.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                        // best-effort rollback
                    }
                    throw e;
                }
            });
        } catch (SQLException e) {
            throw new LyteCacheException("hash operation failed: " + e.getMessage(), e);
        }
    }

    private void capacityCheck(String key) {
        if (eviction == Eviction.NOEVICTION) {
            checkCapacityBeforeWrite(key);
        }
    }

    /** Fallback path: the whole live document, or null if missing/expired. */
    private ObjectNode loadHash(Connection conn, String key, long now) throws SQLException {
        Map<String, Object> p = hashParams(key, now, null);
        try (PreparedStatement ps = NamedSql.of("SELECT value, value_type FROM cache WHERE namespace = :ns AND key = :key AND "
                + LIVE).prepare(conn, p); ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                return null;
            }
            int vt = rs.getInt(2);
            if (vt != TypeCode.JSON) {
                throw wrongType(key, vt, null);
            }
            com.fasterxml.jackson.databind.JsonNode node;
            try {
                node = JsonSupport.EXACT.readTree(new String(rs.getBytes(1), java.nio.charset.StandardCharsets.UTF_8));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw wrongType(key, TypeCode.JSON, null);
            }
            if (!(node instanceof ObjectNode obj)) {
                throw wrongType(key, TypeCode.JSON, JsonSupport.rootType(node));
            }
            return obj;
        }
    }

    private static <R> Map.Entry<Boolean, R> entry(boolean changed, R result) {
        return new AbstractMap.SimpleImmutableEntry<>(changed, result); // Map.entry rejects null
    }

    @FunctionalInterface
    private interface HashMutation<R> {
        /** Mutates {@code doc} in place; returns whether it changed, and the result. */
        Map.Entry<Boolean, R> apply(ObjectNode doc);
    }

    /** Fallback path: read, mutate in Java, write back -- all under BEGIN IMMEDIATE. */
    private <R> R rmwHash(String key, Duration ttl, boolean create, HashMutation<R> mutate) {
        long now = System.currentTimeMillis();
        boolean[] wrote = {false};
        R result = inImmediate(conn -> {
            ObjectNode doc = loadHash(conn, key, now);
            boolean exists = doc != null;
            if (doc == null) {
                if (!create) {
                    return mutate.apply(JsonSupport.EXACT.createObjectNode()).getValue();
                }
                if (eviction == Eviction.NOEVICTION) {
                    checkCapacityInTx(conn, key);
                }
                doc = JsonSupport.EXACT.createObjectNode();
            }
            Map.Entry<Boolean, R> r = mutate.apply(doc);
            if (!r.getKey()) {
                return r.getValue();
            }
            byte[] blob = JsonSupport.compact(doc).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Map<String, Object> p = hashParams(key, now, ttl);
            p.put("v", blob);
            p.put("size", (long) blob.length);
            if (exists) {
                run(conn, "UPDATE cache SET value = :v, size_bytes = :size, last_accessed = :now, "
                        + "access_count = access_count + 1 WHERE namespace = :ns AND key = :key", p);
            } else {
                run(conn, "INSERT OR REPLACE INTO cache (key, namespace, value, value_type, created_at, expires_at, "
                        + "last_accessed, access_count, size_bytes) VALUES (:key, :ns, :v, 4, :now, :exp, :now, 0, :size)", p);
            }
            wrote[0] = true;
            return r.getValue();
        });
        if (wrote[0]) {
            changed(ChangeOp.SET, List.of(key), 1);
            enforceCapacity();
        }
        return result;
    }

    private void checkCapacityInTx(Connection conn, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM cache WHERE namespace = ?")) {
            ps.setString(1, namespace);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getLong(1) >= maxKeys) {
                    throw new CacheFullException("Cache is full (keys=" + rs.getLong(1) + ", max=" + maxKeys + ")");
                }
                if (rs.getLong(2) >= maxBytes) {
                    throw new CacheFullException("Cache is full (bytes=" + rs.getLong(2) + ", max=" + maxBytes + ")");
                }
            }
        }
    }

    private static void requireField(String field) {
        if (field == null) {
            throw new IllegalArgumentException("hash field names must not be null");
        }
    }

    /**
     * Atomically sets one field of the JSON object at {@code key}. A missing (or expired) key is
     * created as a new object with <b>no expiry</b>. Equivalent to
     * {@code hset(key, field, value, null)}.
     *
     * @param key the key
     * @param field the field name (taken literally: {@code "a.b"} is a top-level field)
     * @param value the field value, encoded as JSON
     * @return true if the field was new, false if it replaced a value
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public boolean hset(String key, String field, Object value) {
        return hset(key, field, value, null);
    }

    /**
     * Atomically sets one field of the JSON object at {@code key}. A missing (or expired) key is
     * created as a new object, with {@code ttlIfCreated} as its expiry (null: none); that TTL is
     * ignored for an existing key, whose expiry a field write never changes.
     *
     * @param key the key
     * @param field the field name (taken literally)
     * @param value the field value, encoded as JSON
     * @param ttlIfCreated the TTL to give a newly created key, or null
     * @return true if the field was new, false if it replaced a value
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public boolean hset(String key, String field, Object value, Duration ttlIfCreated) {
        checkNotClosed();
        requireField(field);
        String encoded = JsonSupport.encodeFieldValue(value);
        if (!json1) {
            return rmwHash(key, ttlIfCreated, true, doc -> {
                boolean created = !doc.has(field);
                doc.set(field, readExact(encoded));
                return entry(true, created);
            });
        }
        capacityCheck(key);
        long now = System.currentTimeMillis();
        Map<String, Object> p = hashParams(key, now, ttlIfCreated);
        p.put("f0", field);
        p.put("v0", encoded);
        String setExpr = "json_set(" + DOC + ", " + path("f0") + ", json(:v0))";
        boolean created = inImmediate(conn -> {
            if (run(conn, upsertHashSql(setExpr, "json_object(:f0, json(:v0))",
                    "(" + fieldType("f0") + " IS NULL AND " + IS_OBJ + ")"), p) == 1) {
                return true;
            }
            int n = run(conn, "UPDATE cache SET value = CAST(" + setExpr + " AS BLOB), size_bytes = length(CAST(" + setExpr
                    + " AS BLOB)), last_accessed = :now, access_count = access_count + 1 WHERE namespace = :ns AND key = :key AND "
                    + LIVE + " AND " + IS_OBJ, p);
            if (n == 0) {
                checkHashType(conn, key, now);
            }
            return false;
        });
        changed(ChangeOp.SET, List.of(key), 1);
        enforceCapacity();
        return created;
    }

    private static com.fasterxml.jackson.databind.JsonNode readExact(String json) {
        try {
            return JsonSupport.EXACT.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new SerializationException("invalid JSON: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * Sets a field only if it is absent, atomically. A missing key is created as by
     * {@link #hset(String, String, Object, Duration)}.
     *
     * @param key the key
     * @param field the field name
     * @param value the field value
     * @param ttlIfCreated the TTL for a newly created key, or null
     * @return true if the field was set
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public boolean hsetnx(String key, String field, Object value, Duration ttlIfCreated) {
        checkNotClosed();
        requireField(field);
        String encoded = JsonSupport.encodeFieldValue(value);
        if (!json1) {
            return rmwHash(key, ttlIfCreated, true, doc -> {
                if (doc.has(field)) {
                    return entry(false, false);
                }
                doc.set(field, readExact(encoded));
                return entry(true, true);
            });
        }
        capacityCheck(key);
        long now = System.currentTimeMillis();
        Map<String, Object> p = hashParams(key, now, ttlIfCreated);
        p.put("f0", field);
        p.put("v0", encoded);
        boolean won = inImmediate(conn -> {
            if (run(conn, upsertHashSql("json_set(" + DOC + ", " + path("f0") + ", json(:v0))", "json_object(:f0, json(:v0))",
                    "(" + fieldType("f0") + " IS NULL AND " + IS_OBJ + ")"), p) == 1) {
                return true;
            }
            checkHashType(conn, key, now);
            return false;
        });
        if (won) {
            changed(ChangeOp.SET, List.of(key), 1);
            enforceCapacity();
        }
        return won;
    }

    /**
     * Sets a field only if it is absent; a missing key gets no expiry.
     *
     * @param key the key
     * @param field the field name
     * @param value the field value
     * @return true if the field was set
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public boolean hsetnx(String key, String field, Object value) {
        return hsetnx(key, field, value, null);
    }

    /**
     * Sets several fields at once, atomically (Redis' multi-field HSET): all land in one
     * transaction or none do.
     *
     * @param key the key
     * @param fields the fields to set, in order
     * @param ttlIfCreated the TTL for a newly created key, or null
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public void hsetMany(String key, Map<String, ?> fields, Duration ttlIfCreated) {
        checkNotClosed();
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (Map.Entry<String, ?> e : fields.entrySet()) {
            requireField(e.getKey());
            entries.add(Map.entry(e.getKey(), JsonSupport.encodeFieldValue(e.getValue())));
        }
        if (entries.isEmpty()) {
            return;
        }
        if (!json1) {
            rmwHash(key, ttlIfCreated, true, doc -> {
                for (Map.Entry<String, String> e : entries) {
                    doc.set(e.getKey(), readExact(e.getValue()));
                }
                return entry(true, null);
            });
            return;
        }
        capacityCheck(key);
        long now = System.currentTimeMillis();
        inImmediate(conn -> {
            // SQLite caps function arguments and bound variables, so a large mapping is applied as
            // several statements -- each json_set nested over groups of pairs -- in one transaction.
            for (int start = 0; start < entries.size(); start += HSET_MANY_PER_STATEMENT) {
                int end = Math.min(start + HSET_MANY_PER_STATEMENT, entries.size());
                Map<String, Object> p = hashParams(key, now, ttlIfCreated);
                String liveDoc = DOC;
                String freshDoc = "'{}'";
                for (int g = start; g < end; g += JSON_SET_PAIRS) {
                    List<String> pairs = new ArrayList<>();
                    for (int i = g; i < Math.min(g + JSON_SET_PAIRS, end); i++) {
                        p.put("f" + i, entries.get(i).getKey());
                        p.put("v" + i, entries.get(i).getValue());
                        pairs.add(path("f" + i) + ", json(:v" + i + ")");
                    }
                    String joined = String.join(", ", pairs);
                    liveDoc = "json_set(" + liveDoc + ", " + joined + ")";
                    freshDoc = "json_set(" + freshDoc + ", " + joined + ")";
                }
                if (run(conn, upsertHashSql(liveDoc, freshDoc, IS_OBJ), p) == 0) {
                    checkHashType(conn, key, now);
                }
            }
            return null;
        });
        changed(ChangeOp.SET, List.of(key), 1);
        enforceCapacity();
    }

    /**
     * Sets several fields at once, atomically; a missing key gets no expiry.
     *
     * @param key the key
     * @param fields the fields to set
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public void hsetMany(String key, Map<String, ?> fields) {
        hsetMany(key, fields, null);
    }

    /** One field's JSON text per requested field (null where missing), or null for a missing key. */
    private List<String> hmgetRaw(String key, List<String> fields) {
        checkNotClosed();
        for (String f : fields) {
            requireField(f);
        }
        long now = System.currentTimeMillis();
        try {
            return dataSource.executeQuery(conn -> {
                List<String> out = new ArrayList<>();
                if (!json1) {
                    ObjectNode doc = loadHash(conn, key, now);
                    recordHashRead(key, now, doc != null);
                    if (doc == null) {
                        return null;
                    }
                    for (String f : fields) {
                        out.add(doc.has(f) ? JsonSupport.write(doc.get(f)) : null);
                    }
                    return out;
                }
                Map<String, Object> p = hashParams(key, now, null);
                StringBuilder cols = new StringBuilder();
                for (int i = 0; i < fields.size(); i++) {
                    p.put("f" + i, fields.get(i));
                    cols.append(", CASE WHEN ").append(IS_OBJ).append(" THEN ").append(DOC).append(" -> ")
                            .append(path("f" + i)).append(" END");
                }
                try (PreparedStatement ps = NamedSql.of("SELECT cache.value_type, " + ROOT_TYPE + cols
                        + " FROM cache WHERE namespace = :ns AND key = :key AND " + LIVE).prepare(conn, p);
                        ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        recordHashRead(key, now, false);
                        return null;
                    }
                    if (!(rs.getInt(1) == TypeCode.JSON && "object".equals(rs.getString(2)))) {
                        throw wrongType(key, rs.getInt(1), rs.getString(2));
                    }
                    for (int i = 0; i < fields.size(); i++) {
                        out.add(rs.getString(3 + i));
                    }
                }
                recordHashRead(key, now, true);
                return out;
            });
        } catch (SQLException e) {
            throw new LyteCacheException("hget failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns one field of the JSON object at {@code key} as its natural Java type (String, Long,
     * Double, Boolean, Map, List), or null for a missing key, missing field, expired key or a
     * stored JSON null. Never throws on a miss.
     *
     * @param key the key
     * @param field the field name
     * @return the field value, or null
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public Object hget(String key, String field) {
        List<String> raw = hmgetRaw(key, List.of(field));
        return raw == null || raw.get(0) == null ? null : JsonSupport.decodeNatural(raw.get(0));
    }

    /**
     * Returns one field decoded into {@code type} with Jackson, or null on any miss.
     *
     * @param <T> the target type
     * @param key the key
     * @param field the field name
     * @param type the target class
     * @return the decoded field, or null
     * @throws SerializationException if {@code key} is not a JSON object or the field cannot be decoded
     */
    public <T> T hget(String key, String field, Class<T> type) {
        List<String> raw = hmgetRaw(key, List.of(field));
        return raw == null || raw.get(0) == null ? null : JsonSupport.decode(raw.get(0), type);
    }

    /**
     * Typed convenience for {@link #hget(String, String, Class)}.
     *
     * @param key the key
     * @param field the field name
     * @return the field as a String, or null
     */
    public String hgetString(String key, String field) {
        return hget(key, field, String.class);
    }

    /**
     * Typed convenience for {@link #hget(String, String, Class)}.
     *
     * @param key the key
     * @param field the field name
     * @return the field as a Long, or null
     */
    public Long hgetLong(String key, String field) {
        return hget(key, field, Long.class);
    }

    /**
     * Returns several fields in order, with null for each one that is missing.
     *
     * @param key the key
     * @param fields the field names
     * @return the values, one per field
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public List<Object> hmget(String key, List<String> fields) {
        List<Object> out = new ArrayList<>();
        if (fields.isEmpty()) {
            return out;
        }
        List<String> raw = hmgetRaw(key, fields);
        for (int i = 0; i < fields.size(); i++) {
            String v = raw == null ? null : raw.get(i);
            out.add(v == null ? null : JsonSupport.decodeNatural(v));
        }
        return out;
    }

    /**
     * Returns the whole object at {@code key}, in document order (empty if missing).
     *
     * @param key the key
     * @return an insertion-ordered map of the fields
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public Map<String, Object> hgetAll(String key) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : hashEntries(key, true)) {
            out.put(e.getKey(), JsonSupport.decodeNatural(e.getValue()));
        }
        return out;
    }

    /**
     * Removes fields and returns how many were present and removed. Unlike Redis, removing the
     * last field leaves an empty object rather than deleting the key. A missing key returns 0.
     *
     * @param key the key
     * @param fields the fields to remove
     * @return the number removed
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public int hdel(String key, String... fields) {
        checkNotClosed();
        List<String> unique = new ArrayList<>(new LinkedHashSet<>(Arrays.asList(fields)));
        for (String f : unique) {
            requireField(f);
        }
        if (unique.isEmpty()) {
            return 0;
        }
        if (!json1) {
            return rmwHash(key, null, false, doc -> {
                int n = 0;
                for (String f : unique) {
                    if (doc.remove(f) != null) {
                        n++;
                    }
                }
                return entry(n > 0, n);
            });
        }
        long now = System.currentTimeMillis();
        String removeExpr = "json_remove(" + DOC + ", " + path("f0") + ")";
        String sql = "UPDATE cache SET value = CAST(" + removeExpr + " AS BLOB), size_bytes = length(CAST(" + removeExpr
                + " AS BLOB)), last_accessed = :now, access_count = access_count + :bump WHERE namespace = :ns AND key = :key AND "
                + LIVE + " AND " + fieldType("f0") + " IS NOT NULL";
        int removed = inImmediate(conn -> {
            int n = 0;
            for (String f : unique) {
                Map<String, Object> p = hashParams(key, now, null);
                p.put("f0", f);
                p.put("bump", n == 0 ? 1L : 0L); // one hdel call is one access
                n += run(conn, sql, p);
            }
            if (n == 0) {
                checkHashType(conn, key, now);
            }
            return n;
        });
        if (removed > 0) {
            changed(ChangeOp.SET, List.of(key), 1);
        }
        return removed;
    }

    /**
     * Whether {@code key} is a live JSON object containing {@code field}.
     *
     * @param key the key
     * @param field the field name
     * @return true if present
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public boolean hexists(String key, String field) {
        List<String> raw = hmgetRaw(key, List.of(field));
        return raw != null && raw.get(0) != null;
    }

    /**
     * Field names in document order (empty if missing).
     *
     * @param key the key
     * @return the field names
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public List<String> hkeys(String key) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, String> e : hashEntries(key, false)) {
            out.add(e.getKey());
        }
        return out;
    }

    /**
     * Field values in document order, as natural Java types (empty if missing).
     *
     * @param key the key
     * @return the values
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public List<Object> hvals(String key) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<String, String> e : hashEntries(key, true)) {
            out.add(JsonSupport.decodeNatural(e.getValue()));
        }
        return out;
    }

    /**
     * The number of fields (0 if missing).
     *
     * @param key the key
     * @return the field count
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public int hlen(String key) {
        return hashEntries(key, false).size();
    }

    private List<Map.Entry<String, String>> hashEntries(String key, boolean withValues) {
        checkNotClosed();
        long now = System.currentTimeMillis();
        try {
            return dataSource.executeQuery(conn -> {
                List<Map.Entry<String, String>> out = new ArrayList<>();
                if (!json1) {
                    ObjectNode doc = loadHash(conn, key, now);
                    if (doc != null) {
                        lruBuffer.put(key, now);
                        Iterator<Map.Entry<String, com.fasterxml.jackson.databind.JsonNode>> it = doc.fields();
                        while (it.hasNext()) {
                            Map.Entry<String, com.fasterxml.jackson.databind.JsonNode> e = it.next();
                            out.add(Map.entry(e.getKey(), JsonSupport.write(e.getValue())));
                        }
                    }
                    return out;
                }
                String valueCol = withValues ? "CASE WHEN j.key IS NOT NULL THEN " + DOC + " -> j.fullkey END" : "NULL";
                try (PreparedStatement ps = NamedSql.of("SELECT cache.value_type, " + ROOT_TYPE + ", j.key, " + valueCol
                        + " FROM cache LEFT JOIN json_each(CASE WHEN " + IS_OBJ + " THEN " + DOC + " ELSE '{}' END) AS j "
                        + "WHERE cache.namespace = :ns AND cache.key = :key AND " + LIVE).prepare(conn, hashParams(key, now, null));
                        ResultSet rs = ps.executeQuery()) {
                    boolean first = true;
                    while (rs.next()) {
                        if (first && !(rs.getInt(1) == TypeCode.JSON && "object".equals(rs.getString(2)))) {
                            throw wrongType(key, rs.getInt(1), rs.getString(2));
                        }
                        if (first) {
                            lruBuffer.put(key, now);
                        }
                        first = false;
                        String k = rs.getString(3);
                        if (k != null) {
                            String v = rs.getString(4);
                            out.add(new AbstractMap.SimpleImmutableEntry<>(k, v == null ? "null" : v));
                        }
                    }
                }
                return out;
            });
        } catch (SQLException e) {
            throw new LyteCacheException("hash read failed: " + e.getMessage(), e);
        }
    }

    /**
     * Atomically adds {@code amount} to an integer field and returns the new value. A missing field
     * starts at 0; a missing key is created with no expiry.
     *
     * @param key the key
     * @param field the field name
     * @param amount the amount to add (may be negative)
     * @return the new value
     * @throws IllegalStateException if the field holds anything but a JSON integer
     * @throws ArithmeticException if the result would overflow a long (nothing is written)
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public long hincrBy(String key, String field, long amount) {
        return Long.parseLong(hincr(key, field, amount, true));
    }

    /**
     * Atomically adds {@code amount} to a numeric (integer or float) field and returns the new
     * value, stored as a float.
     *
     * @param key the key
     * @param field the field name
     * @param amount a finite amount to add
     * @return the new value
     * @throws IllegalStateException if the field holds anything but a JSON number
     * @throws ArithmeticException if the result would overflow a double (nothing is written)
     * @throws SerializationException if {@code key} holds anything but a JSON object
     */
    public double hincrByFloat(String key, String field, double amount) {
        if (!Double.isFinite(amount)) {
            throw new IllegalArgumentException("hincrByFloat amount must be finite");
        }
        return Double.parseDouble(hincr(key, field, amount, false));
    }

    private String hincr(String key, String field, Number amount, boolean integer) {
        checkNotClosed();
        requireField(field);
        String kind = integer ? "an integer" : "a number";
        if (!json1) {
            return rmwHash(key, null, true, doc -> {
                com.fasterxml.jackson.databind.JsonNode cur = doc.has(field) ? doc.get(field) : null;
                if (integer) {
                    if (cur != null && !(cur.isIntegralNumber() && cur.canConvertToLong())) {
                        if (cur.isIntegralNumber()) {
                            throw new ArithmeticException("incrementing field '" + field + "' of key '" + key + "' would overflow");
                        }
                        throw new IllegalStateException("hash field '" + field + "' of key '" + key + "' is not " + kind);
                    }
                    long next = Math.addExact(cur == null ? 0 : cur.longValue(), amount.longValue());
                    doc.put(field, next);
                    return entry(true, Long.toString(next));
                }
                if (cur != null && !cur.isNumber()) {
                    throw new IllegalStateException("hash field '" + field + "' of key '" + key + "' is not " + kind);
                }
                double next = (cur == null ? 0 : cur.doubleValue()) + amount.doubleValue();
                if (!Double.isFinite(next)) {
                    throw new ArithmeticException("incrementing field '" + field + "' of key '" + key + "' would overflow");
                }
                doc.put(field, new java.math.BigDecimal(Double.toString(next)));
                return entry(true, Double.toString(next));
            });
        }
        capacityCheck(key);
        long now = System.currentTimeMillis();
        Map<String, Object> p = hashParams(key, now, null);
        p.put("f0", field);
        p.put("amount", integer ? (Object) amount.longValue() : (Object) amount.doubleValue());
        String stored = "json_extract(" + DOC + ", " + path("f0") + ")";
        String newVal = integer ? "(COALESCE(" + stored + ", 0) + :amount)" : "(CAST(COALESCE(" + stored + ", 0) AS REAL) + :amount)";
        String guard = integer
                ? "COALESCE(" + fieldType("f0") + ", 'integer') = 'integer' AND typeof(" + newVal + ") = 'integer'"
                : "COALESCE(" + fieldType("f0") + ", 'real') IN ('integer', 'real') AND " + newVal
                        + " BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308";
        String storedVal = integer ? newVal : "json(" + floatText(newVal) + ")";
        String sql = upsertHashSql("json_set(" + DOC + ", " + path("f0") + ", " + storedVal + ")", "json_object(:f0, :amount)",
                "(CASE WHEN " + IS_OBJ + " THEN (" + guard + ") ELSE 0 END)")
                + " RETURNING " + (integer ? "CAST(json_extract(CAST(value AS TEXT), " + path("f0") + ") AS TEXT)"
                        : "json_extract(CAST(value AS TEXT), " + path("f0") + ")");
        String result = inImmediate(conn -> {
            try (PreparedStatement ps = NamedSql.of(sql).prepare(conn, p); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return integer ? rs.getString(1) : Double.toString(rs.getDouble(1));
                }
            }
            checkHashType(conn, key, now);
            String ft;
            try (PreparedStatement ps = NamedSql.of("SELECT " + fieldType("f0") + " FROM cache WHERE namespace = :ns AND key = :key")
                    .prepare(conn, p); ResultSet rs = ps.executeQuery()) {
                ft = rs.next() ? rs.getString(1) : null;
            }
            if ((integer && "integer".equals(ft)) || (!integer && ("integer".equals(ft) || "real".equals(ft)))) {
                throw new ArithmeticException("incrementing field '" + field + "' of key '" + key + "' would overflow");
            }
            throw new IllegalStateException("hash field '" + field + "' of key '" + key + "' holds a JSON " + ft + ", not " + kind);
        });
        changed(ChangeOp.SET, List.of(key), 1);
        enforceCapacity();
        return result;
    }

    private void recordHashRead(String key, long now, boolean found) {
        if (found) {
            recordHit();
            lruBuffer.put(key, now);
        } else {
            recordMiss();
        }
    }

    // -- events: local notification (notify / watch) ------------------------------------------
    //
    // SINGLE HOST ONLY. POLLED, NOT PUSHED. DURABLE. Not Redis Pub/Sub: events are rows in the
    // `events` table with a monotonic seq; watchers on the same machine poll it (every 50 ms by
    // default, backing off to ~1 s when idle). See docs/notifications.md.

    private static final String CACHE_CHANNEL = "__cache__";
    private static final String EVENTS_DDL = """
            CREATE TABLE IF NOT EXISTS events (
              seq        INTEGER PRIMARY KEY AUTOINCREMENT,
              channel    TEXT    NOT NULL,
              namespace  TEXT    NOT NULL DEFAULT 'default',
              payload    BLOB,
              value_type INTEGER NOT NULL DEFAULT 4,
              created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_seq_channel ON events(channel, seq);
            CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
            INSERT OR IGNORE INTO meta (k, v) VALUES ('events_version', '1')
            """;

    private void ensureEvents() {
        if (eventsReady) {
            return;
        }
        try {
            dataSource.executeUpdate(conn -> {
                try (Statement s = conn.createStatement()) {
                    for (String stmt : EVENTS_DDL.split(";")) {
                        if (!stmt.isBlank()) {
                            s.executeUpdate(stmt);
                        }
                    }
                }
                return null;
            });
            eventsReady = true;
        } catch (SQLException e) {
            throw new LyteCacheException("creating events table failed: " + e.getMessage(), e);
        }
    }

    private boolean eventsExist() throws SQLException {
        if (!eventsReady) {
            eventsReady = dataSource.executeQuery(conn -> {
                try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events'")) {
                    return rs.next();
                }
            });
        }
        return eventsReady;
    }

    private static void validateChannel(String channel) {
        if (channel == null || channel.isEmpty()) {
            throw new IllegalArgumentException("channel must be a non-empty string");
        }
        if (channel.contains("*")) {
            throw new IllegalArgumentException("channel '" + channel + "' may not contain '*' (reserved for watch patterns)");
        }
        if (channel.startsWith("__")) {
            throw new IllegalArgumentException("channel names starting with '__' are reserved: '" + channel + "'");
        }
    }

    /**
     * Publishes an event for watchers <b>on this host</b> and returns its seq.
     *
     * <p>Single host only, delivered by polling (not pushed), durable until retention deletes it
     * (default 1 hour). {@code payload} is encoded exactly like a cached value. Each call is one
     * write under the same lock as {@link #set}, so a high event rate slows cache writes -- batch
     * with {@link #notifyMany}. Call it after your own database transaction commits, never inside
     * one you might roll back.
     *
     * @param channel the channel: non-empty, no {@code *}, not starting with {@code __}
     * @param payload the payload, or null
     * @return the event's seq
     */
    public long notify(String channel, Object payload) {
        checkNotClosed();
        validateChannel(channel);
        return insertEvents(List.of(new Notification(channel, payload))).get(0);
    }

    /**
     * Publishes several events in one transaction (one write-lock acquisition), with the same
     * semantics as {@link #notify(String, Object)}.
     *
     * @param events the events, in order
     * @return their seqs, in order
     */
    public List<Long> notifyMany(List<Notification> events) {
        checkNotClosed();
        for (Notification n : events) {
            validateChannel(n.channel());
        }
        return events.isEmpty() ? List.of() : insertEvents(events);
    }

    private List<Long> insertEvents(List<Notification> events) {
        long now = System.currentTimeMillis();
        List<Serializer.SerializedValue> encoded = new ArrayList<>();
        for (Notification n : events) {
            encoded.add(n.payload() == null
                    ? new Serializer.SerializedValue("null".getBytes(java.nio.charset.StandardCharsets.UTF_8), TypeCode.JSON)
                    : serializer.serialize(n.payload()));
        }
        ensureEvents();
        SqlFunction<List<Long>> insertAll = conn -> {
            List<Long> seqs = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO events (channel, namespace, payload, value_type, created_at) VALUES (?, ?, ?, ?, ?) RETURNING seq")) {
                for (int i = 0; i < events.size(); i++) {
                    ps.setString(1, events.get(i).channel());
                    ps.setString(2, namespace);
                    ps.setBytes(3, encoded.get(i).bytes());
                    ps.setInt(4, encoded.get(i).typeCode());
                    ps.setLong(5, now);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        seqs.add(rs.getLong(1));
                    }
                }
            }
            return seqs;
        };
        List<Long> seqs;
        try {
            seqs = events.size() == 1 ? dataSource.executeUpdate(insertAll) : null;
        } catch (SQLException e) {
            throw new LyteCacheException("notify failed: " + e.getMessage(), e);
        }
        if (seqs == null) {
            seqs = inImmediate(insertAll);
        }
        // In-process optimization only: wake this cache's watchers now instead of at their next
        // poll. The committed row stays the source of truth.
        for (Subscription s : subscriptions) {
            s.wake();
        }
        return seqs;
    }

    /**
     * Subscribes to events published <b>on this host</b> to channels matching {@code patterns},
     * receiving only events published from now on. See {@link #watch(WatchOptions, String...)}.
     *
     * @param patterns exact channel names, or prefixes ending in {@code *}; {@code "*"} matches all
     * @return a subscription; close it when done
     */
    public Subscription watch(String... patterns) {
        return watch(WatchOptions.defaults(), patterns);
    }

    /**
     * Subscribes to events published <b>on this host</b> to channels matching {@code patterns}:
     * exact names, or a trailing {@code *} for a prefix ({@code "cache:invalidate:*"}); {@code "*"}
     * matches every channel. Only events from this cache's namespace are seen.
     *
     * <p>Single host only; polled (latency about the poll interval, up to about 1 s after an idle
     * spell); durable and resumable via {@link WatchOptions#fromSeq}. Delivery is at-least-once
     * from your cursor, in total seq order, to every subscriber (fan-out; no consumer groups) --
     * handlers must be idempotent. Lost events surface as a {@link GapException}.
     *
     * @param options where to start and how often to poll
     * @param patterns the channel patterns
     * @return a subscription; close it when done
     */
    public Subscription watch(WatchOptions options, String... patterns) {
        checkNotClosed();
        if (patterns.length == 0) {
            throw new IllegalArgumentException("watch() needs at least one channel or pattern");
        }
        List<String> list = Arrays.asList(patterns);
        Subscription.channelFilter(list, new HashMap<>()); // validates
        ensureEvents();
        Long fromSeq = options.fromSeqOrNull();
        long[] start = {0};
        GapException[] gap = {null};
        try {
            dataSource.executeQuery(conn -> {
                try (Statement s = conn.createStatement()) {
                    s.execute("BEGIN");
                }
                try {
                    if (fromSeq == null) {
                        start[0] = Subscription.lastAssignedSeq(conn);
                    } else if (fromSeq == 0) {
                        Long oldest = Subscription.oldestSeq(conn);
                        start[0] = oldest != null ? oldest - 1 : Subscription.lastAssignedSeq(conn);
                    } else {
                        start[0] = fromSeq;
                        gap[0] = Subscription.gapCheck(conn, fromSeq);
                    }
                } finally {
                    try (Statement s = conn.createStatement()) {
                        s.execute("COMMIT");
                    }
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LyteCacheException("watch failed: " + e.getMessage(), e);
        }
        Subscription sub = new Subscription("jdbc:sqlite:" + dbPath.toAbsolutePath(), namespace, list, start[0], gap[0],
                options.pollIntervalValue(), serializer, subscriptions::remove);
        subscriptions.add(sub);
        return sub;
    }

    /**
     * Persists a named subscriber cursor (in the {@code meta} table). Save an event's seq after
     * you have finished handling it, then resume with
     * {@code watch(WatchOptions.defaults().fromSeq(loadCursor(name).getAsLong()), ...)}. Each save
     * is a write: on a busy stream save every N events, accepting up to N redeliveries.
     *
     * @param name the cursor name
     * @param seq the seq to store
     */
    public void saveCursor(String name, long seq) {
        checkNotClosed();
        try {
            dataSource.executeUpdate(conn -> {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v")) {
                    ps.setString(1, "cursor:" + namespace + ":" + name);
                    ps.setString(2, Long.toString(seq));
                    ps.executeUpdate();
                }
                return null;
            });
        } catch (SQLException e) {
            throw new LyteCacheException("saveCursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * The cursor last saved under {@code name}.
     *
     * @param name the cursor name
     * @return the stored seq, or empty
     */
    public OptionalLong loadCursor(String name) {
        checkNotClosed();
        try {
            return dataSource.executeQuery(conn -> {
                try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM meta WHERE k = ?")) {
                    ps.setString(1, "cursor:" + namespace + ":" + name);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next() ? OptionalLong.of(Long.parseLong(rs.getString(1))) : OptionalLong.empty();
                    }
                }
            });
        } catch (SQLException e) {
            throw new LyteCacheException("loadCursor failed: " + e.getMessage(), e);
        }
    }

    /**
     * Retention in bounded batches. Always deletes a prefix of the seq range (everything up to the
     * newest event past retention or beyond the count cap), never a hole, which keeps gap detection
     * sound even if clocks disagree.
     */
    private void sweepEvents(long now) throws SQLException {
        if (!eventsExist()) {
            return;
        }
        dataSource.executeUpdate(conn -> {
            Long bound = null;
            try (PreparedStatement ps = conn.prepareStatement("SELECT max(seq) FROM events WHERE created_at <= ?")) {
                ps.setLong(1, now - eventRetention.toMillis());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long v = rs.getLong(1);
                    if (!rs.wasNull()) {
                        bound = v;
                    }
                }
            }
            if (eventMaxCount > 0) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?")) {
                    ps.setLong(1, eventMaxCount);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next() && (bound == null || rs.getLong(1) > bound)) {
                            bound = rs.getLong(1);
                        }
                    }
                }
            }
            if (bound == null) {
                return null;
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM events WHERE seq IN (SELECT seq FROM events WHERE seq <= ? ORDER BY seq LIMIT ?)")) {
                ps.setLong(1, bound);
                ps.setInt(2, BATCH_DELETE_LIMIT);
                while (ps.executeUpdate() >= BATCH_DELETE_LIMIT) {
                    // keep deleting in bounded batches
                }
            }
            return null;
        });
    }

    // -- change listeners (in-process) and auto-publish ------------------------------------------

    /**
     * Calls {@code listener} after mutations <b>in this process</b> commit, for every operation.
     * See {@link #onChange(Consumer, Set)}.
     *
     * @param listener the listener
     * @return an unsubscribe action
     */
    public Runnable onChange(Consumer<ChangeRecord> listener) {
        return onChange(listener, EnumSet.allOf(ChangeOp.class));
    }

    /**
     * Calls {@code listener} after mutations <b>in this process</b> commit, for the given
     * operations. In-process only, with no SQLite involvement and effectively zero latency. A
     * record arrives only after its transaction commits (a rolled-back {@code setAll} notifies
     * nobody), on a dispatch thread that holds no cache lock, so a slow listener never stalls
     * writers and a listener may write to the cache. A throwing listener is logged and skipped.
     * Writes made by listeners can trigger listeners again; past 8 levels those records are dropped
     * with a warning. Bulk operations produce ONE record. TTL expiry and eviction are not reported.
     *
     * @param listener the listener
     * @param ops the operations to receive
     * @return an unsubscribe action; once it returns, the listener is never called again
     */
    public Runnable onChange(Consumer<ChangeRecord> listener, Set<ChangeOp> ops) {
        checkNotClosed();
        ChangeDispatcher d;
        synchronized (subscriptions) {
            if (dispatcher == null) {
                dispatcher = new ChangeDispatcher(); // the first listener starts the thread
            }
            d = dispatcher;
        }
        return d.add(listener, ops);
    }

    private void changed(ChangeOp op, List<String> keys, int count) {
        ChangeDispatcher d = dispatcher;
        boolean toListeners = d != null && d.wants(op);
        boolean publish = publishOps.contains(op);
        if (!toListeners && !publish) {
            return; // the common case: nothing registered
        }
        ChangeRecord record = ChangeRecord.of(op, namespace, keys, count, changeKeyCap);
        if (toListeners) {
            d.submit(record);
        }
        if (publish) {
            try {
                insertEvents(List.of(new Notification(CACHE_CHANNEL, record.toPayload())));
            } catch (LyteCacheException e) {
                if (strict) {
                    throw e;
                }
                logger.log(Level.WARNING, "lytecache: failed to publish " + op.wireName() + " change event: " + e.getMessage());
            }
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
                    throw new LyteCacheException("keys query failed: " + e.getMessage(), e);
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
            int removed = dataSource.executeUpdate(conn -> {
                String sql = "DELETE FROM cache WHERE namespace = ?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, namespace);
                    return ps.executeUpdate();
                }
            });
            if (removed > 0) {
                changed(ChangeOp.FLUSH, null, removed);
            }
        } catch (SQLException e) {
            throw new LyteCacheException("flush failed: " + e.getMessage(), e);
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

                    return new CacheStats(hits, misses, keyCount, sizeBytes, evictions, expiredRemoved, dbPath, hashMode());
                });
            } catch (SQLException e) {
                throw new LyteCacheException("stats query failed: " + e.getMessage(), e);
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
        return addInternal(lockKey, token, ttl, false);
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
            throw new LyteCacheException("lock release failed: " + e.getMessage(), e);
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
                throw new LyteCacheException("Query failed: " + e.getMessage(), e);
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
            throw new LyteCacheException("vacuum failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        for (Subscription s : new ArrayList<>(subscriptions)) {
            s.close();
        }
        ChangeDispatcher d = dispatcher;
        if (d != null) {
            d.close();
            dispatcher = null;
        }

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
            throw new LyteCacheException("Cache is closed");
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
            sweepEvents(now);
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
                throw new LyteCacheException("capacity check failed: " + e.getMessage(), e);
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
                throw new LyteCacheException("enforceCapacity failed: " + e.getMessage(), e);
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
                        // INSERT OR IGNORE: another process opening this same fresh file for the
                        // first time may win this exact race between the SELECT above and this
                        // INSERT, in which case this becomes a harmless no-op instead of a
                        // PRIMARY KEY constraint violation.
                        String insertVersion = "INSERT OR IGNORE INTO meta (k, v) VALUES ('schema_version', '1')";
                        try (Statement insertStmt = conn.createStatement()) {
                            insertStmt.executeUpdate(insertVersion);
                        }
                    }
                }

                return null;
            });
        } catch (SQLException e) {
            throw new LyteCacheException("Database initialization failed: " + e.getMessage(), e);
        }
    }

    /**
     * Builder for LyteCache configuration.
     */
    public static class Builder {
        private Path dbPath = defaultPath();
        private String namespace = "default";
        private long maxKeys = 1_000_000;
        private long maxBytes = 1024L * 1024 * 1024; // 1 GB
        private Eviction eviction = Eviction.LRU;
        private Duration sweepInterval = Duration.ofSeconds(60);
        private boolean strict = false;
        private Serializer serializer = new DefaultSerializer();
        private Set<ChangeOp> publishOps = EnumSet.noneOf(ChangeOp.class);
        private Duration eventRetention = Duration.ofHours(1);
        private long eventMaxCount = 0;
        private int changeKeyCap = 100;

        /**
         * Creates a new builder with default settings. Obtain one via {@link LyteCache#builder()}
         * rather than calling this directly.
         */
        Builder() {}

        /**
         * Sets the database file path. Optional escape hatch; omit to use the zero-config default
         * location (see {@link LyteCache#defaultPath()}).
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
         * Makes the given mutations also publish a {@link ChangeRecord} event to the reserved
         * {@code __cache__} channel, for watchers in other processes on this host. Off by default.
         * Each published mutation costs one extra event row written under the write lock (one per
         * call, not per key), and delivery is polled, not immediate -- for in-process reactions
         * use {@link LyteCache#onChange} instead.
         *
         * @param ops the operations to publish
         * @return this builder
         */
        public Builder publishChanges(ChangeOp... ops) {
            this.publishOps = ops.length == 0 ? EnumSet.noneOf(ChangeOp.class) : EnumSet.copyOf(Arrays.asList(ops));
            return this;
        }

        /**
         * How long events are kept before the sweeper deletes them, read or not (default 1 hour). A
         * subscriber offline for longer loses events, and is told so with a {@link GapException}.
         *
         * @param retention a positive duration
         * @return this builder
         */
        public Builder eventRetention(Duration retention) {
            if (retention == null || retention.isZero() || retention.isNegative()) {
                throw new IllegalArgumentException("eventRetention must be positive");
            }
            this.eventRetention = retention;
            return this;
        }

        /**
         * Additionally keeps at most the newest {@code n} events (default: no count cap).
         *
         * @param n the cap, or 0 for none
         * @return this builder
         */
        public Builder eventMaxCount(long n) {
            if (n < 0) {
                throw new IllegalArgumentException("eventMaxCount must be >= 0");
            }
            this.eventMaxCount = n;
            return this;
        }

        /**
         * How many keys a {@link ChangeRecord} lists before it is truncated (default 100).
         *
         * @param cap the cap, {@code >= 0}
         * @return this builder
         */
        public Builder changeKeyCap(int cap) {
            if (cap < 0) {
                throw new IllegalArgumentException("changeKeyCap must be >= 0");
            }
            this.changeKeyCap = cap;
            return this;
        }

        /**
         * Builds the LyteCache instance.
         *
         * @return a new, ready-to-use LyteCache
         */
        public LyteCache build() {
            return new LyteCache(this);
        }
    }
}
