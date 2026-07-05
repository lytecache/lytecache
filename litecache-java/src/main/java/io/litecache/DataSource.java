package io.litecache;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal: manages SQLite connection pooling and WAL mode.
 */
class DataSource {
    private final Path dbPath;
    private final ConcurrentHashMap<String, Connection> readConnections = new ConcurrentHashMap<>();
    private Connection writeConnection;
    private final Object writeLock = new Object();

    public DataSource(Path dbPath) {
        this.dbPath = dbPath;
        ensureDirectoryExists();
        initializeWriteConnection();
    }

    private void ensureDirectoryExists() {
        try {
            Path parentDir = dbPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
        } catch (IOException e) {
            throw new LiteCacheException("Failed to create database directory: " + e.getMessage(), e);
        }
    }

    private void initializeWriteConnection() {
        synchronized (writeLock) {
            try {
                writeConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
                configurePragmas(writeConnection);
            } catch (SQLException e) {
                throw new LiteCacheException("Failed to initialize write connection: " + e.getMessage(), e);
            }
        }
    }

    private Connection getReadConnection() {
        // Use thread-local connections for reads
        String threadId = Thread.currentThread().getName();
        return readConnections.computeIfAbsent(threadId, k -> {
            try {
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
                configurePragmas(conn);
                return conn;
            } catch (SQLException e) {
                throw new LiteCacheException("Failed to create read connection: " + e.getMessage(), e);
            }
        });
    }

    private void configurePragmas(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("PRAGMA journal_mode=WAL");
            stmt.executeUpdate("PRAGMA synchronous=NORMAL");
            stmt.executeUpdate("PRAGMA busy_timeout=5000");
        }
    }

    public <T> T executeQuery(SqlFunction<T> fn) throws SQLException {
        Connection conn = getReadConnection();
        return fn.apply(conn);
    }

    public <T> T executeUpdate(SqlFunction<T> fn) throws SQLException {
        synchronized (writeLock) {
            return fn.apply(writeConnection);
        }
    }

    public void close() {
        synchronized (writeLock) {
            if (writeConnection != null) {
                try {
                    writeConnection.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }

        for (Connection conn : readConnections.values()) {
            try {
                conn.close();
            } catch (SQLException e) {
                // Ignore
            }
        }
    }
}
