/**
 * LiteCache: Redis-like embedded caching library backed by SQLite.
 *
 * Provides zero-configuration caching with:
 * <ul>
 *   <li>Automatic database creation and schema initialization
 *   <li>Thread-safe operations via WAL mode and connection pooling
 *   <li>Cross-language JSON serialization via Jackson
 *   <li>TTL and expiration with lazy and active deletion
 *   <li>Multiple eviction policies (LRU, TTL, RANDOM, NOEVICTION)
 *   <li>Atomic counters for concurrent operations
 *   <li>Background maintenance via ScheduledExecutorService
 *   <li>Process-safe distributed locking
 * </ul>
 */
module io.litecache {
    requires java.logging;
    requires transitive com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires org.xerial.sqlitejdbc;

    exports io.litecache;
}
