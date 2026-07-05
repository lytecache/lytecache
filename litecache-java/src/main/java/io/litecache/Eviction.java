package io.litecache;

/**
 * Eviction policy for the cache when capacity limits are reached.
 *
 * <ul>
 *   <li>LRU: Least Recently Used (default)
 *   <li>TTL: Soonest expiry first
 *   <li>RANDOM: Randomly selected entries
 *   <li>NOEVICTION: Throw CacheFullException instead of evicting
 * </ul>
 */
public enum Eviction {
    /** Least Recently Used */
    LRU,
    /** Soonest expiry first */
    TTL,
    /** Random eviction */
    RANDOM,
    /** No eviction; throw on overflow */
    NOEVICTION
}
