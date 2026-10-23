package io.lytecache;

/**
 * A kind of cache mutation reported by {@link LyteCache#onChange} and published by
 * {@link LyteCache.Builder#publishChanges}.
 */
public enum ChangeOp {
    /**
     * Every write that changes a value: {@code set}, {@code setAll}, {@code add}, {@code replace},
     * {@code getSet}, counters and hash writes.
     */
    SET,
    /** {@link LyteCache#delete(String...)}. */
    DELETE,
    /** {@link LyteCache#flush()}. */
    FLUSH;

    /**
     * The operation's wire name, as used in change-record JSON ({@code "set"}, {@code "delete"},
     * {@code "flush"}).
     *
     * @return the lowercase name
     */
    public String wireName() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }
}
