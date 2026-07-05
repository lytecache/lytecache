package io.litecache;

/**
 * Exception thrown when a cache operation attempts to violate capacity constraints.
 */
public class CacheFullException extends LiteCacheException {
    private static final long serialVersionUID = 1L;
    /**
     * Constructs a CacheFullException with the given message.
     *
     * @param message the error message
     */
    public CacheFullException(String message) {
        super(message);
    }

    /**
     * Constructs a CacheFullException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public CacheFullException(String message, Throwable cause) {
        super(message, cause);
    }
}
