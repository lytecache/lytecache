package io.litecache;

/**
 * Base unchecked exception for all LiteCache errors.
 */
public class LiteCacheException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a LiteCacheException with the given message.
     *
     * @param message the error message
     */
    public LiteCacheException(String message) {
        super(message);
    }

    /**
     * Constructs a LiteCacheException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public LiteCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
