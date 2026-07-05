package io.litecache;

/**
 * Exception thrown when a cache lock acquisition times out.
 */
public class LockTimeoutException extends LiteCacheException {
    private static final long serialVersionUID = 1L;
    /**
     * Constructs a LockTimeoutException with the given message.
     *
     * @param message the error message
     */
    public LockTimeoutException(String message) {
        super(message);
    }

    /**
     * Constructs a LockTimeoutException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public LockTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
