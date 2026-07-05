package io.litecache;

/**
 * Exception thrown when a value cannot be serialized or deserialized.
 */
public class SerializationException extends LiteCacheException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a SerializationException with the given message.
     *
     * @param message the error message
     */
    public SerializationException(String message) {
        super(message);
    }

    /**
     * Constructs a SerializationException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
