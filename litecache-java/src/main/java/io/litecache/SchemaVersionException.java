package io.litecache;

/**
 * Exception thrown when the database schema version is incompatible.
 */
public class SchemaVersionException extends LiteCacheException {    private static final long serialVersionUID = 1L;    /**
     * Constructs a SchemaVersionException with the given message.
     *
     * @param message the error message
     */
    public SchemaVersionException(String message) {
        super(message);
    }

    /**
     * Constructs a SchemaVersionException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public SchemaVersionException(String message, Throwable cause) {
        super(message, cause);
    }
}
