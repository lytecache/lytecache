package io.litecache;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Pluggable interface for custom serialization/deserialization.
 * The default implementation uses Jackson for JSON and supports all built-in type codes.
 *
 * <p>Implementations must be thread-safe.
 */
public interface Serializer {
    /**
     * Serializes a value to bytes with a type code.
     *
     * @param value the value to serialize
     * @return a SerializedValue containing the bytes and type code
     * @throws SerializationException if serialization fails
     */
    SerializedValue serialize(Object value) throws SerializationException;

    /**
     * Deserializes bytes with a type code into the target type.
     *
     * @param <T> the target type
     * @param bytes the serialized bytes
     * @param typeCode the type code
     * @param targetType the target class to deserialize into
     * @return the deserialized value
     * @throws SerializationException if deserialization fails or type code is not supported
     */
    <T> T deserialize(byte[] bytes, int typeCode, Class<T> targetType) throws SerializationException;

    /**
     * Deserializes bytes with a type code into a fully-parameterized generic type (e.g.
     * {@code Map<String, MyRecord>}, {@code List<MyRecord>}), which a raw {@link Class} can't
     * express due to type erasure. Only meaningful for JSON-encoded values (type code 4); the
     * default implementation throws {@link UnsupportedOperationException} so that existing custom
     * {@link Serializer} implementations aren't forced to add support for this on upgrade -- they
     * keep working for {@link #deserialize(byte[], int, Class)}, just not this overload.
     *
     * @param <T> the target type
     * @param bytes the serialized bytes
     * @param typeCode the type code
     * @param typeRef the target generic type
     * @return the deserialized value
     * @throws SerializationException if deserialization fails or type code is not supported
     */
    default <T> T deserialize(byte[] bytes, int typeCode, TypeReference<T> typeRef) throws SerializationException {
        throw new UnsupportedOperationException(
                getClass().getName() + " does not support deserializing into a TypeReference");
    }

    /**
     * Represents a serialized value with its type code.
     *
     * @param bytes the serialized bytes
     * @param typeCode the type code (0-6)
     */
    record SerializedValue(byte[] bytes, int typeCode) {}
}
