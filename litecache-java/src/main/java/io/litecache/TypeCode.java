package io.litecache;

/**
 * Type codes for cache values. These are part of the cross-language storage spec and must not change.
 *
 * <ul>
 *   <li>0: raw bytes
 *   <li>1: UTF-8 string
 *   <li>2: 64-bit signed integer
 *   <li>3: 64-bit IEEE double
 *   <li>4: JSON (UTF-8)
 *   <li>5: Python pickle (reserved; reading throws SerializationException)
 *   <li>6: Java serialized (reserved; reading throws SerializationException)
 * </ul>
 */
public final class TypeCode {
    /** Raw bytes; stored as-is with no interpretation. */
    public static final int BYTES = 0;
    /** UTF-8 string. */
    public static final int STRING = 1;
    /** 64-bit signed integer, stored as UTF-8 decimal text (see SPEC.md). */
    public static final int INT64 = 2;
    /** 64-bit IEEE 754 double, stored as UTF-8 decimal text (see SPEC.md). */
    public static final int FLOAT64 = 3;
    /** UTF-8 JSON text. */
    public static final int JSON = 4;
    /** Python pickle; reserved, reading throws {@link SerializationException}. */
    public static final int PYTHON_PICKLE = 5;
    /** Java native serialization; reserved, reading throws {@link SerializationException}. */
    public static final int JAVA_SERIALIZED = 6;

    private TypeCode() {}

    /**
     * Returns true if the type code is supported for reading.
     *
     * @param typeCode the type code to check
     * @return true if supported, false otherwise
     */
    public static boolean isSupported(int typeCode) {
        return typeCode >= BYTES && typeCode <= JSON;
    }
}
