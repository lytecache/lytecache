package io.lytecache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Internal: JSON encoding for hash field values, event payloads' JSON views, and the hash
 * read-modify-write fallback.
 */
final class JsonSupport {
    /** Encodes values exactly as {@link DefaultSerializer} encodes type-code-4 values. */
    static final ObjectMapper VALUES = DefaultSerializer.newMapper()
            .enable(DeserializationFeature.USE_LONG_FOR_INTS);

    /**
     * Parses documents for the fallback path without losing anything: ObjectNode keeps field
     * order, and numbers stay exact (BigInteger/BigDecimal) so a rewrite never changes them.
     */
    static final ObjectMapper EXACT = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

    private JsonSupport() {}

    /**
     * Encodes one hash field value as compact JSON text. byte[] is rejected (Jackson would store
     * base64), as are NaN and infinities, which are not JSON.
     */
    static String encodeFieldValue(Object value) {
        if (value instanceof byte[]) {
            throw new SerializationException("hash fields hold JSON values; byte[] is not supported");
        }
        if (value instanceof Double d && !Double.isFinite(d) || value instanceof Float f && !Float.isFinite(f)) {
            throw new SerializationException("cannot store NaN or Infinity in a hash field");
        }
        try {
            String text = VALUES.writeValueAsString(value);
            EXACT.readTree(text); // strict re-parse: rejects nested NaN/Infinity tokens
            return text;
        } catch (JsonProcessingException e) {
            throw new SerializationException(
                    "cannot store a value of type " + (value == null ? "null" : value.getClass().getName())
                            + " in a hash field: " + e.getOriginalMessage(), e);
        }
    }

    /** Decodes one field's JSON text to its natural Java type (Long, Double, String, Map, ...). */
    static Object decodeNatural(String json) {
        try {
            return VALUES.readValue(json, Object.class);
        } catch (JsonProcessingException e) {
            throw new SerializationException("stored hash field is not valid JSON: " + e.getOriginalMessage(), e);
        }
    }

    static <T> T decode(String json, Class<T> type) {
        try {
            return VALUES.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new SerializationException("cannot decode hash field as " + type.getName() + ": "
                    + e.getOriginalMessage(), e);
        }
    }

    /** The root type of parsed JSON, using SQLite's json_type() names. */
    static String rootType(JsonNode node) {
        return switch (node.getNodeType()) {
            case OBJECT -> "object";
            case ARRAY -> "array";
            case STRING -> "text";
            case BOOLEAN -> node.booleanValue() ? "true" : "false";
            case NULL -> "null";
            case NUMBER -> node.isIntegralNumber() ? "integer" : "real";
            default -> "unknown";
        };
    }

    /** Compact JSON text for one node, exactly as parsed. */
    static String write(JsonNode node) {
        try {
            return EXACT.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new SerializationException("cannot encode JSON: " + e.getOriginalMessage(), e);
        }
    }

    static String compact(ObjectNode node) {
        try {
            return EXACT.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new SerializationException("cannot encode hash: " + e.getOriginalMessage(), e);
        }
    }
}
