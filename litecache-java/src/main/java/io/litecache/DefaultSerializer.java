package io.litecache;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Default serializer using Jackson for JSON serialization.
 * Supports cross-language type codes and conventions matching the Python library.
 *
 * <p>Numeric values (int64/float64) are stored as UTF-8 decimal text, not raw binary,
 * so that both this library and the Python reference implementation can perform an
 * atomic single-statement SQL UPSERT (e.g. {@code CAST(CAST(value AS TEXT) + ? AS TEXT)})
 * for counters, and so that files written by either language are readable by the other.
 */
class DefaultSerializer implements Serializer {
    private final ObjectMapper mapper;

    public DefaultSerializer() {
        this.mapper = new ObjectMapper();
        // Register JSR-310 (java.time) module for ISO-8601 strings
        mapper.registerModule(new JavaTimeModule());
        // Disable timestamps for dates, use ISO-8601 strings
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        SimpleModule module = new SimpleModule();
        // BigDecimal as a JSON string to avoid float precision loss across languages.
        module.addSerializer(BigDecimal.class, new StdSerializer<BigDecimal>(BigDecimal.class) {
            @Override
            public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeString(value.toPlainString());
            }
        });
        mapper.registerModule(module);
    }

    @Override
    public SerializedValue serialize(Object value) throws SerializationException {
        if (value == null) {
            // NULL → treat as empty string with STRING type
            return new SerializedValue(new byte[0], TypeCode.STRING);
        }

        if (value instanceof String str) {
            return new SerializedValue(str.getBytes(StandardCharsets.UTF_8), TypeCode.STRING);
        }

        if (value instanceof byte[] bytes) {
            return new SerializedValue(bytes, TypeCode.BYTES);
        }

        if (value instanceof Long l) {
            return new SerializedValue(longToTextBytes(l), TypeCode.INT64);
        }

        if (value instanceof Integer i) {
            return new SerializedValue(longToTextBytes(i.longValue()), TypeCode.INT64);
        }

        if (value instanceof Double d) {
            return new SerializedValue(doubleToTextBytes(d), TypeCode.FLOAT64);
        }

        if (value instanceof Float f) {
            return new SerializedValue(doubleToTextBytes(f.doubleValue()), TypeCode.FLOAT64);
        }

        // Everything else → JSON
        try {
            String json = mapper.writeValueAsString(value);
            return new SerializedValue(json.getBytes(StandardCharsets.UTF_8), TypeCode.JSON);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize value: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, int typeCode, Class<T> targetType) throws SerializationException {
        if (!TypeCode.isSupported(typeCode)) {
            throw new SerializationException(
                    "Unsupported type code: " + typeCode + " (reading non-Java values is not supported)");
        }

        try {
            return switch (typeCode) {
                case TypeCode.BYTES -> deserializeBytes(bytes, targetType);
                case TypeCode.STRING -> deserializeString(bytes, targetType);
                case TypeCode.INT64 -> deserializeInt64(bytes, targetType);
                case TypeCode.FLOAT64 -> deserializeFloat64(bytes, targetType);
                case TypeCode.JSON -> deserializeJson(bytes, targetType);
                default -> throw new SerializationException("Unsupported type code: " + typeCode);
            };
        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerializationException("Deserialization failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, int typeCode, TypeReference<T> typeRef) throws SerializationException {
        if (typeCode != TypeCode.JSON) {
            throw new SerializationException(
                    "Cannot deserialize type code " + typeCode + " into a TypeReference; only JSON values (type code 4) support generic types");
        }
        try {
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), typeRef);
        } catch (Exception e) {
            throw new SerializationException("Deserialization failed: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeBytes(byte[] bytes, Class<T> targetType) throws SerializationException {
        if (targetType == byte[].class) {
            return (T) bytes;
        }
        if (targetType == String.class) {
            return (T) new String(bytes, StandardCharsets.UTF_8);
        }
        throw new SerializationException(
                "Cannot deserialize bytes into " + targetType.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeString(byte[] bytes, Class<T> targetType) throws SerializationException {
        String str = new String(bytes, StandardCharsets.UTF_8);
        if (targetType == String.class) {
            return (T) str;
        }
        if (targetType == byte[].class) {
            return (T) bytes;
        }
        throw new SerializationException(
                "Cannot deserialize string into " + targetType.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeInt64(byte[] bytes, Class<T> targetType) throws SerializationException {
        String text = new String(bytes, StandardCharsets.UTF_8);
        long value;
        try {
            value = Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new SerializationException("Stored int64 value is not a valid integer: " + text, e);
        }
        if (targetType == Long.class) {
            return (T) Long.valueOf(value);
        }
        if (targetType == Integer.class) {
            return (T) Integer.valueOf((int) value);
        }
        if (targetType == String.class) {
            return (T) String.valueOf(value);
        }
        throw new SerializationException(
                "Cannot deserialize int64 into " + targetType.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeFloat64(byte[] bytes, Class<T> targetType) throws SerializationException {
        String text = new String(bytes, StandardCharsets.UTF_8);
        double value;
        try {
            value = Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new SerializationException("Stored float64 value is not a valid number: " + text, e);
        }
        if (targetType == Double.class) {
            return (T) Double.valueOf(value);
        }
        if (targetType == Float.class) {
            return (T) Float.valueOf((float) value);
        }
        if (targetType == String.class) {
            return (T) String.valueOf(value);
        }
        throw new SerializationException(
                "Cannot deserialize float64 into " + targetType.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeJson(byte[] bytes, Class<T> targetType) throws JsonProcessingException {
        String json = new String(bytes, StandardCharsets.UTF_8);

        if (targetType == String.class) {
            return (T) json;
        }

        if (targetType == byte[].class) {
            return (T) bytes;
        }

        if (targetType == JsonNode.class) {
            return (T) mapper.readTree(json);
        }

        if (targetType == BigDecimal.class) {
            return (T) new BigDecimal(mapper.readTree(json).asText());
        }

        if (targetType == Map.class || targetType == Object.class) {
            return (T) mapper.readValue(json, Map.class);
        }

        if (targetType == List.class) {
            return (T) mapper.readValue(json, List.class);
        }

        // Try to deserialize into the target type
        return mapper.readValue(json, targetType);
    }

    private byte[] longToTextBytes(long value) {
        return Long.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] doubleToTextBytes(double value) {
        return Double.toString(value).getBytes(StandardCharsets.UTF_8);
    }
}
