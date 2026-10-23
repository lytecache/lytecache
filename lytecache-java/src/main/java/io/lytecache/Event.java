package io.lytecache;

import java.time.Instant;

/**
 * One event published with {@link LyteCache#notify}. The payload is decoded lazily, with the same
 * rules (and the same {@link Serializer}) as cached values.
 */
public final class Event {
    private final long seq;
    private final String channel;
    private final String namespace;
    private final Instant createdAt;
    private final byte[] payload;
    private final int valueType;
    private final Serializer serializer;

    Event(long seq, String channel, String namespace, long createdAtMs, byte[] payload, int valueType,
            Serializer serializer) {
        this.seq = seq;
        this.channel = channel;
        this.namespace = namespace;
        this.createdAt = Instant.ofEpochMilli(createdAtMs);
        this.payload = payload;
        this.valueType = valueType;
        this.serializer = serializer;
    }

    /**
     * The event's sequence number: monotonic per file, and the cursor to persist.
     *
     * @return the seq
     */
    public long seq() {
        return seq;
    }

    /**
     * The channel it was published to.
     *
     * @return the channel
     */
    public String channel() {
        return channel;
    }

    /**
     * The namespace of the cache that published it.
     *
     * @return the namespace
     */
    public String namespace() {
        return namespace;
    }

    /**
     * When it was published.
     *
     * @return the publish time
     */
    public Instant createdAt() {
        return createdAt;
    }

    /**
     * The stored value_type code of the payload (see SPEC.md).
     *
     * @return the type code
     */
    public int valueType() {
        return valueType;
    }

    /**
     * Whether the payload is null.
     *
     * @return true for a null payload
     */
    public boolean isNullPayload() {
        return payload == null || (valueType == TypeCode.JSON && "null".equals(new String(payload,
                java.nio.charset.StandardCharsets.UTF_8)));
    }

    /**
     * The payload decoded into {@code type}, exactly as {@link LyteCache#get(String, Class)} would.
     *
     * @param <T> the target type
     * @param type the target class
     * @return the payload, or null for a null payload
     * @throws SerializationException if it cannot be decoded as {@code type}
     */
    public <T> T payload(Class<T> type) {
        if (isNullPayload()) {
            return null;
        }
        return serializer.deserialize(payload, valueType, type);
    }

    /**
     * The payload as its natural Java type: String, byte[], Long, Double, or a JSON-decoded value
     * (Map, List, Boolean, ...).
     *
     * @return the payload, or null
     * @throws SerializationException if it cannot be decoded
     */
    public Object payload() {
        if (isNullPayload()) {
            return null;
        }
        Class<?> type = switch (valueType) {
            case TypeCode.BYTES -> byte[].class;
            case TypeCode.STRING -> String.class;
            case TypeCode.INT64 -> Long.class;
            case TypeCode.FLOAT64 -> Double.class;
            default -> Object.class;
        };
        return serializer.deserialize(payload, valueType, type);
    }

    @Override
    public String toString() {
        return "Event[seq=" + seq + ", channel=" + channel + ", namespace=" + namespace + ", createdAt=" + createdAt + "]";
    }
}
