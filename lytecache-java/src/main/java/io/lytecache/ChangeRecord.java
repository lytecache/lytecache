package io.lytecache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One committed mutation. A bulk operation produces ONE record, never one per key: {@code keys}
 * lists the keys the call targeted, or is null with {@code truncated} true when there were more
 * than the configured cap (default 100); {@code flush} always has null keys. {@link #toPayload()}
 * is the JSON form published to the {@code __cache__} channel.
 *
 * @param op the operation
 * @param namespace the namespace the mutation happened in
 * @param keys the keys the call targeted, or null if truncated
 * @param count keys written, or rows removed for delete/flush
 * @param truncated whether {@code keys} was omitted
 */
public record ChangeRecord(ChangeOp op, String namespace, List<String> keys, int count, boolean truncated) {
    /**
     * Creates a record, defensively copying {@code keys}.
     *
     * @param op the operation
     * @param namespace the namespace
     * @param keys the keys, or null
     * @param count keys written or rows removed
     * @param truncated whether keys was omitted
     */
    public ChangeRecord {
        keys = keys == null ? null : List.copyOf(keys);
    }

    static ChangeRecord of(ChangeOp op, String namespace, List<String> keys, int count, int cap) {
        if (keys == null || keys.size() > cap) {
            return new ChangeRecord(op, namespace, null, count, true);
        }
        return new ChangeRecord(op, namespace, keys, count, false);
    }

    /**
     * The record as the JSON object published to {@code __cache__}:
     * {@code {"op","namespace","keys","count","truncated"}}.
     *
     * @return an insertion-ordered map
     */
    public Map<String, Object> toPayload() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("op", op.wireName());
        m.put("namespace", namespace);
        m.put("keys", keys);
        m.put("count", count);
        m.put("truncated", truncated);
        return m;
    }
}
