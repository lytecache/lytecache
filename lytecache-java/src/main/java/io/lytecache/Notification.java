package io.lytecache;

/**
 * One event for {@link LyteCache#notifyMany(java.util.List)}.
 *
 * @param channel the channel to publish to
 * @param payload the payload, encoded like a cached value (null for none)
 */
public record Notification(String channel, Object payload) {}
