package io.lytecache;

import java.time.Duration;

/**
 * Options for {@link LyteCache#watch(WatchOptions, String...)}. Immutable; each method returns a
 * copy.
 */
public final class WatchOptions {
    private final Long fromSeq;
    private final Duration pollInterval;

    private WatchOptions(Long fromSeq, Duration pollInterval) {
        this.fromSeq = fromSeq;
        this.pollInterval = pollInterval;
    }

    /**
     * The defaults: only events published from now on, polling every 50 ms while events flow.
     *
     * @return default options
     */
    public static WatchOptions defaults() {
        return new WatchOptions(null, Duration.ofMillis(50));
    }

    /**
     * Resume after {@code seq} (e.g. a cursor from {@link LyteCache#loadCursor}); 0 replays
     * everything still retained.
     *
     * @param seq the seq to resume after, {@code >= 0}
     * @return updated options
     */
    public WatchOptions fromSeq(long seq) {
        if (seq < 0) {
            throw new IllegalArgumentException("fromSeq must be >= 0");
        }
        return new WatchOptions(seq, pollInterval);
    }

    /**
     * How often to poll while events flow (default 50 ms); polling backs off to about 1 s while
     * idle.
     *
     * @param interval a positive interval
     * @return updated options
     */
    public WatchOptions pollInterval(Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        return new WatchOptions(fromSeq, interval);
    }

    Long fromSeqOrNull() {
        return fromSeq;
    }

    Duration pollIntervalValue() {
        return pollInterval;
    }
}
