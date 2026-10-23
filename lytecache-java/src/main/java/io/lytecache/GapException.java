package io.lytecache;

/**
 * Thrown by a {@link Subscription} when events it should have seen are gone: retention deleted
 * them before it read them, or its cursor is ahead of the file (the file was recreated). Never
 * silently skipped. To accept the loss, watch again from {@link #resumeSeq()}.
 */
public class GapException extends LyteCacheException {
    private static final long serialVersionUID = 1L;

    /** The subscriber's cursor. */
    private final long requestedSeq;
    /** The oldest retained seq, or the next seq to be assigned. */
    private final long oldestSeq;
    /** Whether the cursor was ahead of the last seq ever assigned. */
    private final boolean ahead;

    /**
     * Creates a gap report.
     *
     * @param requestedSeq the subscriber's cursor
     * @param oldestSeq the oldest retained seq (or the next seq to be assigned)
     * @param ahead whether the cursor is ahead of the last seq ever assigned
     */
    public GapException(long requestedSeq, long oldestSeq, boolean ahead) {
        super(ahead
                ? "cursor " + requestedSeq + " is ahead of the last event ever published (" + (oldestSeq - 1)
                        + "); the database file was probably recreated"
                : "events after seq " + requestedSeq + " are no longer retained (oldest retained is " + oldestSeq
                        + "); events " + (requestedSeq + 1) + ".." + (oldestSeq - 1) + " were lost");
        this.requestedSeq = requestedSeq;
        this.oldestSeq = oldestSeq;
        this.ahead = ahead;
    }

    /**
     * The cursor the subscription asked to resume from.
     *
     * @return the requested seq
     */
    public long requestedSeq() {
        return requestedSeq;
    }

    /**
     * The oldest event still retained.
     *
     * @return the oldest retained seq
     */
    public long oldestSeq() {
        return oldestSeq;
    }

    /**
     * Where to resume to accept the loss ({@code oldestSeq - 1}).
     *
     * @return the seq to pass as {@code fromSeq}
     */
    public long resumeSeq() {
        return oldestSeq - 1;
    }

    /**
     * Whether the cursor was ahead of the file rather than behind retention.
     *
     * @return true if the file was probably recreated
     */
    public boolean ahead() {
        return ahead;
    }
}
