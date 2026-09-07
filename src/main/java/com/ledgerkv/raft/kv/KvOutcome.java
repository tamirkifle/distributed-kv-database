package com.ledgerkv.raft.kv;

import java.util.Objects;

/**
 * What became of one client mutation, as read back from {@link RaftKvStateMachine} after the entry
 * carrying it was applied.
 *
 * <p>This exists because a committed log index is not by itself proof that <em>your</em> command
 * took effect. A leader change can replace an uncommitted entry at the index you were assigned, and
 * the state machine may also refuse a command outright. Matching on
 * {@code (clientId, sequence, fingerprint)} answers the question the index cannot.
 */
public final class KvOutcome {

    public enum Status {
        /** The command was applied at this sequence. */
        APPLIED,
        /** Already applied under the same sequence and command; the cached result is returned. */
        DUPLICATE,
        /** The client has since applied a higher sequence, so this one is a late arrival. */
        STALE_SEQUENCE,
        /** This sequence was used by a different command. Applying it would break at-most-once. */
        SEQUENCE_CONFLICT,
        /**
         * The session holds nothing at or above this sequence. Either the entry never applied
         * (replaced at its index after a leader change) or the session was evicted.
         */
        NOT_APPLIED
    }

    private final Status status;
    private final byte[] value;
    private final long appliedIndex;

    private KvOutcome(Status status, byte[] value, long appliedIndex) {
        this.status = Objects.requireNonNull(status, "status");
        this.value = value;
        this.appliedIndex = appliedIndex;
    }

    static KvOutcome of(Status status, byte[] value, long appliedIndex) {
        return new KvOutcome(status, value == null ? null : value.clone(), appliedIndex);
    }

    static KvOutcome of(Status status) {
        return new KvOutcome(status, null, 0L);
    }

    /** The log index the command applied at, which the public API reports as the version. */
    public long appliedIndex() {
        return appliedIndex;
    }

    public Status status() {
        return status;
    }

    /** True when the command took effect, whether on this delivery or an earlier identical one. */
    public boolean succeeded() {
        return status == Status.APPLIED || status == Status.DUPLICATE;
    }

    /** The command's result: for PUT the stored value, for DELETE the previous value or empty. */
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    @Override
    public String toString() {
        return "KvOutcome{" + status + " index=" + appliedIndex
                + " valueBytes=" + (value == null ? -1 : value.length) + '}';
    }
}
