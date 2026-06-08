package com.ledgerkv.raft;

/**
 * The deterministic state machine a committed Raft command is applied to. Implementations MUST be
 * deterministic: applying the same sequence of commands always yields the same state and results.
 * The KV state machine (sub-plan 3c) implements this; snapshotting (sub-plan 5c) adds
 * {@link #snapshot()} / {@link #restore(byte[], long, long)}.
 */
public interface StateMachine {

    /** Apply a committed command and return an opaque result (may be empty). */
    byte[] apply(byte[] command);

    /**
     * Serialize the full applied state (including any at-most-once dedup state) to bytes for a
     * Raft snapshot. The default returns empty — suitable for trivial/test state machines that
     * never compact.
     */
    default byte[] snapshot() {
        return new byte[0];
    }

    /**
     * Restore applied state from a snapshot produced by {@link #snapshot()}. {@code lastIncludedIndex}
     * / {@code lastIncludedTerm} identify the log position the snapshot covers (for state machines
     * that track it). The default is a no-op.
     */
    default void restore(byte[] data, long lastIncludedIndex, long lastIncludedTerm) {
        // no-op default
    }
}
