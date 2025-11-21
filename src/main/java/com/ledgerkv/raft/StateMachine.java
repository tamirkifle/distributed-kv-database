package com.ledgerkv.raft;

/**
 * The deterministic state machine a committed Raft command is applied to. Implementations MUST be
 * deterministic: applying the same sequence of commands always yields the same state and results.
 * The KV state machine (sub-plan 3c) implements this.
 */
public interface StateMachine {

    /** Apply a committed command and return an opaque result (may be empty). */
    byte[] apply(byte[] command);
}
