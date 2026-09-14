package com.ledgerkv.checker;

/**
 * What a client learned about its own operation.
 *
 * <p>The three-way split matters to {@link LinearizabilityChecker}. Collapsing {@link #UNKNOWN}
 * into {@link #FAILURE} is the standard way to get a wrong answer out of a correct checker: a
 * write that timed out may still be sitting in a leader's log, and calling it a failure tells the
 * checker a value was never written that a later read then observes.
 */
public enum OperationResult {

    /** The client received a definite answer. The operation took effect. */
    SUCCESS,

    /**
     * The client received a definite refusal, given before the operation could take effect —
     * a rejected request, or a coordinator that never reached a quorum. It did not happen.
     */
    FAILURE,

    /**
     * The client never learned the outcome: a timeout, a dropped connection, a killed node. The
     * operation may have taken effect, may still take effect, or may never have been applied at
     * all. Jepsen calls these {@code :info} operations.
     */
    UNKNOWN
}
