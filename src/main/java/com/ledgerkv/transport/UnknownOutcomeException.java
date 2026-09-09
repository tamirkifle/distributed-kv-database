package com.ledgerkv.transport;

/**
 * Thrown when a mutation's deadline expired without an answer. The write may or may not have taken
 * effect: it can be sitting in a leader's log, committed but unacknowledged, or never have been
 * appended at all.
 *
 * <p>Distinct from an ordinary failure on purpose. Reporting a timeout as a failure invites the
 * caller to compensate for a write that actually succeeded, which is how a retry turns into a
 * double-apply. Read the key back, or retry with {@link #sequence()} to let the state machine
 * deduplicate.
 */
public final class UnknownOutcomeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String clientId;
    private final long sequence;

    public UnknownOutcomeException(String clientId, long sequence, String detail, Throwable cause) {
        super("outcome of " + clientId + "#" + sequence + " is unknown: " + detail, cause);
        this.clientId = clientId;
        this.sequence = sequence;
    }

    public String clientId() {
        return clientId;
    }

    /** The sequence the abandoned mutation carried. Retrying it under this id is deduplicated. */
    public long sequence() {
        return sequence;
    }
}
