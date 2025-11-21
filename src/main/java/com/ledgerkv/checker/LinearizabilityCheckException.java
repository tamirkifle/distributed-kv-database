package com.ledgerkv.checker;

/**
 * Thrown when a per-key sub-history exceeds the checker's bounded-history guardrail. The checker is
 * an exponential-worst-case bug finder on bounded histories, not a soundness proof; rather than
 * silently blowing up, it refuses histories larger than the configured bound.
 */
public final class LinearizabilityCheckException extends RuntimeException {

    public LinearizabilityCheckException(String message) {
        super(message);
    }
}
