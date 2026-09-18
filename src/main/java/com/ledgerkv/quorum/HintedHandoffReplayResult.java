package com.ledgerkv.quorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The outcome of one hinted-handoff replay pass, including why each hint that failed did.
 *
 * <p>The failure list earns its place. A replay that reports only "0 of 1 applied" cannot
 * distinguish a replica that is still down — the ordinary case, and not a problem — from a
 * coordinator that can no longer deliver anything at all. Operationally those need opposite
 * responses, and when this count is wrong in a test it is the difference between a five-minute
 * diagnosis and an afternoon of re-runs.
 */
public final class HintedHandoffReplayResult {

    private final int attemptedCount;
    private final int appliedCount;
    private final int remainingCount;
    private final List<Failure> failures;

    /** One hint that could not be delivered, and the exception that stopped it. */
    public static final class Failure {
        private final String targetNodeId;
        private final String key;
        private final RuntimeException cause;

        Failure(String targetNodeId, String key, RuntimeException cause) {
            this.targetNodeId = targetNodeId;
            this.key = key;
            this.cause = cause;
        }

        public String getTargetNodeId() {
            return targetNodeId;
        }

        public String getKey() {
            return key;
        }

        public RuntimeException getCause() {
            return cause;
        }

        @Override
        public String toString() {
            return targetNodeId + " key=" + key + ": " + cause;
        }
    }

    public HintedHandoffReplayResult(int attemptedCount, int appliedCount, int remainingCount) {
        this(attemptedCount, appliedCount, remainingCount, Collections.emptyList());
    }

    public HintedHandoffReplayResult(int attemptedCount, int appliedCount, int remainingCount,
                                     List<Failure> failures) {
        if (attemptedCount < 0 || appliedCount < 0 || remainingCount < 0) {
            throw new IllegalArgumentException("hint counts must be non-negative");
        }
        if (appliedCount > attemptedCount) {
            throw new IllegalArgumentException("applied count cannot exceed attempted count");
        }
        this.attemptedCount = attemptedCount;
        this.appliedCount = appliedCount;
        this.remainingCount = remainingCount;
        this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
    }

    public int getAttemptedCount() {
        return attemptedCount;
    }

    public int getAppliedCount() {
        return appliedCount;
    }

    public int getRemainingCount() {
        return remainingCount;
    }

    /** Why each undelivered hint failed, in attempt order. Empty when everything was delivered. */
    public List<Failure> getFailures() {
        return failures;
    }

    /** One line naming every failure, for an assertion message or a log. */
    public String describeFailures() {
        return failures.isEmpty() ? "no failures" : failures.toString();
    }

    @Override
    public String toString() {
        return "HintedHandoffReplayResult{attempted=" + attemptedCount
            + " applied=" + appliedCount + " remaining=" + remainingCount
            + " failures=" + describeFailures() + '}';
    }
}
