package com.ledgerkv.quorum;

public final class HintedHandoffReplayResult {
    private final int attemptedCount;
    private final int appliedCount;
    private final int remainingCount;

    public HintedHandoffReplayResult(int attemptedCount, int appliedCount, int remainingCount) {
        if (attemptedCount < 0 || appliedCount < 0 || remainingCount < 0) {
            throw new IllegalArgumentException("hint counts must be non-negative");
        }
        if (appliedCount > attemptedCount) {
            throw new IllegalArgumentException("applied count cannot exceed attempted count");
        }
        this.attemptedCount = attemptedCount;
        this.appliedCount = appliedCount;
        this.remainingCount = remainingCount;
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
}
