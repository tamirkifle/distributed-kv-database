package com.ledgerkv.metrics;

import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot of quorum operation metrics.
 */
public final class OperationMetrics {

    private final long operationCount;
    private final long readCount;
    private final long writeCount;
    private final long successCount;
    private final long failureCount;
    private final long quorumFailureCount;
    private final long staleReadCount;
    private final long conflictCount;
    private final long hedgedRequestCount;
    private final List<Long> latencySamplesMs;

    /**
     * Backward-compatible constructor (pre-5b call sites): defaults {@code hedgedRequestCount} to 0.
     */
    public OperationMetrics(long operationCount,
                            long readCount,
                            long writeCount,
                            long successCount,
                            long failureCount,
                            long quorumFailureCount,
                            long staleReadCount,
                            long conflictCount,
                            List<Long> latencySamplesMs) {
        this(operationCount, readCount, writeCount, successCount, failureCount, quorumFailureCount,
            staleReadCount, conflictCount, 0L, latencySamplesMs);
    }

    public OperationMetrics(long operationCount,
                            long readCount,
                            long writeCount,
                            long successCount,
                            long failureCount,
                            long quorumFailureCount,
                            long staleReadCount,
                            long conflictCount,
                            long hedgedRequestCount,
                            List<Long> latencySamplesMs) {
        validateNonNegative("operationCount", operationCount);
        validateNonNegative("readCount", readCount);
        validateNonNegative("writeCount", writeCount);
        validateNonNegative("successCount", successCount);
        validateNonNegative("failureCount", failureCount);
        validateNonNegative("quorumFailureCount", quorumFailureCount);
        validateNonNegative("staleReadCount", staleReadCount);
        validateNonNegative("conflictCount", conflictCount);
        validateNonNegative("hedgedRequestCount", hedgedRequestCount);
        Objects.requireNonNull(latencySamplesMs, "latencySamplesMs must not be null");
        for (Long sample : latencySamplesMs) {
            if (sample == null) {
                throw new IllegalArgumentException("latencySamplesMs cannot contain null samples");
            }
            validateNonNegative("latency sample", sample);
        }

        this.operationCount = operationCount;
        this.readCount = readCount;
        this.writeCount = writeCount;
        this.successCount = successCount;
        this.failureCount = failureCount;
        this.quorumFailureCount = quorumFailureCount;
        this.staleReadCount = staleReadCount;
        this.conflictCount = conflictCount;
        this.hedgedRequestCount = hedgedRequestCount;
        this.latencySamplesMs = List.copyOf(latencySamplesMs);
    }

    public static OperationMetrics empty() {
        return new OperationMetrics(0, 0, 0, 0, 0, 0, 0, 0, List.of());
    }

    public long getOperationCount() {
        return operationCount;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public long getFailureCount() {
        return failureCount;
    }

    public long getQuorumFailureCount() {
        return quorumFailureCount;
    }

    public long getStaleReadCount() {
        return staleReadCount;
    }

    public long getConflictCount() {
        return conflictCount;
    }

    public long getHedgedRequestCount() {
        return hedgedRequestCount;
    }

    public List<Long> getLatencySamplesMs() {
        return latencySamplesMs;
    }

    private static void validateNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OperationMetrics)) {
            return false;
        }
        OperationMetrics that = (OperationMetrics) o;
        return operationCount == that.operationCount
            && readCount == that.readCount
            && writeCount == that.writeCount
            && successCount == that.successCount
            && failureCount == that.failureCount
            && quorumFailureCount == that.quorumFailureCount
            && staleReadCount == that.staleReadCount
            && conflictCount == that.conflictCount
            && hedgedRequestCount == that.hedgedRequestCount
            && latencySamplesMs.equals(that.latencySamplesMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationCount, readCount, writeCount, successCount, failureCount,
            quorumFailureCount, staleReadCount, conflictCount, hedgedRequestCount, latencySamplesMs);
    }

    @Override
    public String toString() {
        return "OperationMetrics{"
            + "operationCount=" + operationCount
            + ", readCount=" + readCount
            + ", writeCount=" + writeCount
            + ", successCount=" + successCount
            + ", failureCount=" + failureCount
            + ", quorumFailureCount=" + quorumFailureCount
            + ", staleReadCount=" + staleReadCount
            + ", conflictCount=" + conflictCount
            + ", hedgedRequestCount=" + hedgedRequestCount
            + ", latencySamplesMs=" + latencySamplesMs
            + '}';
    }
}
