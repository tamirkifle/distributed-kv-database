package com.ledgerkv.metrics;

import com.ledgerkv.QuorumResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * In-process collector for quorum read and write metrics.
 */
public final class OperationMetricsCollector {

    private final Object lock = new Object();
    private long operationCount;
    private long readCount;
    private long writeCount;
    private long successCount;
    private long failureCount;
    private long quorumFailureCount;
    private long staleReadCount;
    private long conflictCount;
    private final List<Long> latencySamplesMs = new ArrayList<>();

    public void recordRead(QuorumResponse response) {
        Objects.requireNonNull(response, "response must not be null");
        synchronized (lock) {
            operationCount++;
            readCount++;
            recordOutcome(response);
            if (response.isSuccessful() && response.getStaleNodeCount() > 0) {
                staleReadCount++;
            }
            if (response.isSuccessful() && response.hasConflicts()) {
                conflictCount++;
            }
            recordLatency(response);
        }
    }

    public void recordWrite(QuorumResponse response) {
        Objects.requireNonNull(response, "response must not be null");
        synchronized (lock) {
            operationCount++;
            writeCount++;
            recordOutcome(response);
            recordLatency(response);
        }
    }

    /**
     * Records the operation's latency, clamping at zero. Quorum durations are measured with a
     * monotonic clock, but a defensive clamp here guarantees a stray non-monotonic sample (e.g. a
     * backward wall-clock step from a legacy caller) can never poison the {@code /metrics} endpoint:
     * {@link OperationMetrics} and {@link LatencySummary} reject negative samples, so an unclamped
     * value would make every scrape throw forever.
     */
    private void recordLatency(QuorumResponse response) {
        latencySamplesMs.add(Math.max(0L, response.getLatencyMs()));
    }

    public OperationMetrics snapshot() {
        synchronized (lock) {
            return new OperationMetrics(
                operationCount,
                readCount,
                writeCount,
                successCount,
                failureCount,
                quorumFailureCount,
                staleReadCount,
                conflictCount,
                latencySamplesMs
            );
        }
    }

    private void recordOutcome(QuorumResponse response) {
        if (response.isSuccessful()) {
            successCount++;
        } else {
            failureCount++;
            quorumFailureCount++;
        }
    }
}
