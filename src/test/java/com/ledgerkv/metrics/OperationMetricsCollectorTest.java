package com.ledgerkv.metrics;

import com.ledgerkv.QuorumResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression coverage for the latency-metrics poisoning bug: a backward wall-clock step during a
 * quorum operation produced a negative duration that, stored verbatim, made every {@code /metrics}
 * scrape throw (the {@link OperationMetrics} constructor and {@link LatencySummary} reject negative
 * samples). The collector now clamps durations at record time so the endpoint can never be poisoned.
 */
public class OperationMetricsCollectorTest {

    private static QuorumResponse responseWithLatency(long latencyMs) {
        return new QuorumResponse(true, null, List.of(), 2, 2, latencyMs);
    }

    @Test
    public void clampsNegativeWriteLatencyToZeroSoSnapshotRenders() {
        OperationMetricsCollector collector = new OperationMetricsCollector();

        collector.recordWrite(responseWithLatency(-5));

        OperationMetrics snapshot = collector.snapshot();
        assertEquals(List.of(0L), snapshot.getLatencySamplesMs());
        // Must not throw — the exporter renders off this summary on every scrape.
        LatencySummary.from(snapshot);
    }

    @Test
    public void clampsNegativeReadLatencyToZeroSoSnapshotRenders() {
        OperationMetricsCollector collector = new OperationMetricsCollector();

        collector.recordRead(responseWithLatency(-42));

        OperationMetrics snapshot = collector.snapshot();
        assertEquals(List.of(0L), snapshot.getLatencySamplesMs());
        LatencySummary.from(snapshot);
    }

    @Test
    public void preservesNonNegativeLatencySamples() {
        OperationMetricsCollector collector = new OperationMetricsCollector();

        collector.recordWrite(responseWithLatency(7));
        collector.recordRead(responseWithLatency(3));

        assertEquals(List.of(7L, 3L), collector.snapshot().getLatencySamplesMs());
    }
}
