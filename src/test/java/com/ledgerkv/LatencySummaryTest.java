package com.ledgerkv;

import com.ledgerkv.metrics.LatencySummary;
import com.ledgerkv.metrics.OperationMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LatencySummaryTest {

    @Test
    @DisplayName("Latency summary computes deterministic nearest-rank percentiles")
    void latencySummaryComputesDeterministicPercentiles() {
        List<Long> samples = LongStream.rangeClosed(1, 100)
            .boxed()
            .collect(Collectors.toCollection(ArrayList::new));
        samples.add(0, samples.remove(99));

        LatencySummary summary = LatencySummary.fromSamples(samples);

        assertEquals(50, summary.getP50Ms());
        assertEquals(95, summary.getP95Ms());
        assertEquals(99, summary.getP99Ms());
    }

    @Test
    @DisplayName("Empty latency summary reports zero percentiles")
    void emptyLatencySummaryReportsZeroPercentiles() {
        LatencySummary summary = LatencySummary.empty();

        assertEquals(0, summary.getSampleCount());
        assertEquals(0, summary.getP50Ms());
        assertEquals(0, summary.getP95Ms());
        assertEquals(0, summary.getP99Ms());
    }

    @Test
    @DisplayName("Latency summary consumes operation metrics without mutating samples")
    void latencySummaryConsumesOperationMetricsWithoutMutatingSamples() {
        List<Long> samples = new ArrayList<>(List.of(30L, 10L, 20L, 40L));
        OperationMetrics metrics = new OperationMetrics(4, 2, 2, 4, 0, 0, 0, 0, samples);

        LatencySummary summary = LatencySummary.from(metrics);

        assertEquals(4, summary.getSampleCount());
        assertEquals(20, summary.getP50Ms());
        assertEquals(40, summary.getP95Ms());
        assertEquals(List.of(30L, 10L, 20L, 40L), metrics.getLatencySamplesMs());
    }

    @Test
    @DisplayName("Latency summary rejects null and negative samples")
    void latencySummaryRejectsNullAndNegativeSamples() {
        List<Long> samplesWithNull = new ArrayList<>();
        samplesWithNull.add(null);

        assertThrows(NullPointerException.class, () -> LatencySummary.fromSamples(null));
        assertThrows(IllegalArgumentException.class, () -> LatencySummary.fromSamples(samplesWithNull));
        assertThrows(IllegalArgumentException.class, () -> LatencySummary.fromSamples(List.of(-1L)));
    }
}
