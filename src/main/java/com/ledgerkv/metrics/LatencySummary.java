package com.ledgerkv.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable percentile summary for operation latency samples.
 */
public final class LatencySummary {

    private final long sampleCount;
    private final long p50Ms;
    private final long p95Ms;
    private final long p99Ms;

    private LatencySummary(long sampleCount, long p50Ms, long p95Ms, long p99Ms) {
        this.sampleCount = sampleCount;
        this.p50Ms = p50Ms;
        this.p95Ms = p95Ms;
        this.p99Ms = p99Ms;
    }

    public static LatencySummary empty() {
        return new LatencySummary(0, 0, 0, 0);
    }

    public static LatencySummary from(OperationMetrics metrics) {
        Objects.requireNonNull(metrics, "metrics must not be null");
        return fromSamples(metrics.getLatencySamplesMs());
    }

    public static LatencySummary fromSamples(List<Long> latencySamplesMs) {
        Objects.requireNonNull(latencySamplesMs, "latencySamplesMs must not be null");
        if (latencySamplesMs.isEmpty()) {
            return empty();
        }

        List<Long> sortedSamples = new ArrayList<>(latencySamplesMs.size());
        for (Long sample : latencySamplesMs) {
            if (sample == null) {
                throw new IllegalArgumentException("latencySamplesMs cannot contain null samples");
            }
            if (sample < 0) {
                throw new IllegalArgumentException("latency sample must be non-negative");
            }
            sortedSamples.add(sample);
        }
        Collections.sort(sortedSamples);

        return new LatencySummary(
            sortedSamples.size(),
            percentile(sortedSamples, 0.50),
            percentile(sortedSamples, 0.95),
            percentile(sortedSamples, 0.99)
        );
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public long getP50Ms() {
        return p50Ms;
    }

    public long getP95Ms() {
        return p95Ms;
    }

    public long getP99Ms() {
        return p99Ms;
    }

    private static long percentile(List<Long> sortedSamples, double percentile) {
        int rank = (int) Math.ceil(percentile * sortedSamples.size());
        int index = Math.max(0, rank - 1);
        return sortedSamples.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LatencySummary)) {
            return false;
        }
        LatencySummary that = (LatencySummary) o;
        return sampleCount == that.sampleCount
            && p50Ms == that.p50Ms
            && p95Ms == that.p95Ms
            && p99Ms == that.p99Ms;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sampleCount, p50Ms, p95Ms, p99Ms);
    }

    @Override
    public String toString() {
        return "LatencySummary{"
            + "sampleCount=" + sampleCount
            + ", p50Ms=" + p50Ms
            + ", p95Ms=" + p95Ms
            + ", p99Ms=" + p99Ms
            + '}';
    }
}
