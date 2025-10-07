package com.ledgerkv.metrics;

import java.util.Objects;

/**
 * Immutable snapshot of read repair and anti-entropy repair metrics.
 */
public final class RepairMetrics {

    private final long repairCount;
    private final long replicasRepaired;
    private final long totalRepairLatencyMs;

    public RepairMetrics(long repairCount, long replicasRepaired, long totalRepairLatencyMs) {
        if (repairCount < 0) {
            throw new IllegalArgumentException("repairCount must be non-negative");
        }
        if (replicasRepaired < 0) {
            throw new IllegalArgumentException("replicasRepaired must be non-negative");
        }
        if (totalRepairLatencyMs < 0) {
            throw new IllegalArgumentException("totalRepairLatencyMs must be non-negative");
        }
        this.repairCount = repairCount;
        this.replicasRepaired = replicasRepaired;
        this.totalRepairLatencyMs = totalRepairLatencyMs;
    }

    public static RepairMetrics empty() {
        return new RepairMetrics(0, 0, 0);
    }

    public long getRepairCount() {
        return repairCount;
    }

    public long getReplicasRepaired() {
        return replicasRepaired;
    }

    public long getTotalRepairLatencyMs() {
        return totalRepairLatencyMs;
    }

    public long getAverageRepairLatencyMs() {
        if (repairCount == 0) {
            return 0;
        }
        return totalRepairLatencyMs / repairCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RepairMetrics)) {
            return false;
        }
        RepairMetrics that = (RepairMetrics) o;
        return repairCount == that.repairCount
            && replicasRepaired == that.replicasRepaired
            && totalRepairLatencyMs == that.totalRepairLatencyMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(repairCount, replicasRepaired, totalRepairLatencyMs);
    }

    @Override
    public String toString() {
        return "RepairMetrics{"
            + "repairCount=" + repairCount
            + ", replicasRepaired=" + replicasRepaired
            + ", totalRepairLatencyMs=" + totalRepairLatencyMs
            + ", averageRepairLatencyMs=" + getAverageRepairLatencyMs()
            + '}';
    }
}
