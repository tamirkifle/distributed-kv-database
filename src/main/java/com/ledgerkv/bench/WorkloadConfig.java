package com.ledgerkv.bench;

/** Immutable configuration for a {@link WorkloadGenerator} run. */
public final class WorkloadConfig {

    /** Which YCSB workload's op-mix to emit. */
    public final Workload workload;
    /** Records inserted in the load phase (key space size for reads). */
    public final int recordCount;
    /** Mixed operations emitted in the run phase. */
    public final int operationCount;
    /** Value size in bytes for writes. */
    public final int valueSize;
    /** Maximum scan length (entries) for SCAN ops. */
    public final int maxScanLength;
    /** Key-access distribution for the run phase. */
    public final Distribution distribution;
    /** Seed making both phases fully reproducible. */
    public final long seed;

    public WorkloadConfig(Workload workload, int recordCount, int operationCount, int valueSize,
                          int maxScanLength, Distribution distribution, long seed) {
        this.workload = workload;
        this.recordCount = recordCount;
        this.operationCount = operationCount;
        this.valueSize = valueSize;
        this.maxScanLength = maxScanLength;
        this.distribution = distribution;
        this.seed = seed;
    }
}
