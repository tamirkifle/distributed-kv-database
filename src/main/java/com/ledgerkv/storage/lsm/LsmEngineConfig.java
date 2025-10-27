package com.ledgerkv.storage.lsm;

import com.ledgerkv.storage.compaction.CompactionStrategy;
import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.wal.DurabilityMode;

/** Immutable {@link LsmEngine} configuration. A {@code null} {@link #strategy} disables the
 *  background compactor (useful for deterministic tests). */
public final class LsmEngineConfig {

    /** WAL durability mode. */
    public final DurabilityMode durability;
    /** Approximate active-MemTable byte size that triggers a flush to a new SSTable. */
    public final long memtableFlushBytes;
    /** Compaction strategy, or {@code null} to disable background compaction. */
    public final CompactionStrategy strategy;
    /** Roll the compactor over to a new output file once it passes this many bytes. */
    public final long maxSstableBytes;
    /** Idle poll interval for the background compactor loop, in milliseconds. */
    public final long compactionPollMillis;

    public LsmEngineConfig(DurabilityMode durability, long memtableFlushBytes,
                           CompactionStrategy strategy, long maxSstableBytes,
                           long compactionPollMillis) {
        this.durability = durability;
        this.memtableFlushBytes = memtableFlushBytes;
        this.strategy = strategy;
        this.maxSstableBytes = maxSstableBytes;
        this.compactionPollMillis = compactionPollMillis;
    }

    /** SYNC durability, 4 MB flush threshold, size-tiered compaction (min 4), 64 MB SSTables. */
    public static LsmEngineConfig defaults() {
        return new LsmEngineConfig(
                DurabilityMode.SYNC, 4L * 1024 * 1024, new SizeTieredCompaction(4),
                64L * 1024 * 1024, 50L);
    }
}
