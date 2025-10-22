package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Size-tiered compaction: SSTables of a similar size form a tier, and once a tier accumulates at
 * least {@code minThreshold} tables they are merged into one larger table. Lower write
 * amplification than leveled, at the cost of higher space and read amplification.
 *
 * <p>Tier membership is keyed by the bit-length of {@code sizeBytes}, so tables within roughly 2x
 * of each other share a tier. All output stays at level 0 (size-tiered does not use levels).
 * Tombstones may be dropped only when the chosen tier is the entire table set, since otherwise an
 * older table outside the tier could still hold a value the tombstone must shadow.
 */
public final class SizeTieredCompaction implements CompactionStrategy {

    private final int minThreshold;

    public SizeTieredCompaction(int minThreshold) {
        if (minThreshold < 2) {
            throw new IllegalArgumentException("minThreshold must be >= 2");
        }
        this.minThreshold = minThreshold;
    }

    @Override
    public Optional<CompactionTask> planCompaction(List<SSTableHandle> tables) {
        if (tables.size() < minThreshold) {
            return Optional.empty();
        }
        // Bucket by size tier; bit-length groups sizes within ~2x of each other.
        Map<Integer, List<SSTableHandle>> tiers = new LinkedHashMap<>();
        for (SSTableHandle t : tables) {
            int tier = 64 - Long.numberOfLeadingZeros(Math.max(1L, t.sizeBytes()));
            tiers.computeIfAbsent(tier, k -> new ArrayList<>()).add(t);
        }
        for (List<SSTableHandle> tier : tiers.values()) {
            if (tier.size() >= minThreshold) {
                boolean full = tier.size() == tables.size();
                return Optional.of(new CompactionTask(tier, 0, full));
            }
        }
        return Optional.empty();
    }
}
