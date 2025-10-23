package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Leveled compaction. Level 0 holds freshly flushed, possibly overlapping tables; levels 1+ hold
 * non-overlapping tables with a {@code levelRatio}x size budget between successive levels. Lower
 * read and space amplification than size-tiered, at the cost of higher write amplification.
 *
 * <p>Planning order:
 * <ol>
 *   <li>If level 0 has at least {@code l0Trigger} tables, merge all of L0 plus every overlapping L1
 *       table into L1.</li>
 *   <li>Otherwise find the lowest level {@code n >= 1} whose total size exceeds its budget
 *       ({@code baseLevelBytes * levelRatio^(n-1)}), take its lowest-key table, and merge it with
 *       overlapping tables in level {@code n+1} into level {@code n+1}.</li>
 * </ol>
 * Tombstones may be dropped when no table currently sits below the output level.
 */
public final class LeveledCompaction implements CompactionStrategy {

    private final int l0Trigger;
    private final long baseLevelBytes;
    private final int levelRatio;

    public LeveledCompaction(int l0Trigger, long baseLevelBytes, int levelRatio) {
        if (l0Trigger < 1 || baseLevelBytes < 1 || levelRatio < 2) {
            throw new IllegalArgumentException("invalid leveled compaction configuration");
        }
        this.l0Trigger = l0Trigger;
        this.baseLevelBytes = baseLevelBytes;
        this.levelRatio = levelRatio;
    }

    @Override
    public Optional<CompactionTask> planCompaction(List<SSTableHandle> tables) {
        if (tables.isEmpty()) {
            return Optional.empty();
        }
        int maxLevel = tables.stream().mapToInt(SSTableHandle::level).max().orElse(0);

        // (1) Level 0 count trigger.
        List<SSTableHandle> l0 = atLevel(tables, 0);
        if (l0.size() >= l0Trigger) {
            List<SSTableHandle> inputs = new ArrayList<>(l0);
            for (SSTableHandle t : atLevel(tables, 1)) {
                if (overlapsAny(t, l0)) {
                    inputs.add(t);
                }
            }
            boolean drop = noTableBelow(tables, 1);
            return Optional.of(new CompactionTask(inputs, 1, drop));
        }

        // (2) Per-level byte budget trigger, lowest level first.
        for (int n = 1; n <= maxLevel; n++) {
            List<SSTableHandle> levelN = atLevel(tables, n);
            long total = levelN.stream().mapToLong(SSTableHandle::sizeBytes).sum();
            if (total > budgetFor(n) && !levelN.isEmpty()) {
                SSTableHandle picked = levelN.stream()
                        .min(Comparator.comparing(SSTableHandle::firstKey))
                        .get();
                List<SSTableHandle> inputs = new ArrayList<>();
                inputs.add(picked);
                for (SSTableHandle t : atLevel(tables, n + 1)) {
                    if (t.overlaps(picked)) {
                        inputs.add(t);
                    }
                }
                boolean drop = noTableBelow(tables, n + 1);
                return Optional.of(new CompactionTask(inputs, n + 1, drop));
            }
        }
        return Optional.empty();
    }

    private long budgetFor(int level) {
        long budget = baseLevelBytes;
        for (int i = 1; i < level; i++) {
            budget *= levelRatio;
        }
        return budget;
    }

    private static List<SSTableHandle> atLevel(List<SSTableHandle> tables, int level) {
        List<SSTableHandle> out = new ArrayList<>();
        for (SSTableHandle t : tables) {
            if (t.level() == level) {
                out.add(t);
            }
        }
        return out;
    }

    private static boolean overlapsAny(SSTableHandle t, List<SSTableHandle> others) {
        for (SSTableHandle o : others) {
            if (t.overlaps(o)) {
                return true;
            }
        }
        return false;
    }

    private static boolean noTableBelow(List<SSTableHandle> tables, int level) {
        for (SSTableHandle t : tables) {
            if (t.level() > level) {
                return false;
            }
        }
        return true;
    }
}
