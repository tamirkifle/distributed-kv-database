package com.ledgerkv.storage.compaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.SSTableHandle;
import com.ledgerkv.storage.lsm.SSTableWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LeveledCompactionTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** A table at {@code level} spanning keys [from, to) with a fixed value size. */
    private static SSTableHandle table(Path dir, String name, int level, int from, int to,
                                       int valueLen) throws IOException {
        Path path = dir.resolve(name);
        byte[] val = new byte[valueLen];
        Arrays.fill(val, (byte) 'x');
        try (SSTableWriter w = new SSTableWriter(path, Math.max(1, to - from))) {
            for (int i = from; i < to; i++) {
                w.add(Entry.put(String.format("k%06d", i), val, i));
            }
            w.finish();
        }
        return SSTableHandle.open(path, level);
    }

    @Test
    void l0CountTriggerMergesL0IntoL1(@TempDir Path dir) throws IOException {
        List<SSTableHandle> tables = new ArrayList<>();
        // 4 overlapping L0 tables -> trigger.
        for (int i = 0; i < 4; i++) {
            tables.add(table(dir, "l0-" + i + ".sst", 0, 0, 10, 8));
        }
        // One L1 table overlapping the L0 range, one disjoint L1 table.
        tables.add(table(dir, "l1-overlap.sst", 1, 5, 15, 8));
        tables.add(table(dir, "l1-disjoint.sst", 1, 100, 110, 8));

        LeveledCompaction strategy = new LeveledCompaction(4, 1L << 20, 10);
        Optional<CompactionTask> task = strategy.planCompaction(tables);

        assertTrue(task.isPresent());
        assertEquals(1, task.get().outputLevel());
        // 4 L0 + the overlapping L1 only (5 inputs); the disjoint L1 stays out.
        assertEquals(5, task.get().inputs().size());
        for (SSTableHandle h : tables) {
            h.close();
        }
    }

    @Test
    void levelByteBudgetTriggerMergesIntoNextLevel(@TempDir Path dir) throws IOException {
        List<SSTableHandle> tables = new ArrayList<>();
        // L1 over budget: two big tables (baseLevelBytes set tiny so L1 exceeds it).
        tables.add(table(dir, "l1-a.sst", 1, 0, 200, 64));
        tables.add(table(dir, "l1-b.sst", 1, 200, 400, 64));
        // An L2 table overlapping the lowest-key L1 table.
        tables.add(table(dir, "l2.sst", 2, 0, 100, 64));

        LeveledCompaction strategy = new LeveledCompaction(4, 1024, 10);
        Optional<CompactionTask> task = strategy.planCompaction(tables);

        assertTrue(task.isPresent());
        assertEquals(2, task.get().outputLevel());
        // The lowest-key L1 table (l1-a, keys 0..199) + overlapping L2 (keys 0..99).
        assertEquals(2, task.get().inputs().size());
        for (SSTableHandle h : tables) {
            h.close();
        }
    }

    @Test
    void dropsTombstonesOnlyAtBottomLevel(@TempDir Path dir) throws IOException {
        // L0 trigger into L1, and an L2 exists below -> must NOT drop tombstones.
        List<SSTableHandle> withLower = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            withLower.add(table(dir, "a-l0-" + i + ".sst", 0, 0, 10, 8));
        }
        withLower.add(table(dir, "a-l2.sst", 2, 0, 10, 8));
        LeveledCompaction strategy = new LeveledCompaction(4, 1L << 20, 10);
        CompactionTask t1 = strategy.planCompaction(withLower).get();
        assertFalse(t1.dropTombstones(), "L2 exists below the L1 output");
        for (SSTableHandle h : withLower) {
            h.close();
        }

        // L0 trigger into L1 with nothing below -> may drop tombstones.
        List<SSTableHandle> noLower = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            noLower.add(table(dir, "b-l0-" + i + ".sst", 0, 0, 10, 8));
        }
        CompactionTask t2 = strategy.planCompaction(noLower).get();
        assertTrue(t2.dropTombstones(), "nothing below L1 output");
        for (SSTableHandle h : noLower) {
            h.close();
        }
    }

    @Test
    void noCompactionWhenEverythingWithinBudget(@TempDir Path dir) throws IOException {
        List<SSTableHandle> tables = Arrays.asList(
                table(dir, "l0.sst", 0, 0, 10, 8),
                table(dir, "l1.sst", 1, 0, 10, 8));
        LeveledCompaction strategy = new LeveledCompaction(4, 1L << 20, 10);
        assertFalse(strategy.planCompaction(tables).isPresent());
        for (SSTableHandle h : tables) {
            h.close();
        }
    }
}
