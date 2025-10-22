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

class SizeTieredCompactionTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** Writes a table holding {@code count} entries each with a value of {@code valueLen} bytes. */
    private static SSTableHandle table(Path dir, String name, int count, int valueLen)
            throws IOException {
        Path path = dir.resolve(name);
        byte[] val = new byte[valueLen];
        Arrays.fill(val, (byte) 'x');
        try (SSTableWriter w = new SSTableWriter(path, Math.max(1, count))) {
            for (int i = 0; i < count; i++) {
                w.add(Entry.put(String.format("k%06d", i), val, i));
            }
            w.finish();
        }
        return SSTableHandle.open(path, 0);
    }

    @Test
    void triggersWhenEnoughSameSizeTables(@TempDir Path dir) throws IOException {
        List<SSTableHandle> tables = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            tables.add(table(dir, "t" + i + ".sst", 50, 16));
        }
        SizeTieredCompaction strategy = new SizeTieredCompaction(4);

        Optional<CompactionTask> task = strategy.planCompaction(tables);

        assertTrue(task.isPresent());
        assertEquals(4, task.get().inputs().size());
        assertEquals(0, task.get().outputLevel());
        assertTrue(task.get().dropTombstones(), "full compaction may drop tombstones");
        for (SSTableHandle h : tables) {
            h.close();
        }
    }

    @Test
    void noCompactionBelowThreshold(@TempDir Path dir) throws IOException {
        List<SSTableHandle> tables = Arrays.asList(
                table(dir, "a.sst", 50, 16),
                table(dir, "b.sst", 50, 16));
        SizeTieredCompaction strategy = new SizeTieredCompaction(4);

        assertFalse(strategy.planCompaction(tables).isPresent());
        for (SSTableHandle h : tables) {
            h.close();
        }
    }

    @Test
    void onlyMergesWithinTheSameSizeTier(@TempDir Path dir) throws IOException {
        // Three small tables (one tier) + two much larger tables (another tier).
        List<SSTableHandle> tables = new ArrayList<>(Arrays.asList(
                table(dir, "s0.sst", 20, 8),
                table(dir, "s1.sst", 20, 8),
                table(dir, "s2.sst", 20, 8),
                table(dir, "big0.sst", 4000, 64),
                table(dir, "big1.sst", 4000, 64)));
        SizeTieredCompaction strategy = new SizeTieredCompaction(3);

        Optional<CompactionTask> task = strategy.planCompaction(tables);

        assertTrue(task.isPresent());
        assertEquals(3, task.get().inputs().size(), "only the small tier has >= 3 tables");
        assertFalse(task.get().dropTombstones(), "larger tier remains, so cannot drop tombstones");
        for (SSTableHandle h : tables) {
            h.close();
        }
    }
}
