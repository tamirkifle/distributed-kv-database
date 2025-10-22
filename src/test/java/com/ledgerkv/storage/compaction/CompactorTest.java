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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactorTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** Writes the given entries (must be strictly key-ascending) to a new SSTable handle at a level. */
    private static SSTableHandle table(Path dir, String name, int level, Entry... entries)
            throws IOException {
        Path path = dir.resolve(name);
        try (SSTableWriter w = new SSTableWriter(path, Math.max(1, entries.length))) {
            for (Entry e : entries) {
                w.add(e);
            }
            w.finish();
        }
        return SSTableHandle.open(path, level);
    }

    private static List<String> liveKeys(SSTableHandle h) {
        List<String> keys = new ArrayList<>();
        h.table().iterator().forEachRemaining(e -> keys.add(e.key()));
        return keys;
    }

    @Test
    void mergesOverlappingTablesNewestWins(@TempDir Path dir) throws IOException {
        SSTableHandle older = table(dir, "older.sst", 0,
                Entry.put("a", b("a-old"), 1),
                Entry.put("b", b("b-old"), 2));
        SSTableHandle newer = table(dir, "newer.sst", 0,
                Entry.put("b", b("b-new"), 5),
                Entry.put("c", b("c-new"), 6));

        Compactor compactor = new Compactor(dir, 1L << 30, new AtomicLong());
        CompactionResult result = compactor.compact(
                new CompactionTask(Arrays.asList(older, newer), 0, false));

        assertEquals(1, result.added().size());
        SSTableHandle out = result.added().get(0);
        assertEquals(Arrays.asList("a", "b", "c"), liveKeys(out));
        assertArrayEquals(b("b-new"), out.table().get("b").get().value());
        assertEquals(Arrays.asList(older, newer), result.obsolete());
        out.close();
    }

    @Test
    void dropsTombstonesAtBottomLevel(@TempDir Path dir) throws IOException {
        SSTableHandle older = table(dir, "older.sst", 0, Entry.put("k", b("v"), 1));
        SSTableHandle newer = table(dir, "newer.sst", 0, Entry.tombstone("k", 3));

        Compactor compactor = new Compactor(dir, 1L << 30, new AtomicLong());

        // Not dropping: tombstone survives as a single entry, no added table only if empty? Keep it.
        CompactionResult kept = compactor.compact(
                new CompactionTask(Arrays.asList(older, newer), 0, false));
        assertEquals(1, kept.added().size());
        assertTrue(kept.added().get(0).table().get("k").get().isTombstone());
        kept.added().get(0).close();

        // Dropping at the bottom: both the value and its tombstone disappear -> no output table.
        SSTableHandle older2 = table(dir, "older2.sst", 0, Entry.put("k", b("v"), 1));
        SSTableHandle newer2 = table(dir, "newer2.sst", 0, Entry.tombstone("k", 3));
        CompactionResult dropped = compactor.compact(
                new CompactionTask(Arrays.asList(older2, newer2), 0, true));
        assertTrue(dropped.added().isEmpty(), "empty merge output produces no SSTable");
    }

    @Test
    void rollsOutputIntoMultipleFilesPastMaxSize(@TempDir Path dir) throws IOException {
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            entries.add(Entry.put(String.format("key%04d", i), b("value-padding-" + i), i));
        }
        SSTableHandle big = table(dir, "big.sst", 0, entries.toArray(new Entry[0]));

        // Tiny max file size forces several output files.
        Compactor compactor = new Compactor(dir, 2048, new AtomicLong());
        CompactionResult result = compactor.compact(
                new CompactionTask(Arrays.asList(big), 1, false));

        assertTrue(result.added().size() > 1, "small max size should split output across files");
        // Outputs are non-overlapping and globally ordered.
        for (int i = 1; i < result.added().size(); i++) {
            String prevLast = result.added().get(i - 1).lastKey();
            String curFirst = result.added().get(i).firstKey();
            assertTrue(prevLast.compareTo(curFirst) < 0, "output files must not overlap");
        }
        // Every key is present across the outputs and at the target level.
        int total = 0;
        for (SSTableHandle h : result.added()) {
            assertEquals(1, h.level());
            total += h.table().entryCount();
            h.close();
        }
        assertEquals(500, total);
    }

    @Test
    void strategyTypesAreImmutableValues(@TempDir Path dir) throws IOException {
        SSTableHandle t = table(dir, "t.sst", 2, Entry.put("a", b("1"), 1));
        CompactionTask task = new CompactionTask(Arrays.asList(t), 3, true);
        assertEquals(3, task.outputLevel());
        assertTrue(task.dropTombstones());
        assertEquals(1, task.inputs().size());
        assertThrows(UnsupportedOperationException.class, () -> task.inputs().clear());
        t.close();
    }
}
