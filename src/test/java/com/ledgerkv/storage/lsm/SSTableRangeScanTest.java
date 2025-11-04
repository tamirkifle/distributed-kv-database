package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableRangeScanTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** Writes key0000..key{n-1} (one put each, sequence = i) with the given block size. */
    private static Path writeTable(Path path, int n, int blockSize) throws IOException {
        try (SSTableWriter w = new SSTableWriter(path, n, blockSize)) {
            for (int i = 0; i < n; i++) {
                w.add(Entry.put(String.format("key%04d", i), b("value-" + i), i));
            }
            w.finish();
        }
        return path;
    }

    private static List<String> keysOf(Iterator<Entry> it) {
        List<String> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().key());
        }
        return keys;
    }

    @Test
    void blocksReadCountsEveryBlockRead(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 500, 64);
        try (SSTable t = SSTable.open(path)) {
            assertTrue(t.blockCount() > 1, "small block size should span multiple blocks");
            assertEquals(0, t.blocksRead(), "no blocks read before any access");
            keysOf(t.iterator());
            assertEquals(t.blockCount(), t.blocksRead(),
                    "a full iterator drain reads every block exactly once");
        }
    }

    @Test
    void rangeScanInclusiveFromExclusiveTo(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 100, 4096);
        try (SSTable t = SSTable.open(path)) {
            List<String> got = keysOf(t.rangeScan("key0010", "key0013"));
            assertEquals(List.of("key0010", "key0011", "key0012"), got,
                    "from is inclusive, to is exclusive");
        }
    }

    @Test
    void rangeScanOpenBoundsEqualsFullIterator(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 200, 128);
        try (SSTable a = SSTable.open(path); SSTable c = SSTable.open(path)) {
            assertEquals(keysOf(a.iterator()), keysOf(c.rangeScan(null, null)));
        }
    }

    @Test
    void rangeScanOpenLowerBound(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 4096);
        try (SSTable t = SSTable.open(path)) {
            List<String> got = keysOf(t.rangeScan(null, "key0003"));
            assertEquals(List.of("key0000", "key0001", "key0002"), got);
        }
    }

    @Test
    void rangeScanOpenUpperBound(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 4096);
        try (SSTable t = SSTable.open(path)) {
            List<String> got = keysOf(t.rangeScan("key0047", null));
            assertEquals(List.of("key0047", "key0048", "key0049"), got);
        }
    }

    @Test
    void rangeScanEmptyWhenFromGreaterEqualTo(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 4096);
        try (SSTable t = SSTable.open(path)) {
            assertFalse(t.rangeScan("key0030", "key0030").hasNext(), "from == to is empty");
            assertFalse(t.rangeScan("key0030", "key0020").hasNext(), "from > to is empty");
        }
    }

    @Test
    void rangeScanEntirelyBelowTableIsEmpty(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 4096);
        try (SSTable t = SSTable.open(path)) {
            assertFalse(t.rangeScan("aaaa", "bbbb").hasNext());
        }
    }

    @Test
    void rangeScanEntirelyAboveTableIsEmpty(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 4096);
        try (SSTable t = SSTable.open(path)) {
            assertFalse(t.rangeScan("zzz0", "zzz9").hasNext());
        }
    }

    @Test
    void rangeScanSpansMultipleBlocks(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 500, 64);
        try (SSTable t = SSTable.open(path)) {
            assertTrue(t.blockCount() > 4, "need several blocks for a multi-block span");
            List<String> expected = new ArrayList<>();
            for (int i = 100; i < 300; i++) {
                expected.add(String.format("key%04d", i));
            }
            assertEquals(expected, keysOf(t.rangeScan("key0100", "key0300")));
        }
    }

    @Test
    void rangeScanSkipsBlocksOutsideRange(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 500, 64);
        try (SSTable t = SSTable.open(path)) {
            int total = t.blockCount();
            assertTrue(total > 8, "need many blocks to make skipping observable");
            // Narrow range over the middle of the table.
            keysOf(t.rangeScan("key0250", "key0260"));
            assertTrue(t.blocksRead() < total,
                    "narrow range must skip blocks: read " + t.blocksRead() + " of " + total);
            // A range below the whole table reads at most one covering block (block 0).
            try (SSTable t2 = SSTable.open(path)) {
                keysOf(t2.rangeScan("aaaa", "bbbb"));
                assertTrue(t2.blocksRead() <= 1,
                        "range below the table reads at most one block, got " + t2.blocksRead());
            }
        }
    }

    @Test
    void rangeScanFullRangeReadsEveryBlock(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 500, 64);
        try (SSTable t = SSTable.open(path)) {
            keysOf(t.rangeScan(null, null));
            assertEquals(t.blockCount(), t.blocksRead(), "full range reads every block once");
        }
    }

    @Test
    void rangeScanYieldsTombstonesInRange(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("t.sst");
        try (SSTableWriter w = new SSTableWriter(path, 4, 4096)) {
            w.add(Entry.put("key0000", b("v0"), 0));
            w.add(Entry.tombstone("key0001", 1));
            w.add(Entry.put("key0002", b("v2"), 2));
            w.add(Entry.tombstone("key0003", 3));
            w.finish();
        }
        try (SSTable t = SSTable.open(path)) {
            List<Entry> got = new ArrayList<>();
            Iterator<Entry> it = t.rangeScan("key0001", "key0003");
            while (it.hasNext()) {
                got.add(it.next());
            }
            assertEquals(2, got.size());
            assertEquals("key0001", got.get(0).key());
            assertTrue(got.get(0).isTombstone(), "tombstone must pass through rangeScan");
            assertEquals("key0002", got.get(1).key());
            assertFalse(got.get(1).isTombstone());
        }
    }
}
