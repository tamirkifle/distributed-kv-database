package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static Path writeTable(Path path, int n, int blockSize) throws IOException {
        try (SSTableWriter w = new SSTableWriter(path, n, blockSize)) {
            for (int i = 0; i < n; i++) {
                w.add(Entry.put(String.format("key%04d", i), b("value-" + i), i));
            }
            w.finish();
        }
        return path;
    }

    @Test
    void getReturnsWrittenValuesAcrossManyBlocks(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 500, 128);
        try (SSTable t = SSTable.open(path)) {
            assertEquals(500, t.entryCount());
            assertTrue(t.blockCount() > 1, "small block size should span multiple blocks");
            for (int i = 0; i < 500; i++) {
                Optional<Entry> got = t.get(String.format("key%04d", i));
                assertTrue(got.isPresent(), "missing key " + i);
                assertArrayEquals(b("value-" + i), got.get().value());
                assertEquals(i, got.get().sequence());
            }
        }
    }

    @Test
    void getMissReturnsEmpty(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 100, 4096);
        try (SSTable t = SSTable.open(path)) {
            assertFalse(t.get("nonexistent").isPresent());
            assertFalse(t.get("key9999").isPresent());
        }
    }

    @Test
    void bloomReportsEveryWrittenKeyPresent(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 200, 256);
        try (SSTable t = SSTable.open(path)) {
            for (int i = 0; i < 200; i++) {
                assertTrue(t.get(String.format("key%04d", i)).isPresent());
            }
        }
    }

    @Test
    void tombstoneIsReadBackAsTombstone(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("tomb.sst");
        try (SSTableWriter w = new SSTableWriter(path, 3)) {
            w.add(Entry.put("a", b("1"), 1));
            w.add(Entry.tombstone("b", 2));
            w.add(Entry.put("c", b("3"), 3));
            w.finish();
        }
        try (SSTable t = SSTable.open(path)) {
            assertTrue(t.get("b").isPresent());
            assertTrue(t.get("b").get().isTombstone());
            assertFalse(t.get("a").get().isTombstone());
        }
    }

    @Test
    void iteratorYieldsAllEntriesInKeyOrder(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 300, 128);
        try (SSTable t = SSTable.open(path)) {
            List<String> keys = new ArrayList<>();
            Iterator<Entry> it = t.iterator();
            while (it.hasNext()) {
                keys.add(it.next().key());
            }
            assertEquals(300, keys.size());
            for (int i = 1; i < keys.size(); i++) {
                assertTrue(keys.get(i - 1).compareTo(keys.get(i)) < 0, "iteration not sorted at " + i);
            }
            assertEquals("key0000", keys.get(0));
            assertEquals("key0299", keys.get(299));
        }
    }

    @Test
    void dataBlockCorruptionIsDetected(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 50, 64);
        // Flip a byte at the start of the first data block (offset 0), breaking its CRC.
        try (FileChannel ch = FileChannel.open(path, READ, WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, 0);
            byte v = one.get(0);
            ch.write(ByteBuffer.wrap(new byte[] {(byte) (v ^ 0xFF)}), 0);
        }
        try (SSTable t = SSTable.open(path)) {
            assertThrows(SSTableCorruptionException.class, () -> t.get("key0000"));
        }
    }

    @Test
    void openRejectsBadMagic(@TempDir Path dir) throws IOException {
        Path path = writeTable(dir.resolve("t.sst"), 10, 4096);
        long size = Files.size(path);
        // The magic int sits 8 bytes before EOF (footer = [..][magic:int][crc:int]).
        try (FileChannel ch = FileChannel.open(path, READ, WRITE)) {
            ByteBuffer bad = ByteBuffer.allocate(4).putInt(0xDEADBEEF);
            bad.flip();
            ch.write(bad, size - 8);
        }
        assertThrows(SSTableCorruptionException.class, () -> SSTable.open(path));
    }

    @Test
    void addingKeysOutOfOrderThrows(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("bad.sst");
        try (SSTableWriter w = new SSTableWriter(path, 10)) {
            w.add(Entry.put("b", b("1"), 1));
            assertThrows(IllegalArgumentException.class, () -> w.add(Entry.put("a", b("2"), 2)));
        }
    }

    @Test
    void footerPersistsMaxSequence(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("sst-maxseq.db");
        try (SSTableWriter w = new SSTableWriter(path, 3)) {
            w.add(Entry.put("a", b("1"), 5L));
            w.add(Entry.put("b", b("2"), 12L));
            w.add(Entry.put("c", b("3"), 9L)); // out-of-order seq, in-order key
            w.finish();
        }
        try (SSTable t = SSTable.open(path)) {
            assertEquals(12L, t.maxSequence());
        }
    }

    @Test
    void emptyTableHasZeroMaxSequence(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("sst-empty.db");
        try (SSTableWriter w = new SSTableWriter(path, 1)) {
            w.finish();
        }
        try (SSTable t = SSTable.open(path)) {
            assertEquals(0L, t.maxSequence());
        }
    }
}
