package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableHandleTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** Writes an SSTable whose keys are key{from}..key{to-1} inclusive of from, exclusive of to. */
    private static Path writeRange(Path path, int from, int to) throws IOException {
        try (SSTableWriter w = new SSTableWriter(path, to - from)) {
            for (int i = from; i < to; i++) {
                w.add(Entry.put(String.format("key%04d", i), b("v" + i), i));
            }
            w.finish();
        }
        return path;
    }

    @Test
    void exposesKeyRangeAndSize(@TempDir Path dir) throws IOException {
        Path path = writeRange(dir.resolve("a.sst"), 0, 10);
        try (SSTableHandle h = SSTableHandle.open(path, 1)) {
            assertEquals("key0000", h.firstKey());
            assertEquals("key0009", h.lastKey());
            assertEquals(1, h.level());
            assertEquals(path, h.path());
            assertEquals(10, h.table().entryCount());
            assertTrue(h.sizeBytes() > 0);
        }
    }

    @Test
    void overlapDetection(@TempDir Path dir) throws IOException {
        try (SSTableHandle a = SSTableHandle.open(writeRange(dir.resolve("a.sst"), 0, 10), 1);
             SSTableHandle b = SSTableHandle.open(writeRange(dir.resolve("b.sst"), 5, 15), 1);
             SSTableHandle c = SSTableHandle.open(writeRange(dir.resolve("c.sst"), 20, 30), 1)) {
            assertTrue(a.overlaps(b));
            assertTrue(b.overlaps(a));
            assertFalse(a.overlaps(c));
            assertFalse(c.overlaps(a));
        }
    }

    @Test
    void writerReportsGrowingSize(@TempDir Path dir) throws IOException {
        try (SSTableWriter w = new SSTableWriter(dir.resolve("w.sst"), 100, 64)) {
            long start = w.approximateSizeBytes();
            for (int i = 0; i < 100; i++) {
                w.add(Entry.put(String.format("key%04d", i), b("value-" + i), i));
            }
            assertTrue(w.approximateSizeBytes() > start, "size should grow as entries are added");
        }
    }
}
