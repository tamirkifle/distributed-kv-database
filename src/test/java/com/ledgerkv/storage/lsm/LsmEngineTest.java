package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.StorageEngine;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    /** No auto-flush (huge threshold) and no background compaction — fully deterministic. */
    private static LsmEngineConfig noFlushNoCompaction() {
        return new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
    }

    @Test
    void putThenGetReturnsValue(@TempDir Path dir) throws IOException {
        try (StorageEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            engine.put("alpha", b("one"));
            assertArrayEquals(b("one"), engine.get("alpha").orElse(null));
        }
    }

    @Test
    void getMissingReturnsEmpty(@TempDir Path dir) throws IOException {
        try (StorageEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            assertEquals(Optional.empty(), engine.get("absent"));
        }
    }

    @Test
    void overwriteReturnsLatest(@TempDir Path dir) throws IOException {
        try (StorageEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            engine.put("k", b("v1"));
            engine.put("k", b("v2"));
            assertArrayEquals(b("v2"), engine.get("k").orElse(null));
        }
    }

    @Test
    void deleteThenGetReturnsEmpty(@TempDir Path dir) throws IOException {
        try (StorageEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            engine.put("k", b("v"));
            engine.delete("k");
            assertEquals(Optional.empty(), engine.get("k"));
        }
    }

    /**
     * Flush threshold above the tiny per-test data (each entry is ~51 bytes given MemTable's
     * 48-byte per-entry overhead), so only the explicit {@code flush()} triggers a flush — keeping
     * the SSTable count deterministic. No background compaction.
     */
    private static LsmEngineConfig smallFlushNoCompaction() {
        return new LsmEngineConfig(DurabilityMode.SYNC, 64L * 1024, null, 1L << 30, 50L);
    }

    @Test
    void flushWritesSstableAndKeepsKeysReadable(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlushNoCompaction())) {
            engine.put("a", b("1"));
            engine.put("b", b("2"));
            engine.put("c", b("3"));
            engine.flush();
            assertEquals(1, engine.sstableCount(), "one SSTable after a flush");
            assertArrayEquals(b("1"), engine.get("a").orElse(null));
            assertArrayEquals(b("2"), engine.get("b").orElse(null));
            assertArrayEquals(b("3"), engine.get("c").orElse(null));
        }
    }

    @Test
    void activeMemtableWinsOverOlderSstable(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlushNoCompaction())) {
            engine.put("k", b("old"));
            engine.flush();                       // "old" now lives in an SSTable
            engine.put("k", b("new"));            // higher sequence in the active MemTable
            assertArrayEquals(b("new"), engine.get("k").orElse(null));
        }
    }

    @Test
    void tombstoneInMemtableShadowsSstable(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlushNoCompaction())) {
            engine.put("k", b("v"));
            engine.flush();
            engine.delete("k");                   // tombstone with the highest sequence
            assertEquals(Optional.empty(), engine.get("k"));
        }
    }

    @Test
    void walTruncatedAfterFlush(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlushNoCompaction())) {
            engine.put("a", b("1"));
            engine.put("b", b("2"));
            engine.flush();
            assertEquals(0L, java.nio.file.Files.size(dir.resolve("wal.log")),
                    "WAL is truncated once the MemTable is durable in an SSTable");
        }
    }
}
