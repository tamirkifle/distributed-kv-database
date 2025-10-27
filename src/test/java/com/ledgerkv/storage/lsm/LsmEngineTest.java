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
}
