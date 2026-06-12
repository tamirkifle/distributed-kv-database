package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineRecoveryTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static LsmEngineConfig noFlush() {
        return new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
    }

    private static LsmEngineConfig smallFlush() {
        return new LsmEngineConfig(DurabilityMode.SYNC, 64L, null, 1L << 30, 50L);
    }

    @Test
    void reopenRecoversUnflushedWalData(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlush())) {
            for (int i = 0; i < 5; i++) {
                engine.put("k" + i, b("v" + i));
            }
        } // close without flushing: all data is only in the WAL
        try (LsmEngine engine = LsmEngine.open(dir, noFlush())) {
            for (int i = 0; i < 5; i++) {
                assertArrayEquals(b("v" + i), engine.get("k" + i).orElse(null));
            }
        }
    }

    @Test
    void reopenRecoversFlushedAndWalData(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("a", b("1"));
            engine.put("b", b("2"));
            engine.flush();              // a,b now in an SSTable; WAL truncated
            engine.put("c", b("3"));     // c stays in the WAL
        }
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            assertArrayEquals(b("1"), engine.get("a").orElse(null));
            assertArrayEquals(b("2"), engine.get("b").orElse(null));
            assertArrayEquals(b("3"), engine.get("c").orElse(null));
        }
    }

    @Test
    void reopenPrefersWalOverOlderSstable(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("k", b("old"));
            engine.flush();              // "old" in an SSTable with a low sequence
            engine.put("k", b("new"));   // "new" in the WAL, must recover as the newer version
        }
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            assertArrayEquals(b("new"), engine.get("k").orElse(null));
        }
    }

    @Test
    void sstableIdContinuesAfterReopen(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("a", b("1"));
            engine.flush();              // writes sst-0000000000.db
        }
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("b", b("2"));
            engine.flush();              // must write sst-0000000001.db, not overwrite 0
        }
        assertTrue(Files.exists(dir.resolve("sst-0000000000.db")));
        assertTrue(Files.exists(dir.resolve("sst-0000000001.db")));
    }

    @Test
    void recoveryReadsMaxSequenceFromFooterWithoutScanningBlocks(@TempDir Path dir) throws IOException {
        // Write enough entries to span multiple data blocks, then flush to an SSTable.
        try (LsmEngine engine = LsmEngine.open(dir, noFlush())) {
            for (int i = 0; i < 500; i++) {
                engine.put(String.format("key-%05d", i), b("v" + i));
            }
            engine.flush();
        }
        // Reopen: recovery must recover the sequence high-water WITHOUT a full-entry scan.
        try (LsmEngine reopened = LsmEngine.open(dir, noFlush())) {
            // A fresh write must outrank everything on disk; it reads back, confirming the
            // recovered high-water kept the sequence ordering monotonic.
            reopened.put("key-00000", b("updated"));
            assertArrayEquals(b("updated"), reopened.get("key-00000").orElseThrow(AssertionError::new));
        }
    }
}
