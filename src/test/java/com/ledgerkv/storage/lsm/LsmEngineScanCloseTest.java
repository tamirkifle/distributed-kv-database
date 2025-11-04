package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ledgerkv.storage.CloseableIterator;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineScanCloseTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static LsmEngineConfig noCompaction() {
        return new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
    }

    @Test
    void scanReturnsCloseableIteratorThatCanBeAbandonedEarly(@TempDir Path dir) throws Exception {
        try (LsmEngine engine = LsmEngine.open(dir, noCompaction())) {
            for (int i = 0; i < 10; i++) {
                engine.put(String.format("k%02d", i), b("v"));
            }
            engine.flush();   // ensure the data lives in a pinned SSTable, not just the MemTable
            int seen = 0;
            try (CloseableIterator<Entry> it = engine.scan("k00", null)) {
                while (it.hasNext() && seen < 3) {   // abandon early, like WorkloadRunner
                    it.next();
                    seen++;
                }
            }
            assertEquals(3, seen);
        }
    }

    @Test
    void fullScanStillReturnsEveryLiveEntryInOrder(@TempDir Path dir) throws Exception {
        try (LsmEngine engine = LsmEngine.open(dir, noCompaction())) {
            engine.put("b", b("2"));
            engine.put("a", b("1"));
            engine.put("c", b("3"));
            engine.flush();
            StringBuilder sb = new StringBuilder();
            try (CloseableIterator<Entry> it = engine.scan(null, null)) {
                while (it.hasNext()) {
                    sb.append(it.next().key());
                }
            }
            assertEquals("abc", sb.toString());
        }
    }
}
