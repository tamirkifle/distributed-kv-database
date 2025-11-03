package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineCompactionTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void backgroundCompactorMergesFlushedTables(@TempDir Path dir) throws Exception {
        // Huge flush threshold (we flush explicitly); size-tiered merges as soon as 2 tables exist.
        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.SYNC, Long.MAX_VALUE, new SizeTieredCompaction(2), 1L << 30, 5L);
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            for (int i = 0; i < 10; i++) {
                engine.put(String.format("k%03d", i), b("v" + i));
            }
            engine.flush();                          // SSTable #0
            for (int i = 10; i < 20; i++) {
                engine.put(String.format("k%03d", i), b("v" + i));
            }
            engine.flush();                          // SSTable #1 → triggers a size-tiered merge

            // Deterministic bounded wait: poll until the merge collapses the two tables into one.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (engine.sstableCount() != 1 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            assertEquals(1, engine.sstableCount(), "compactor should merge the two tables");
            assertNull(engine.compactionError(), "compaction must not error");
            for (int i = 0; i < 20; i++) {
                assertArrayEquals(b("v" + i), engine.get(String.format("k%03d", i)).orElse(null),
                        "key k" + i + " must survive compaction");
            }
        }
    }
}
