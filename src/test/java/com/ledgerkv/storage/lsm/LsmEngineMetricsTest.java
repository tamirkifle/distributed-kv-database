package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineMetricsTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void flushPathCountsSstableBytes(@TempDir Path dir) throws IOException {
        LsmEngineConfig cfg =
                new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            assertEquals(0L, engine.sstableBytesWritten(), "no bytes before any flush");
            for (int i = 0; i < 50; i++) {
                engine.put(String.format("k%03d", i), b("value-" + i));
            }
            engine.flush();
            long onDisk = engine.currentTables().get(0).sizeBytes();
            assertEquals(onDisk, engine.sstableBytesWritten(),
                    "counter equals the flushed table's on-disk size");
            assertTrue(engine.sstableBytesWritten() > 0);
        }
    }

    @Test
    void compactionPathAddsToTheCounter(@TempDir Path dir) throws Exception {
        // size-tiered(2): two flushed tables trigger a merge that writes a third (merged) table.
        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.SYNC, Long.MAX_VALUE, new SizeTieredCompaction(2), 1L << 30, 5L);
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            for (int i = 0; i < 20; i++) {
                engine.put(String.format("k%03d", i), b("v" + i));
            }
            engine.flush();
            for (int i = 20; i < 40; i++) {
                engine.put(String.format("k%03d", i), b("v" + i));
            }
            engine.flush();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (engine.currentTables().size() != 1 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, engine.currentTables().size(), "compactor should merge to one table");

            long liveBytes = engine.currentTables().get(0).sizeBytes();
            assertTrue(engine.sstableBytesWritten() > liveBytes,
                    "counter (" + engine.sstableBytesWritten() + ") includes the two inputs plus the "
                            + "merged output, so it exceeds the single live table (" + liveBytes + ")");
        }
    }
}
