package com.ledgerkv.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AmplificationHarnessTest {

    @Test
    void runProducesSaneMeasurement(@TempDir Path dir) throws Exception {
        WorkloadConfig cfg = new WorkloadConfig(
                Workload.A, 200, 200, 64, 10, Distribution.ZIPFIAN, 42L);
        // Small flush threshold so several SSTables form and the compactor has work to do.
        AmplificationHarness harness =
                new AmplificationHarness(4L * 1024, 64L * 1024 * 1024, 5L,
                        java.util.concurrent.TimeUnit.SECONDS.toNanos(30));

        Measurement m = harness.run(dir, "size-tiered", new SizeTieredCompaction(2), cfg);

        assertTrue(m.userLogicalBytes() > 0, "should account user bytes");
        assertTrue(m.sstableBytesWritten() > 0, "should write SSTable bytes");
        assertTrue(m.writeAmplification() > 0.0, "WA must be positive");
        assertTrue(m.spaceAmplification() > 0.0, "SA must be positive");
        assertTrue(m.readAmplification() >= 0.0, "RA must be non-negative");
        assertTrue(m.liveTableCount() >= 1, "at least one live table after flush");
    }
}
