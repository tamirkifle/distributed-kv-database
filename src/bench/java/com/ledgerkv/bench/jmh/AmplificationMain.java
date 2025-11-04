package com.ledgerkv.bench.jmh;

import com.ledgerkv.bench.AmplificationHarness;
import com.ledgerkv.bench.AmplificationReport;
import com.ledgerkv.bench.Distribution;
import com.ledgerkv.bench.Measurement;
import com.ledgerkv.bench.Workload;
import com.ledgerkv.bench.WorkloadConfig;
import com.ledgerkv.storage.compaction.LeveledCompaction;
import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Runs the amplification harness for size-tiered and leveled compaction and prints the markdown report. */
public final class AmplificationMain {

    private AmplificationMain() {
    }

    public static void main(String[] args) throws Exception {
        WorkloadConfig cfg = new WorkloadConfig(
                Workload.A, 50_000, 200_000, 100, 100, Distribution.ZIPFIAN, 42L);
        AmplificationHarness harness = new AmplificationHarness();
        Path base = Files.createTempDirectory("ledgerkv-amplification");
        List<Measurement> results = new ArrayList<>();
        try {
            results.add(harness.run(base.resolve("size-tiered"), "size-tiered",
                    new SizeTieredCompaction(4), cfg));
            results.add(harness.run(base.resolve("leveled"), "leveled",
                    new LeveledCompaction(4, 4L * 1024 * 1024, 10), cfg));
            System.out.println();
            System.out.println("## Compaction amplification (workload A, Zipfian, 50k records, 200k ops)");
            System.out.println();
            System.out.print(AmplificationReport.render(results));
        } finally {
            EngineBenchmarks.deleteRecursively(base);
        }
    }
}
