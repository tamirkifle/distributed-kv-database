package com.ledgerkv.bench.jmh;

import com.ledgerkv.bench.WorkloadGenerator;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.storage.lsm.LsmEngineConfig;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Single-shot recovery (open) time over a pre-populated directory; compaction disabled to isolate WAL replay + SSTable load. */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class RecoveryBenchmark {

    @Param({"50000"})
    int recordCount;

    private Path dir;

    private static LsmEngineConfig noCompaction() {
        return new LsmEngineConfig(DurabilityMode.SYNC, 4L * 1024 * 1024, null,
                64L * 1024 * 1024, 50L);
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("ledgerkv-recovery");
        try (LsmEngine engine = LsmEngine.open(dir, noCompaction())) {
            byte[] value = new byte[100];
            for (int i = 0; i < recordCount; i++) {
                engine.put(WorkloadGenerator.key(i), value);
            }
        }
    }

    @Benchmark
    public void openAndClose() throws IOException {
        LsmEngine engine = LsmEngine.open(dir, noCompaction());
        engine.close();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        EngineBenchmarks.deleteRecursively(dir);
    }
}
