package com.ledgerkv.bench.jmh;

import com.ledgerkv.bench.Distribution;
import com.ledgerkv.bench.Operation;
import com.ledgerkv.bench.Workload;
import com.ledgerkv.bench.WorkloadConfig;
import com.ledgerkv.bench.WorkloadGenerator;
import com.ledgerkv.bench.WorkloadRunner;
import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.storage.lsm.LsmEngineConfig;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

/** YCSB workloads A–F under Zipfian and uniform key distributions; throughput per single op. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class YcsbBenchmarks {

    @Param({"A", "B", "C", "D", "E", "F"})
    String workload;

    @Param({"ZIPFIAN", "UNIFORM"})
    String distribution;

    private Path dir;
    private LsmEngine engine;
    private List<Operation> runOps;
    private int idx;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("ledgerkv-ycsb");
        WorkloadConfig cfg = new WorkloadConfig(
                Workload.valueOf(workload), 50_000, 50_000, 100, 100,
                Distribution.valueOf(distribution), 42L);
        LsmEngineConfig engineConfig = new LsmEngineConfig(
                DurabilityMode.ASYNC, 4L * 1024 * 1024, new SizeTieredCompaction(4),
                64L * 1024 * 1024, 50L);
        engine = LsmEngine.open(dir, engineConfig);
        WorkloadGenerator gen = new WorkloadGenerator(cfg);
        for (Operation op : gen.load()) {
            WorkloadRunner.execute(engine, op);
        }
        runOps = gen.run();
    }

    @Benchmark
    public int operation() {
        Operation op = runOps.get(idx);
        idx = (idx + 1) % runOps.size();
        return WorkloadRunner.execute(engine, op);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        engine.close();
        EngineBenchmarks.deleteRecursively(dir);
    }
}
