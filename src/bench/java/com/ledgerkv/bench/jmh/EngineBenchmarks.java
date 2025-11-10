package com.ledgerkv.bench.jmh;

import com.ledgerkv.bench.WorkloadGenerator;
import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.storage.lsm.LsmEngineConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
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
import org.openjdk.jmh.infra.Blackhole;

/** Point-operation micro-benchmarks against a populated {@link LsmEngine}. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class EngineBenchmarks {

    @Param({"100000"})
    int recordCount;

    @Param({"100"})
    int valueSize;

    private Path dir;
    private LsmEngine engine;
    private byte[] value;
    private Random rng;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("ledgerkv-bench");
        engine = LsmEngine.open(dir, LsmEngineConfig.defaults());
        value = new byte[valueSize];
        rng = new Random(42);
        for (int i = 0; i < recordCount; i++) {
            engine.put(WorkloadGenerator.key(i), value);
        }
    }

    @Benchmark
    public Optional<byte[]> getHit() {
        return engine.get(WorkloadGenerator.key(rng.nextInt(recordCount)));
    }

    @Benchmark
    public Optional<byte[]> getMiss() {
        return engine.get("missing" + rng.nextInt());
    }

    @Benchmark
    public void put() {
        engine.put(WorkloadGenerator.key(rng.nextInt(recordCount)), value);
    }

    @Benchmark
    public int scan(Blackhole bh) {
        Iterator<Entry> it = engine.scan(WorkloadGenerator.key(rng.nextInt(recordCount)), null);
        int n = 0;
        while (it.hasNext() && n < 100) {
            bh.consume(it.next());
            n++;
        }
        return n;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        engine.close();
        deleteRecursively(dir);
    }

    /** Recursively deletes a directory tree; shared by the other benchmark classes. */
    static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort cleanup of a temp dir
                    }
                });
    }
}
