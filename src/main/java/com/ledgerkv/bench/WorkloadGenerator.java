package com.ledgerkv.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic YCSB-shaped operation stream: a {@link #load()} phase that inserts
 * every record, and a {@link #run()} phase of {@code operationCount} ops mixed per the configured
 * {@link Workload}, with keys drawn from the configured {@link Distribution}. Both phases are
 * reproducible from {@link WorkloadConfig#seed}.
 */
public final class WorkloadGenerator {

    private final WorkloadConfig config;

    public WorkloadGenerator(WorkloadConfig config) {
        this.config = config;
    }

    /** Sequential INSERT of every record key (indices 0..recordCount-1). */
    public List<Operation> load() {
        List<Operation> ops = new ArrayList<>(config.recordCount);
        for (int i = 0; i < config.recordCount; i++) {
            ops.add(Operation.insert(key(i), value(i)));
        }
        return ops;
    }

    /** {@code operationCount} mixed ops per the workload. Deterministic from the seed. */
    public List<Operation> run() {
        Random rng = new Random(config.seed);
        // Decouple key selection from op-type rolls with a distinct (but deterministic) seed.
        IntGenerator keys = config.distribution.create(config.recordCount, config.seed ^ 0x9E3779B9L);
        List<Operation> ops = new ArrayList<>(config.operationCount);
        int nextInsert = config.recordCount;
        for (int i = 0; i < config.operationCount; i++) {
            OpType type = config.workload.pick(rng.nextDouble());
            switch (type) {
                case READ:
                    ops.add(Operation.read(key(keys.nextInt())));
                    break;
                case UPDATE:
                    ops.add(Operation.update(key(keys.nextInt()), value(i)));
                    break;
                case INSERT:
                    ops.add(Operation.insert(key(nextInsert), value(i)));
                    nextInsert++;
                    break;
                case SCAN:
                    int len = 1 + rng.nextInt(Math.max(1, config.maxScanLength));
                    ops.add(Operation.scan(key(keys.nextInt()), len));
                    break;
                case READ_MODIFY_WRITE:
                    ops.add(Operation.readModifyWrite(key(keys.nextInt()), value(i)));
                    break;
                default:
                    throw new IllegalStateException("unhandled op type: " + type);
            }
        }
        return ops;
    }

    /** Zero-padded key for an index, e.g. {@code user0000000042}. Ordered lexicographically by index. */
    public static String key(int index) {
        return String.format("user%010d", index);
    }

    /** Deterministic value bytes of the configured size; content is irrelevant to the engine. */
    private byte[] value(int salt) {
        byte[] v = new byte[config.valueSize];
        for (int i = 0; i < v.length; i++) {
            v[i] = (byte) ('a' + ((i + salt) % 26));
        }
        return v;
    }
}
