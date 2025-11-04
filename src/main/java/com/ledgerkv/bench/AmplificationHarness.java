package com.ledgerkv.bench;

import com.ledgerkv.storage.compaction.CompactionStrategy;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.storage.lsm.LsmEngineConfig;
import com.ledgerkv.storage.lsm.SSTableHandle;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Drives an {@link LsmEngine} under one {@link CompactionStrategy} with a workload, then measures
 * write/read/space amplification. Uses ASYNC durability (durability is not under test here) and a
 * modest flush threshold so multiple SSTables form and the compactor has work to do.
 */
public final class AmplificationHarness {

    private final long flushBytes;
    private final long maxSstableBytes;
    private final long pollMillis;
    private final long quiesceTimeoutNanos;

    public AmplificationHarness() {
        this(256L * 1024, 64L * 1024 * 1024, 10L, TimeUnit.SECONDS.toNanos(60));
    }

    public AmplificationHarness(long flushBytes, long maxSstableBytes, long pollMillis,
                                long quiesceTimeoutNanos) {
        this.flushBytes = flushBytes;
        this.maxSstableBytes = maxSstableBytes;
        this.pollMillis = pollMillis;
        this.quiesceTimeoutNanos = quiesceTimeoutNanos;
    }

    public Measurement run(Path dir, String label, CompactionStrategy strategy, WorkloadConfig cfg)
            throws IOException, InterruptedException {
        Files.createDirectories(dir);
        LsmEngineConfig engineConfig = new LsmEngineConfig(
                DurabilityMode.ASYNC, flushBytes, strategy, maxSstableBytes, pollMillis);

        long userLogicalBytes = 0;
        Map<String, Integer> liveValueLen = new TreeMap<>();   // final live key set -> value length

        try (LsmEngine engine = LsmEngine.open(dir, engineConfig)) {
            WorkloadGenerator gen = new WorkloadGenerator(cfg);
            List<Operation> ops = new ArrayList<>(gen.load());
            ops.addAll(gen.run());
            for (Operation op : ops) {
                userLogicalBytes += accountWrite(op, liveValueLen);
                WorkloadRunner.execute(engine, op);
            }
            engine.flush();      // force the active MemTable to disk before measuring
            quiesce(engine);     // bounded poll until background compaction settles

            long sstWritten = engine.sstableBytesWritten();
            List<SSTableHandle> tables = engine.currentTables();
            long liveOnDisk = 0;
            for (SSTableHandle h : tables) {
                liveOnDisk += h.sizeBytes();
            }
            long logicalLive = 0;
            for (Map.Entry<String, Integer> e : liveValueLen.entrySet()) {
                logicalLive += e.getKey().getBytes(StandardCharsets.UTF_8).length + e.getValue();
            }
            double readAmp = averageOverlap(tables, liveValueLen.keySet());
            return new Measurement(label, userLogicalBytes, sstWritten, logicalLive, liveOnDisk,
                    readAmp, tables.size());
        }
    }

    /** Folds a write op into the live model and returns its logical bytes (0 for reads/scans). */
    private static long accountWrite(Operation op, Map<String, Integer> liveValueLen) {
        switch (op.type()) {
            case INSERT:
            case UPDATE:
            case READ_MODIFY_WRITE:
                liveValueLen.put(op.key(), op.value().length);
                return op.key().getBytes(StandardCharsets.UTF_8).length + op.value().length;
            default:
                return 0;
        }
    }

    /** Bounded poll until the live SSTable count is stable for 5 polls, or the deadline passes. */
    private void quiesce(LsmEngine engine) throws InterruptedException {
        long deadline = System.nanoTime() + quiesceTimeoutNanos;
        int stableCount = engine.currentTables().size();
        int stableRounds = 0;
        while (System.nanoTime() < deadline) {
            Thread.sleep(pollMillis);
            int now = engine.currentTables().size();
            if (now == stableCount) {
                if (++stableRounds >= 5) {
                    return;
                }
            } else {
                stableCount = now;
                stableRounds = 0;
            }
        }
    }

    /** Average count of live SSTables whose key range covers a sampled key — a read-amp proxy. */
    private static double averageOverlap(List<SSTableHandle> tables, Iterable<String> keys) {
        List<String> keyList = new ArrayList<>();
        for (String k : keys) {
            keyList.add(k);
        }
        if (keyList.isEmpty()) {
            return 0.0;
        }
        int stride = keyList.size() > 1000 ? keyList.size() / 1000 : 1;   // sample ~1000 keys
        long totalOverlap = 0;
        long sampled = 0;
        for (int i = 0; i < keyList.size(); i += stride) {
            String key = keyList.get(i);
            int overlap = 0;
            for (SSTableHandle h : tables) {
                if (h.firstKey() != null && h.lastKey() != null
                        && h.firstKey().compareTo(key) <= 0 && key.compareTo(h.lastKey()) <= 0) {
                    overlap++;
                }
            }
            totalOverlap += overlap;
            sampled++;
        }
        return sampled == 0 ? 0.0 : (double) totalOverlap / sampled;
    }
}
