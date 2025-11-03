package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ledgerkv.storage.wal.DurabilityMode;
import java.nio.file.Paths;

/**
 * Child-JVM entry point for the engine crash-recovery test. Opens an {@link LsmEngine} with a tiny
 * flush threshold (so some records flush to SSTables and the rest stay in the WAL), writes
 * {@code count} records in SYNC mode (each fsync'd), then dies hard with no cleanup.
 *
 * <p>Compaction is disabled so no SSTable files are reclaimed mid-run. Usage:
 * {@code java -cp <cp> com.ledgerkv.storage.lsm.LsmCrashHarness <dir> <count>}
 */
public final class LsmCrashHarness {

    static String key(int i) {
        return String.format("k%06d", i);
    }

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int count = Integer.parseInt(args[1]);

        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.SYNC, 256L, null, 1L << 30, 50L);
        LsmEngine engine = LsmEngine.open(Paths.get(dir), cfg);
        for (int i = 0; i < count; i++) {
            engine.put(key(i), ("v" + i).getBytes(UTF_8));
        }
        // SYNC fsync'd every WAL append; flushed SSTables were fsync'd by finish().
        // Die hard: no shutdown hooks, no close(), no final flush.
        Runtime.getRuntime().halt(0);
    }
}
