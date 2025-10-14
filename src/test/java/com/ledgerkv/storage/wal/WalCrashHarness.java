package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.file.Paths;

/**
 * Child-JVM entry point for the crash-recovery test. Appends {@code count} records
 * in SYNC mode (each fsync'd), then dies hard with no cleanup.
 *
 * Usage: java -cp <cp> com.ledgerkv.storage.wal.WalCrashHarness <path> <count>
 */
public final class WalCrashHarness {

    public static void main(String[] args) throws Exception {
        String path = args[0];
        int count = Integer.parseInt(args[1]);

        WriteAheadLog wal = new WriteAheadLog(Paths.get(path), DurabilityMode.SYNC);
        for (int i = 0; i < count; i++) {
            wal.append(WalRecord.put("k" + i, ("v" + i).getBytes(UTF_8)));
        }
        // SYNC already fsync'd every append. Die hard: no shutdown hooks, no close().
        Runtime.getRuntime().halt(0);
    }
}
