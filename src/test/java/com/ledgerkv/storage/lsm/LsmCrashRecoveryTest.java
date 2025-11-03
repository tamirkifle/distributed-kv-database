package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmCrashRecoveryTest {

    @Test
    void recoversAllSyncedRecordsAfterHardCrash(@TempDir Path dir) throws Exception {
        int count = 500;

        String javaBin = System.getProperty("java.home")
                + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        Process proc = new ProcessBuilder(
                javaBin, "-cp", classpath,
                "com.ledgerkv.storage.lsm.LsmCrashHarness",
                dir.toString(), String.valueOf(count))
                .inheritIO()
                .start();

        assertTrue(proc.waitFor(30, TimeUnit.SECONDS), "harness JVM did not exit in time");
        assertEquals(0, proc.exitValue(), "harness should exit via halt(0)");

        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.SYNC, 256L, null, 1L << 30, 50L);
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            for (int i = 0; i < count; i++) {
                Optional<byte[]> v = engine.get(LsmCrashHarness.key(i));
                assertTrue(v.isPresent(), "missing key " + i + " after crash recovery");
                assertArrayEquals(("v" + i).getBytes(UTF_8), v.get(), "wrong value for key " + i);
            }

            // A full scan returns every record in ascending key order.
            Iterator<Entry> it = engine.scan(null, null);
            int seen = 0;
            String prev = null;
            while (it.hasNext()) {
                Entry e = it.next();
                if (prev != null) {
                    assertTrue(prev.compareTo(e.key()) < 0, "scan must be strictly ascending");
                }
                prev = e.key();
                seen++;
            }
            assertEquals(count, seen, "scan must return every recovered record exactly once");
        }
    }
}
