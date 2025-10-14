package com.ledgerkv.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalCrashRecoveryTest {

    @Test
    void recoversAllSyncedRecordsAfterHardCrash(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("wal.log");
        int count = 200;

        String javaBin = System.getProperty("java.home")
                + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        Process proc = new ProcessBuilder(
                javaBin, "-cp", classpath,
                "com.ledgerkv.storage.wal.WalCrashHarness",
                path.toString(), String.valueOf(count))
                .inheritIO()
                .start();

        assertTrue(proc.waitFor(30, TimeUnit.SECONDS), "harness JVM did not exit in time");
        assertEquals(0, proc.exitValue(), "harness should exit via halt(0)");

        List<String> keys = new ArrayList<>();
        WriteAheadLog.replay(path, r -> keys.add(r.key()));

        assertEquals(count, keys.size(), "every fsync'd record must survive the crash");
        for (int i = 0; i < count; i++) {
            assertEquals("k" + i, keys.get(i));
        }
    }
}
