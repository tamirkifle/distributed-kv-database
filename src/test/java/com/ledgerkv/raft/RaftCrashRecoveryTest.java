package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftCrashRecoveryTest {

    @Test
    void recoversDurableStateAfterHardCrash(@TempDir Path dir) throws Exception {
        int count = 200;

        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        Process proc = new ProcessBuilder(
                javaBin, "-cp", classpath,
                "com.ledgerkv.raft.RaftCrashHarness",
                dir.toString(), String.valueOf(count))
                .inheritIO()
                .start();

        assertTrue(proc.waitFor(30, TimeUnit.SECONDS), "harness JVM did not exit in time");
        assertEquals(0, proc.exitValue(), "harness should exit via halt(0)");

        RaftState recovered = RaftPersistence.replay(dir);
        assertTrue(recovered.currentTerm() >= 1, "term must survive the crash");
        assertEquals("n0", recovered.votedFor(), "self-vote must survive the crash");

        List<LogEntry> entries = recovered.entries();
        assertEquals(count, entries.size(), "every fsync'd entry must survive");
        for (int i = 0; i < count; i++) {
            assertEquals(i + 1, entries.get(i).index(), "entries must be in dense index order");
            assertArrayEquals(RaftCrashHarness.cmd(i).getBytes(UTF_8), entries.get(i).command(),
                    "wrong command at index " + (i + 1));
        }
    }
}
