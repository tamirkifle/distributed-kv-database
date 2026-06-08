package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.raft.kv.KvCommand;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftSnapshotCrashRecoveryTest {

    @Test
    void recoversStateMachineFromSnapshotPlusWalTail(@TempDir Path dir) throws Exception {
        int pre = 10;
        int post = 5;

        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        Process proc = new ProcessBuilder(
                javaBin, "-cp", classpath,
                "com.ledgerkv.raft.RaftSnapshotCrashHarness",
                dir.toString(), String.valueOf(pre), String.valueOf(post))
                .inheritIO()
                .start();

        assertTrue(proc.waitFor(30, TimeUnit.SECONDS), "harness JVM did not exit in time");
        assertEquals(0, proc.exitValue(), "harness should exit via halt(0)");

        RaftState recovered = RaftPersistence.replay(dir);
        assertNotNull(recovered.snapshot(), "a snapshot must have been persisted");
        assertTrue(recovered.snapshot().lastIncludedIndex() >= pre,
                "snapshot covers at least the pre batch");

        // Rebuild the node from snapshot + WAL tail and verify EVERY key is present.
        RaftKvStateMachine sm = new RaftKvStateMachine();
        RaftNode node = new RaftNode("n0", Collections.emptyList(), sm, () -> 1, null, recovered);
        node.tick(); // self-elect
        assertTrue(node.isLeader());
        // Drive the leader to commit+apply the recovered tail (an old-term entry only commits once
        // a current-term entry above it commits, per §5.4.2 — propose a no-op to carry it).
        long noopSeq = pre + post + 1;
        while (node.lastApplied() < node.log().lastIndex()) {
            // A valid KvCommand the fresh state machine can decode and apply (carries the
            // recovered prior-term tail to commit). Distinct key so it doesn't shadow recovered ones.
            node.propose(KvCommand.put("recovery", noopSeq++, "__noop", new byte[0]).encode());
            node.tick();
        }

        for (int i = 0; i < pre + post; i++) {
            byte[] v = sm.get(RaftSnapshotCrashHarness.key(i));
            assertNotNull(v, "missing key " + RaftSnapshotCrashHarness.key(i));
            assertEquals(RaftSnapshotCrashHarness.value(i), new String(v, StandardCharsets.UTF_8),
                    "wrong value for key " + RaftSnapshotCrashHarness.key(i));
        }
    }
}
