package com.ledgerkv.raft.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RaftKvStateMachineSnapshotTest {

    @Test
    void snapshotRestoreRoundTripsStoreAndDedup() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("clientA", 1, "k1", "v1".getBytes(UTF_8)).encode());
        sm.apply(KvCommand.put("clientA", 2, "k2", "v2".getBytes(UTF_8)).encode());
        sm.apply(KvCommand.put("clientB", 1, "k3", "v3".getBytes(UTF_8)).encode());

        byte[] snap = sm.snapshot();

        RaftKvStateMachine restored = new RaftKvStateMachine();
        restored.restore(snap, 3, 1);

        assertArrayEquals("v1".getBytes(UTF_8), restored.get("k1"));
        assertArrayEquals("v2".getBytes(UTF_8), restored.get("k2"));
        assertArrayEquals("v3".getBytes(UTF_8), restored.get("k3"));
        // dedup survives: replaying clientA seq=2 is a no-op returning the cached result
        assertEquals(2, restored.lastAppliedSequence("clientA"));
        assertEquals(1, restored.lastAppliedSequence("clientB"));
    }

    @Test
    void restoredDedupSuppressesAlreadyAppliedRetry() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "first".getBytes(UTF_8)).encode());
        byte[] snap = sm.snapshot();

        RaftKvStateMachine restored = new RaftKvStateMachine();
        restored.restore(snap, 1, 1);
        // a retry of seq=1 with a DIFFERENT value must NOT overwrite (at-most-once preserved)
        restored.apply(KvCommand.put("c", 1, "k", "second".getBytes(UTF_8)).encode());
        assertArrayEquals("first".getBytes(UTF_8), restored.get("k"));
    }

    @Test
    void snapshotIsDeterministic() {
        RaftKvStateMachine a = new RaftKvStateMachine();
        RaftKvStateMachine b = new RaftKvStateMachine();
        for (int i = 0; i < 5; i++) {
            byte[] cmd = KvCommand.put("c", i + 1, "k" + i, ("v" + i).getBytes(UTF_8)).encode();
            a.apply(cmd);
            b.apply(cmd);
        }
        assertArrayEquals(a.snapshot(), b.snapshot());
    }
}
