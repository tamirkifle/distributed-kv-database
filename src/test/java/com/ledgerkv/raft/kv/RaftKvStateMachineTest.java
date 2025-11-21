package com.ledgerkv.raft.kv;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RaftKvStateMachineTest {

    @Test
    void putThenGet() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v".getBytes()).encode());
        assertArrayEquals("v".getBytes(), sm.get("k"));
    }

    @Test
    void deleteRemovesKey() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v".getBytes()).encode());
        sm.apply(KvCommand.delete("c", 2, "k").encode());
        assertNull(sm.get("k"));
    }

    @Test
    void getMissingKeyIsNull() {
        assertNull(new RaftKvStateMachine().get("absent"));
    }

    @Test
    void sameLogYieldsSameState() {
        byte[] c1 = KvCommand.put("c", 1, "a", "1".getBytes()).encode();
        byte[] c2 = KvCommand.put("c", 2, "b", "2".getBytes()).encode();
        byte[] c3 = KvCommand.delete("c", 3, "a").encode();

        RaftKvStateMachine s1 = new RaftKvStateMachine();
        RaftKvStateMachine s2 = new RaftKvStateMachine();
        for (byte[] cmd : new byte[][] {c1, c2, c3}) {
            s1.apply(cmd);
            s2.apply(cmd);
        }
        assertNull(s1.get("a"));
        assertNull(s2.get("a"));
        assertArrayEquals("2".getBytes(), s1.get("b"));
        assertArrayEquals("2".getBytes(), s2.get("b"));
    }

    @Test
    void atMostOnceDoesNotReapplyDuplicate() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v1".getBytes()).encode());
        sm.apply(KvCommand.put("other", 2, "k", "v2".getBytes()).encode());
        // re-deliver client c's seq 1 — must be ignored, not overwrite v2
        sm.apply(KvCommand.put("c", 1, "k", "v1".getBytes()).encode());
        assertArrayEquals("v2".getBytes(), sm.get("k"));
    }

    @Test
    void duplicateApplyReturnsCachedResult() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        byte[] first = sm.apply(KvCommand.put("c", 1, "k", "v1".getBytes()).encode());
        byte[] dup = sm.apply(KvCommand.put("c", 1, "k", "v1".getBytes()).encode());
        assertArrayEquals(first, dup);
        assertEquals(1, sm.lastAppliedSequence("c"));
    }

    @Test
    void staleSequenceIsTreatedAsDuplicate() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 5, "k", "v5".getBytes()).encode());
        // a lower sequence from the same client is stale -> ignored
        sm.apply(KvCommand.put("c", 4, "k", "v4".getBytes()).encode());
        assertArrayEquals("v5".getBytes(), sm.get("k"));
        assertEquals(5, sm.lastAppliedSequence("c"));
    }

    @Test
    void resultForReturnsPreviousValueOnDelete() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v".getBytes()).encode());
        sm.apply(KvCommand.delete("c", 2, "k").encode());
        assertArrayEquals("v".getBytes(), sm.resultFor("c", 2));
    }
}
