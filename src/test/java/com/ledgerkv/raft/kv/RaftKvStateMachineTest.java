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
        sm.apply(KvCommand.put("other", 1, "k", "v2".getBytes()).encode());
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
    void staleSequenceIsRefused() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v1".getBytes()).encode());
        sm.apply(KvCommand.put("c", 5, "k", "v5".getBytes()).encode());

        sm.apply(KvCommand.put("c", 4, "k", "v4".getBytes()).encode());

        assertArrayEquals("v5".getBytes(), sm.get("k"));
        assertEquals(5, sm.lastAppliedSequence("c"));
        assertEquals(KvOutcome.Status.STALE_SEQUENCE,
                sm.outcomeFor("c", 4, KvCommand.put("c", 4, "k", "v4".getBytes()).fingerprint())
                        .status());
    }

    @Test
    void reusingASequenceForADifferentCommandIsRefusedRatherThanDeduped() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "first".getBytes()).encode());

        KvCommand impostor = KvCommand.put("c", 1, "k", "second".getBytes());
        sm.apply(impostor.encode());

        assertArrayEquals("first".getBytes(), sm.get("k"), "the impostor must not take effect");
        assertEquals(KvOutcome.Status.SEQUENCE_CONFLICT,
                sm.outcomeFor("c", 1, impostor.fingerprint()).status(),
                "absorbing this as a duplicate would drop a write with no trace");
    }

    @Test
    void anUnseenClientOpensASessionAtWhateverSequenceItPresents() {
        RaftKvStateMachine sm = new RaftKvStateMachine();

        sm.apply(KvCommand.put("ghost", 7, "k", "v".getBytes()).encode());

        assertArrayEquals("v".getBytes(), sm.get("k"));
        assertEquals(7, sm.lastAppliedSequence("ghost"));
    }

    @Test
    void aClientWhoseFirstWriteWasLostCanStillMakeProgress() {
        // Requiring a new session to open at sequence 1 bricked exactly this client: its opening
        // write never committed, so it moved on to sequence 2, which had no session to attach to,
        // and every write it made from then on was refused without a word.
        RaftKvStateMachine sm = new RaftKvStateMachine();

        // sequence 1 is lost in flight and never applied; the client gives up and moves on
        sm.apply(KvCommand.put("c", 2, "k", "v2".getBytes()).encode());
        sm.apply(KvCommand.put("c", 3, "k", "v3".getBytes()).encode());

        assertArrayEquals("v3".getBytes(), sm.get("k"));
        assertEquals(3, sm.lastAppliedSequence("c"));
    }

    @Test
    void sessionsAreCappedAndEvictedLeastRecentlyApplied() {
        RaftKvStateMachine sm = new RaftKvStateMachine(2);
        sm.apply(KvCommand.put("a", 1, "ka", "1".getBytes()).encode());
        sm.apply(KvCommand.put("b", 1, "kb", "1".getBytes()).encode());
        sm.apply(KvCommand.put("c", 1, "kc", "1".getBytes()).encode());

        assertEquals(2, sm.sessionCount());
        assertEquals(0, sm.lastAppliedSequence("a"), "the oldest session is the one dropped");
        assertEquals(1, sm.lastAppliedSequence("b"));
        assertEquals(1, sm.lastAppliedSequence("c"));
        // Evicting the session must not touch the data it wrote.
        assertArrayEquals("1".getBytes(), sm.get("ka"));
    }

    @Test
    void readsDoNotDisturbEvictionOrder() {
        // Reads run on the leader only. If they moved a session's recency, the leader would evict
        // a different session than its followers and the replicas would diverge.
        RaftKvStateMachine sm = new RaftKvStateMachine(2);
        sm.apply(KvCommand.put("a", 1, "ka", "1".getBytes()).encode());
        sm.apply(KvCommand.put("b", 1, "kb", "1".getBytes()).encode());

        sm.get("ka");
        sm.lastAppliedSequence("a");
        sm.outcomeFor("a", 1, 0L);

        sm.apply(KvCommand.put("c", 1, "kc", "1".getBytes()).encode());
        assertEquals(0, sm.lastAppliedSequence("a"), "'a' is still the least recently applied");
        assertEquals(1, sm.lastAppliedSequence("b"));
    }

    @Test
    void resultForReturnsPreviousValueOnDelete() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        sm.apply(KvCommand.put("c", 1, "k", "v".getBytes()).encode());
        sm.apply(KvCommand.delete("c", 2, "k").encode());
        assertArrayEquals("v".getBytes(), sm.resultFor("c", 2));
    }
}
