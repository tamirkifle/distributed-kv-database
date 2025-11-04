package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;

class RaftNodeRpcTest {

    /** Records applied commands so we can assert exactly-once, in-order apply. */
    private static final class RecordingStateMachine implements StateMachine {
        final List<String> applied = new ArrayList<>();
        @Override
        public byte[] apply(byte[] command) {
            applied.add(new String(command));
            return command;
        }
    }

    private static IntSupplier fixedTimeout(int t) {
        return () -> t;
    }

    private RaftNode follower(StateMachine sm) {
        return new RaftNode("self", Arrays.asList("a", "b"), sm, fixedTimeout(5));
    }

    @Test
    void grantsVoteForUpToDateCandidate() {
        RaftNode node = follower(new RecordingStateMachine());
        RequestVoteResponse resp = node.handleRequestVote(RequestVoteRequest.of(1, "a", 0, 0));
        assertTrue(resp.voteGranted());
        assertEquals(1, node.currentTerm());
        assertEquals("a", node.votedFor());
    }

    @Test
    void doesNotDoubleVoteInSameTerm() {
        RaftNode node = follower(new RecordingStateMachine());
        assertTrue(node.handleRequestVote(RequestVoteRequest.of(1, "a", 0, 0)).voteGranted());
        assertFalse(node.handleRequestVote(RequestVoteRequest.of(1, "b", 0, 0)).voteGranted(),
                "already voted for a this term");
    }

    @Test
    void deniesVoteToLessUpToDateCandidate() {
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode node = follower(sm);
        // give the follower a log: term 2 at index 1
        node.handleAppendEntries(AppendEntriesRequest.of(2, "ldr", 0, 0,
                Collections.singletonList(LogEntry.of(2, 1, "x".getBytes())), 0));
        // candidate's log is shorter / older
        assertFalse(node.handleRequestVote(RequestVoteRequest.of(3, "a", 0, 0)).voteGranted());
    }

    @Test
    void stepsDownOnHigherTerm() {
        RaftNode node = follower(new RecordingStateMachine());
        node.handleRequestVote(RequestVoteRequest.of(1, "a", 0, 0)); // term -> 1
        node.handleAppendEntries(AppendEntriesRequest.of(5, "ldr", 0, 0, Collections.emptyList(), 0));
        assertEquals(5, node.currentTerm());
        assertEquals(RaftRole.FOLLOWER, node.role());
        assertNull(node.votedFor(), "votedFor cleared on term bump");
    }

    @Test
    void rejectsStaleAppendEntries() {
        RaftNode node = follower(new RecordingStateMachine());
        node.handleRequestVote(RequestVoteRequest.of(3, "a", 0, 0)); // term -> 3
        AppendEntriesResponse resp =
                node.handleAppendEntries(AppendEntriesRequest.of(1, "old", 0, 0, Collections.emptyList(), 0));
        assertFalse(resp.success());
        assertEquals(3, resp.term());
    }

    @Test
    void appendEntriesFailsConsistencyCheck() {
        RaftNode node = follower(new RecordingStateMachine());
        // prevLogIndex 5 doesn't exist on an empty log
        AppendEntriesResponse resp = node.handleAppendEntries(
                AppendEntriesRequest.of(1, "ldr", 5, 1, Collections.emptyList(), 0));
        assertFalse(resp.success());
    }

    @Test
    void appendEntriesMergesAndAdvancesCommit() {
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode node = follower(sm);
        List<LogEntry> entries = Arrays.asList(LogEntry.of(1, 1, "c1".getBytes()),
                LogEntry.of(1, 2, "c2".getBytes()));
        AppendEntriesResponse resp =
                node.handleAppendEntries(AppendEntriesRequest.of(1, "ldr", 0, 0, entries, 2));
        assertTrue(resp.success());
        assertEquals(2, resp.matchIndex());
        assertEquals(2, node.commitIndex());
        assertEquals(Arrays.asList("c1", "c2"), sm.applied, "committed entries applied in order");
    }

    @Test
    void commitNeverExceedsLocalLastIndex() {
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode node = follower(sm);
        // leaderCommit 9 but only 1 entry delivered: commit capped at 1
        node.handleAppendEntries(AppendEntriesRequest.of(1, "ldr", 0, 0,
                Collections.singletonList(LogEntry.of(1, 1, "only".getBytes())), 9));
        assertEquals(1, node.commitIndex());
        assertEquals(1, sm.applied.size());
    }
}
