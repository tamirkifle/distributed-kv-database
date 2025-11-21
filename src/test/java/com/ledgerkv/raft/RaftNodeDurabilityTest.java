package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftNodeDurabilityTest {

    private static final class NoopSm implements StateMachine {
        @Override public byte[] apply(byte[] command) { return command; }
    }

    private static IntSupplier fixed(int t) { return () -> t; }

    private static RaftNode durable(String id, List<String> peers, Path dir) throws Exception {
        RaftState recovered = RaftPersistence.replay(dir);
        RaftPersistence p = RaftPersistence.open(dir);
        return new RaftNode(id, peers, new NoopSm(), fixed(2), p, recovered);
    }

    @Test
    void termVoteAndLogSurviveRestart(@TempDir Path dir) throws Exception {
        List<String> peers = Arrays.asList("n1", "n2");
        RaftNode node = durable("n0", peers, dir);

        // Become candidate (term 1, self-vote) then accept a follower-style append at that term.
        node.tick(); node.tick(); // election timeout fires -> startElection persists term+vote
        long termAfterElection = node.currentTerm();
        assertTrue(termAfterElection >= 1);

        // Append an entry as leader if elected; otherwise drive a follower append to exercise log persist.
        if (node.isLeader()) {
            node.propose("durable-cmd".getBytes());
        } else {
            node.handleAppendEntries(AppendEntriesRequest.of(termAfterElection, "n1", 0, 0,
                    Collections.singletonList(LogEntry.of(termAfterElection, 1, "durable-cmd".getBytes())), 0));
        }
        long lastIndex = node.log().lastIndex();
        String votedFor = node.votedFor();
        assertEquals(1, lastIndex);

        // Simulate a restart: close the first node's persistence handle, then reopen over the same dir.
        node.closePersistence();
        RaftNode reopened = durable("n0", peers, dir);
        try {
            assertEquals(termAfterElection, reopened.currentTerm());
            assertEquals(votedFor, reopened.votedFor());
            assertEquals(lastIndex, reopened.log().lastIndex());
            assertArrayEquals("durable-cmd".getBytes(), reopened.log().entryAt(1).command());
        } finally {
            reopened.closePersistence();
        }
    }

    @Test
    void inMemoryConstructorStillWorks() {
        RaftNode node = new RaftNode("n0", Arrays.asList("n1", "n2"), new NoopSm(), fixed(2));
        node.tick(); node.tick();
        assertTrue(node.currentTerm() >= 1);
    }
}
