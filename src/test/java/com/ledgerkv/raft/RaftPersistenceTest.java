package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftPersistenceTest {

    @Test
    void replaysTermVoteAndEntries(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            p.recordTerm(1, null);
            p.recordVote("n0");
            p.recordEntry(LogEntry.of(1, 1, "a".getBytes()));
            p.recordEntry(LogEntry.of(1, 2, "b".getBytes()));
            p.recordTerm(2, null); // new term clears the vote
        }
        RaftState s = RaftPersistence.replay(dir);
        assertEquals(2, s.currentTerm());
        assertNull(s.votedFor());
        assertEquals(2, s.entries().size());
        assertEquals(1, s.entries().get(0).index());
        assertArrayEquals("b".getBytes(), s.entries().get(1).command());
    }

    @Test
    void truncateDiscardsDivergentSuffix(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            p.recordTerm(5, "n1");
            p.recordEntry(LogEntry.of(5, 1, "x".getBytes()));
            p.recordEntry(LogEntry.of(5, 2, "y".getBytes()));
            p.recordTruncate(2);                              // drop index 2+
            p.recordEntry(LogEntry.of(6, 2, "z".getBytes())); // new index 2 from a later term
        }
        RaftState s = RaftPersistence.replay(dir);
        assertEquals(5, s.currentTerm());
        assertEquals("n1", s.votedFor());
        assertEquals(2, s.entries().size());
        assertArrayEquals("x".getBytes(), s.entries().get(0).command());
        assertEquals(6, s.entries().get(1).term());
        assertArrayEquals("z".getBytes(), s.entries().get(1).command());
    }

    @Test
    void emptyDirReplaysToInitialState(@TempDir Path dir) throws Exception {
        RaftState s = RaftPersistence.replay(dir);
        assertEquals(0, s.currentTerm());
        assertNull(s.votedFor());
        assertTrue(s.entries().isEmpty());
    }
}
