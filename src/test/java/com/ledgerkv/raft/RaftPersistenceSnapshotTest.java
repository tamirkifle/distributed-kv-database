package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftPersistenceSnapshotTest {

    @Test
    void snapshotPlusTailReplaysWithBaseAndPostSnapshotEntries(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            p.recordTerm(2, "n0");
            for (long i = 1; i <= 5; i++) {
                p.recordEntry(LogEntry.of(1, i, ("c" + i).getBytes(UTF_8)));
            }
            // Snapshot through index 3, then keep entries 4,5 and add 6,7 after.
            p.recordSnapshot(Snapshot.of(3, 1, "state-at-3".getBytes(UTF_8)));
            p.recordEntry(LogEntry.of(2, 6, "c6".getBytes(UTF_8)));
            p.recordEntry(LogEntry.of(2, 7, "c7".getBytes(UTF_8)));
        }

        RaftState s = RaftPersistence.replay(dir);
        assertEquals(2, s.currentTerm());
        assertEquals("n0", s.votedFor());
        assertNotNull(s.snapshot());
        assertEquals(3, s.snapshot().lastIncludedIndex());
        assertEquals(1, s.snapshot().lastIncludedTerm());
        assertArrayEquals("state-at-3".getBytes(UTF_8), s.snapshot().data());

        // Entries 1..3 are covered by the snapshot and dropped; 4..7 survive as the tail.
        List<LogEntry> entries = s.entries();
        assertEquals(4, entries.size());
        assertEquals(4, entries.get(0).index());
        assertEquals(7, entries.get(3).index());
    }

    @Test
    void laterSnapshotSupersedesEarlierOne(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            for (long i = 1; i <= 4; i++) {
                p.recordEntry(LogEntry.of(1, i, ("c" + i).getBytes(UTF_8)));
            }
            p.recordSnapshot(Snapshot.of(2, 1, "at-2".getBytes(UTF_8)));
            p.recordSnapshot(Snapshot.of(4, 1, "at-4".getBytes(UTF_8)));
        }
        RaftState s = RaftPersistence.replay(dir);
        assertEquals(4, s.snapshot().lastIncludedIndex());
        assertArrayEquals("at-4".getBytes(UTF_8), s.snapshot().data());
        assertTrue(s.entries().isEmpty(), "all entries <= base 4 dropped");
    }

    @Test
    void noSnapshotYieldsNullSnapshotAndAllEntries(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            p.recordEntry(LogEntry.of(1, 1, "c1".getBytes(UTF_8)));
        }
        RaftState s = RaftPersistence.replay(dir);
        assertNull(s.snapshot());
        assertEquals(1, s.entries().size());
    }

    @Test
    void replayDropsSnapshotCoveredPrefixKeepingTail(@TempDir Path dir) throws Exception {
        try (RaftPersistence p = RaftPersistence.open(dir)) {
            p.recordTerm(1, null);
            for (long i = 1; i <= 1000; i++) {
                p.recordEntry(LogEntry.of(1, i, ("cmd" + i).getBytes(UTF_8)));
            }
            // Snapshot through index 990 (term 1). Entries 991..1000 must survive.
            p.recordSnapshot(Snapshot.of(990, 1, "state".getBytes(UTF_8)));
        }

        RaftState s = RaftPersistence.replay(dir);
        assertEquals(990, s.snapshot().lastIncludedIndex());
        assertEquals(10, s.entries().size());
        assertEquals(991, s.entries().get(0).index());
        assertEquals(1000, s.entries().get(s.entries().size() - 1).index());
    }
}
