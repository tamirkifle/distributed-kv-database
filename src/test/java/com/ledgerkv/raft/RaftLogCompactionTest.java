package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogCompactionTest {

    private static LogEntry e(long term, long index) {
        return LogEntry.of(term, index, ("c" + index).getBytes());
    }

    @Test
    void baseStartsAtZeroAndSentinelUnchanged() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIncludedIndex());
        assertEquals(0, log.lastIncludedTerm());
        assertEquals(0, log.size());
        assertEquals(0, log.termAt(0)); // sentinel
        assertTrue(log.matches(0, 0));
    }

    @Test
    void compactThroughDropsPrefixButKeepsAbsoluteIndices() {
        RaftLog log = new RaftLog();
        for (long i = 1; i <= 5; i++) {
            log.append(e(1, i));
        }
        log.compactThrough(3, 1);

        assertEquals(3, log.lastIncludedIndex());
        assertEquals(1, log.lastIncludedTerm());
        assertEquals(2, log.size());           // physical entries 4,5
        assertEquals(5, log.lastIndex());       // absolute last index preserved
        assertFalse(log.hasEntryAt(3));         // compacted away
        assertTrue(log.hasEntryAt(4));
        assertEquals(4, log.entryAt(4).index());
        assertEquals(1, log.termAt(3));         // base acts as the sentinel
        assertTrue(log.matches(3, 1));          // prevLogIndex == base matches
        assertFalse(log.matches(3, 2));         // wrong base term does not match
    }

    @Test
    void fromClampsToFirstPhysicalEntryAfterBase() {
        RaftLog log = new RaftLog();
        for (long i = 1; i <= 5; i++) {
            log.append(e(1, i));
        }
        log.compactThrough(3, 1);
        List<LogEntry> tail = log.from(1); // asked for everything; base is 3
        assertEquals(2, tail.size());
        assertEquals(4, tail.get(0).index());
        assertEquals(5, tail.get(1).index());
    }

    @Test
    void appendStaysDenseAcrossTheBase() {
        RaftLog log = new RaftLog();
        for (long i = 1; i <= 3; i++) {
            log.append(e(1, i));
        }
        log.compactThrough(3, 1);
        log.append(e(2, 4)); // next dense index after base+physical
        assertEquals(4, log.lastIndex());
        assertEquals(2, log.termAt(4));
    }

    @Test
    void resetToSnapshotDiscardsEverythingAndSetsBase() {
        RaftLog log = new RaftLog();
        for (long i = 1; i <= 3; i++) {
            log.append(e(1, i));
        }
        log.resetToSnapshot(10, 4);
        assertEquals(10, log.lastIncludedIndex());
        assertEquals(4, log.lastIncludedTerm());
        assertEquals(0, log.size());
        assertEquals(10, log.lastIndex());
        assertEquals(4, log.termAt(10));
        assertTrue(log.matches(10, 4));
        log.append(e(5, 11)); // continues densely after the installed base
        assertEquals(11, log.lastIndex());
    }

    @Test
    void compactThroughRejectsBeyondLastIndex() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        assertThrows(IllegalArgumentException.class, () -> log.compactThrough(5, 1));
    }
}
