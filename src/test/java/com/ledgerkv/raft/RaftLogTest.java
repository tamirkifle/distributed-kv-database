package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class RaftLogTest {

    private static LogEntry e(long term, long index) {
        return LogEntry.of(term, index, new byte[] {(byte) index});
    }

    @Test
    void emptyLogSentinels() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.termAt(0));
        assertFalse(log.hasEntryAt(1));
        assertTrue(log.matches(0, 0), "empty prefix always matches");
    }

    @Test
    void appendsDenselyAndReports() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.append(e(1, 2));
        log.append(e(2, 3));
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
        assertEquals(1, log.termAt(1));
        assertEquals(2, log.termAt(3));
        assertEquals(e(2, 3), log.entryAt(3));
    }

    @Test
    void appendRejectsGap() {
        RaftLog log = new RaftLog();
        assertThrows(IllegalArgumentException.class, () -> log.append(e(1, 2)));
    }

    @Test
    void matchesEnforcesConsistencyCheck() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.append(e(2, 2));
        assertTrue(log.matches(2, 2));
        assertFalse(log.matches(2, 1), "term mismatch");
        assertFalse(log.matches(5, 2), "missing index");
    }

    @Test
    void fromReturnsSuffix() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.append(e(1, 2));
        log.append(e(1, 3));
        assertEquals(Arrays.asList(e(1, 2), e(1, 3)), log.from(2));
        assertEquals(Collections.emptyList(), log.from(4));
    }

    @Test
    void appendAllIsIdempotentForIdenticalEntries() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.append(e(1, 2));
        log.appendAll(0, Arrays.asList(e(1, 1), e(1, 2))); // re-delivery
        assertEquals(2, log.lastIndex());
        assertEquals(e(1, 2), log.entryAt(2));
    }

    @Test
    void appendAllTruncatesDivergentSuffix() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.append(e(1, 2));
        log.append(e(1, 3)); // this suffix diverges from the leader
        // leader says index 2 onward is term 2
        log.appendAll(1, Arrays.asList(e(2, 2), e(2, 3), e(2, 4)));
        assertEquals(4, log.lastIndex());
        assertEquals(1, log.termAt(1));
        assertEquals(2, log.termAt(2));
        assertEquals(2, log.termAt(4));
    }

    @Test
    void appendAllExtendsWhenPrefixMatches() {
        RaftLog log = new RaftLog();
        log.append(e(1, 1));
        log.appendAll(1, Arrays.asList(e(1, 2), e(1, 3)));
        assertEquals(3, log.lastIndex());
    }
}
