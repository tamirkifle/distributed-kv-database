package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftMessagesTest {

    @Test
    void requestVoteCarriesFields() {
        RequestVoteRequest r = RequestVoteRequest.of(4, "n1", 9, 3);
        assertEquals(4, r.term());
        assertEquals("n1", r.candidateId());
        assertEquals(9, r.lastLogIndex());
        assertEquals(3, r.lastLogTerm());

        RequestVoteResponse resp = RequestVoteResponse.of(4, true);
        assertEquals(4, resp.term());
        assertTrue(resp.voteGranted());
    }

    @Test
    void appendEntriesCarriesFieldsAndCopiesEntries() {
        List<LogEntry> entries = new ArrayList<>(Arrays.asList(LogEntry.of(2, 5, new byte[] {1})));
        AppendEntriesRequest r = AppendEntriesRequest.of(2, "leader", 4, 1, entries, 3);
        assertEquals(2, r.term());
        assertEquals("leader", r.leaderId());
        assertEquals(4, r.prevLogIndex());
        assertEquals(1, r.prevLogTerm());
        assertEquals(3, r.leaderCommit());
        assertEquals(1, r.entries().size());

        entries.clear(); // mutate caller list
        assertEquals(1, r.entries().size(), "must defensively copy entries");
    }

    @Test
    void appendEntriesResponseSuccessAndFailure() {
        AppendEntriesResponse ok = AppendEntriesResponse.success(2, 7);
        assertTrue(ok.success());
        assertEquals(2, ok.term());
        assertEquals(7, ok.matchIndex());

        AppendEntriesResponse no = AppendEntriesResponse.failure(5, 3);
        assertFalse(no.success());
        assertEquals(5, no.term());
        assertEquals(3, no.conflictIndex());
    }

    @Test
    void rolesAndStateMachineSeam() {
        assertEquals(3, RaftRole.values().length);
        StateMachine sm = command -> command; // echo
        assertArrayEquals(new byte[] {9}, sm.apply(new byte[] {9}));
    }
}
