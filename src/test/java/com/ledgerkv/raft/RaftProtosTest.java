package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class RaftProtosTest {

    @Test
    void requestVoteRoundTrips() {
        RequestVoteRequest req = RequestVoteRequest.of(7, "n2", 4, 6);
        RequestVoteRequest back = RaftProtos.fromProto(RaftProtos.toProto(req));
        assertEquals(7, back.term());
        assertEquals("n2", back.candidateId());
        assertEquals(4, back.lastLogIndex());
        assertEquals(6, back.lastLogTerm());

        RequestVoteResponse resp = RequestVoteResponse.of(7, true);
        RequestVoteResponse rback = RaftProtos.fromProto(RaftProtos.toProto(resp));
        assertEquals(7, rback.term());
        assertTrue(rback.voteGranted());
    }

    @Test
    void appendEntriesRoundTrips() {
        AppendEntriesRequest req = AppendEntriesRequest.of(3, "n0", 2, 3,
                Arrays.asList(LogEntry.of(3, 3, "cmd".getBytes())), 2);
        AppendEntriesRequest back = RaftProtos.fromProto(RaftProtos.toProto(req));
        assertEquals(3, back.term());
        assertEquals("n0", back.leaderId());
        assertEquals(2, back.prevLogIndex());
        assertEquals(3, back.prevLogTerm());
        assertEquals(2, back.leaderCommit());
        assertEquals(1, back.entries().size());
        assertEquals(3, back.entries().get(0).index());
        assertArrayEquals("cmd".getBytes(), back.entries().get(0).command());

        AppendEntriesResponse ok = AppendEntriesResponse.success(3, 3);
        AppendEntriesResponse okBack = RaftProtos.fromProto(RaftProtos.toProto(ok));
        assertTrue(okBack.success());
        assertEquals(3, okBack.matchIndex());

        AppendEntriesResponse fail = AppendEntriesResponse.failure(9, 2);
        AppendEntriesResponse failBack = RaftProtos.fromProto(RaftProtos.toProto(fail));
        assertFalse(failBack.success());
        assertEquals(9, failBack.term());
        assertEquals(2, failBack.conflictIndex());
    }

    @Test
    void emptyEntriesHeartbeatRoundTrips() {
        AppendEntriesRequest hb = AppendEntriesRequest.of(1, "n0", 0, 0, Collections.emptyList(), 0);
        AppendEntriesRequest back = RaftProtos.fromProto(RaftProtos.toProto(hb));
        assertTrue(back.entries().isEmpty());
    }
}
