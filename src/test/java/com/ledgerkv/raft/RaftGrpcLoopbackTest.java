package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftGrpcLoopbackTest {

    private static final class NoopSm implements StateMachine {
        @Override public byte[] apply(byte[] command) { return command; }
    }

    private RaftNode node;
    private RaftServer server;
    private RaftClient client;
    private GrpcRaftPeer peer;

    @BeforeEach
    void setUp() throws Exception {
        node = new RaftNode("n1", Arrays.asList("n0"), new NoopSm(), () -> 5);
        server = RaftServer.create(0, node).start();
        client = RaftClient.connect("localhost", server.port());
        peer = new GrpcRaftPeer("n1", client);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void requestVoteRoundTripsOverGrpc() {
        RequestVoteResponse resp = peer.requestVote(RequestVoteRequest.of(1, "n0", 0, 0));
        assertEquals(1, resp.term());
        assertTrue(resp.voteGranted());
        assertEquals("n1", peer.nodeId());
    }

    @Test
    void appendEntriesRoundTripsOverGrpc() {
        AppendEntriesResponse resp = peer.appendEntries(AppendEntriesRequest.of(1, "n0", 0, 0,
                Collections.singletonList(LogEntry.of(1, 1, "x".getBytes())), 0));
        assertTrue(resp.success());
        assertEquals(1, resp.matchIndex());
        assertEquals(1, node.log().lastIndex());
        assertArrayEquals("x".getBytes(), node.log().entryAt(1).command());
    }

    @Test
    void rpcToClosedServerThrowsRuntimeException() throws Exception {
        server.close();
        assertThrows(RuntimeException.class,
                () -> peer.requestVote(RequestVoteRequest.of(2, "n0", 0, 0)));
    }
}
