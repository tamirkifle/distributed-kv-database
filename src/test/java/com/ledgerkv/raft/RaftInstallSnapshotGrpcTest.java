package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RaftInstallSnapshotGrpcTest {

    private static final class EchoSm implements StateMachine {
        byte[] state = new byte[0];
        @Override public byte[] apply(byte[] command) { return command; }
        @Override public byte[] snapshot() { return state; }
        @Override public void restore(byte[] data, long idx, long term) { state = data.clone(); }
    }

    @Test
    void installSnapshotRoundTripsOverGrpc() throws Exception {
        EchoSm sm = new EchoSm();
        RaftNode node = new RaftNode("n1", java.util.Arrays.asList("n0"), sm, () -> 50);
        try (RaftServer server = RaftServer.create(0, node).start();
             RaftClient client = RaftClient.connect("localhost", server.port())) {
            GrpcRaftPeer peer = new GrpcRaftPeer("n1", client);
            InstallSnapshotResponse resp = peer.installSnapshot(
                    InstallSnapshotRequest.of(3, "n0", 7, 2, "snapshot-bytes".getBytes(UTF_8)));
            assertEquals(3, resp.term());
            assertEquals(7, node.lastIncludedIndex());
            assertEquals(7, node.lastApplied());
            assertArrayEquals("snapshot-bytes".getBytes(UTF_8), sm.state);
        }
    }
}
