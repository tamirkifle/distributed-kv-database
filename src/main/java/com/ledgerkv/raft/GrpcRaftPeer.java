package com.ledgerkv.raft;

import java.util.Objects;

/**
 * A {@link RaftPeer} that reaches a remote node over the {@code LedgerKvRaft} gRPC service via a
 * {@link RaftClient}. A transport failure surfaces as a {@code RuntimeException} (gRPC's
 * {@code StatusRuntimeException}), which {@link RaftNode} treats as a dropped RPC — exactly like
 * {@link InProcessRaftPeer}'s unreachable path.
 */
public final class GrpcRaftPeer implements RaftPeer {

    private final String nodeId;
    private final RaftClient client;

    public GrpcRaftPeer(String nodeId, RaftClient client) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return client.requestVote(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return client.appendEntries(request);
    }
}
