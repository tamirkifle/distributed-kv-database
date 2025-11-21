package com.ledgerkv.raft;

/**
 * The outbound-RPC transport seam: how a {@link RaftNode} reaches one peer. Mirrors the Phase 2
 * {@code ReplicaClient} pattern — an in-process implementation routes directly to a peer
 * {@code RaftNode}, and a gRPC implementation (sub-plan 3b) calls the {@code LedgerKvRaft} service.
 *
 * <p>An unreachable peer signals failure by throwing a {@link RuntimeException}; the caller treats
 * that as a failed RPC (no vote / no replication ack), exactly like a dropped network message.
 */
public interface RaftPeer {

    /** The id of the peer node this client targets. */
    String nodeId();

    /** Invoke RequestVote on the peer. */
    RequestVoteResponse requestVote(RequestVoteRequest request);

    /** Invoke AppendEntries on the peer. */
    AppendEntriesResponse appendEntries(AppendEntriesRequest request);
}
