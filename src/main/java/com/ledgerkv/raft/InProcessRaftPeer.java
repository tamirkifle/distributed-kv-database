package com.ledgerkv.raft;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * An in-process {@link RaftPeer} that routes RPCs synchronously to a target {@link RaftNode} in the
 * same JVM — the deterministic test/in-process transport (the gRPC peer arrives in sub-plan 3b).
 * An optional {@code reachable} predicate models a network partition: when it returns false, every
 * RPC throws, exactly as a dropped message would surface to the caller.
 */
public final class InProcessRaftPeer implements RaftPeer {

    private final RaftNode target;
    private final BooleanSupplier reachable;

    public InProcessRaftPeer(RaftNode target) {
        this(target, () -> true);
    }

    public InProcessRaftPeer(RaftNode target, BooleanSupplier reachable) {
        this.target = Objects.requireNonNull(target, "target");
        this.reachable = Objects.requireNonNull(reachable, "reachable");
    }

    @Override
    public String nodeId() {
        return target.nodeId();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        ensureReachable();
        return target.handleRequestVote(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        ensureReachable();
        return target.handleAppendEntries(request);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        ensureReachable();
        return target.handleInstallSnapshot(request);
    }

    private void ensureReachable() {
        if (!reachable.getAsBoolean()) {
            throw new RuntimeException("peer " + target.nodeId() + " unreachable (partitioned)");
        }
    }
}
