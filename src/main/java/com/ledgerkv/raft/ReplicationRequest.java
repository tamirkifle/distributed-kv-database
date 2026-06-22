package com.ledgerkv.raft;

import java.util.Objects;

/**
 * One outbound replication message a leader owes a peer: either an AppendEntries or, when the
 * entries that peer needs have been compacted away, an InstallSnapshot.
 *
 * <p>This is the seam that lets peer I/O happen <em>outside</em> the {@link RaftNode} monitor. The
 * node builds the request under its lock, the caller performs the RPC with the lock released, and
 * the response is fed back through {@code applyAppendEntriesResponse} /
 * {@code applyInstallSnapshotResponse}. It is a scoped version of etcd/raft's {@code Ready} +
 * {@code Step} split, covering replication only.
 */
public final class ReplicationRequest {

    private final AppendEntriesRequest entries;
    private final InstallSnapshotRequest snapshot;

    private ReplicationRequest(AppendEntriesRequest entries, InstallSnapshotRequest snapshot) {
        this.entries = entries;
        this.snapshot = snapshot;
    }

    public static ReplicationRequest entries(AppendEntriesRequest request) {
        return new ReplicationRequest(Objects.requireNonNull(request, "request"), null);
    }

    public static ReplicationRequest snapshot(InstallSnapshotRequest request) {
        return new ReplicationRequest(null, Objects.requireNonNull(request, "request"));
    }

    public boolean isSnapshot() {
        return snapshot != null;
    }

    /** The AppendEntries payload; null when {@link #isSnapshot()}. */
    public AppendEntriesRequest entries() {
        return entries;
    }

    /** The InstallSnapshot payload; null unless {@link #isSnapshot()}. */
    public InstallSnapshotRequest snapshot() {
        return snapshot;
    }
}
