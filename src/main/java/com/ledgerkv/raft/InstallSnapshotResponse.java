package com.ledgerkv.raft;

/** InstallSnapshot RPC result (Raft paper §7): the follower's current term (for leader step-down). */
public final class InstallSnapshotResponse {

    private final long term;

    private InstallSnapshotResponse(long term) {
        this.term = term;
    }

    public static InstallSnapshotResponse of(long term) {
        return new InstallSnapshotResponse(term);
    }

    public long term() {
        return term;
    }
}
