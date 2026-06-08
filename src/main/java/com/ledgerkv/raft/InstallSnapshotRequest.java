package com.ledgerkv.raft;

import java.util.Objects;

/**
 * InstallSnapshot RPC arguments (Raft paper §7). Sent by the leader to a follower whose required
 * next index precedes the leader's snapshot base, so the follower cannot be caught up by
 * AppendEntries. Single-chunk transfer (the snapshots here are small in-memory KV state — the
 * offset/done chunking in the paper is unneeded; YAGNI).
 */
public final class InstallSnapshotRequest {

    private final long term;
    private final String leaderId;
    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final byte[] data;

    private InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex,
            long lastIncludedTerm, byte[] data) {
        this.term = term;
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = Objects.requireNonNull(data, "data").clone();
    }

    public static InstallSnapshotRequest of(long term, String leaderId, long lastIncludedIndex,
            long lastIncludedTerm, byte[] data) {
        return new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
    }

    public long term() {
        return term;
    }

    public String leaderId() {
        return leaderId;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long lastIncludedTerm() {
        return lastIncludedTerm;
    }

    public byte[] data() {
        return data.clone();
    }
}
