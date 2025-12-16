package com.ledgerkv.raft;

import java.util.Objects;

/**
 * An immutable Raft snapshot: the state-machine bytes plus the log position they cover
 * {@code (lastIncludedIndex, lastIncludedTerm)} (Raft paper §7). Stored durably by
 * {@link RaftPersistence} and shipped to a lagging follower via the InstallSnapshot RPC.
 */
public final class Snapshot {

    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final byte[] data;

    private Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        if (lastIncludedIndex < 0 || lastIncludedTerm < 0) {
            throw new IllegalArgumentException("snapshot index/term must be >= 0");
        }
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = Objects.requireNonNull(data, "data").clone();
    }

    public static Snapshot of(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        return new Snapshot(lastIncludedIndex, lastIncludedTerm, data);
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
