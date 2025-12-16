package com.ledgerkv.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable carrier of replayed persistent Raft state (Raft paper §5.1) + the latest snapshot. */
public final class RaftState {

    private final long currentTerm;
    private final String votedFor; // nullable
    private final List<LogEntry> entries;
    private final Snapshot snapshot; // nullable: no snapshot recorded yet

    public RaftState(long currentTerm, String votedFor, List<LogEntry> entries) {
        this(currentTerm, votedFor, entries, null);
    }

    public RaftState(long currentTerm, String votedFor, List<LogEntry> entries, Snapshot snapshot) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.snapshot = snapshot;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public String votedFor() {
        return votedFor;
    }

    public List<LogEntry> entries() {
        return entries;
    }

    /** The latest recovered snapshot, or {@code null} if none was ever recorded. */
    public Snapshot snapshot() {
        return snapshot;
    }
}
