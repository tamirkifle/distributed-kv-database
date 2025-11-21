package com.ledgerkv.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** AppendEntries RPC arguments (Raft paper §5.3); also the heartbeat when entries is empty. */
public final class AppendEntriesRequest {

    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;

    private AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
            List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = Objects.requireNonNull(leaderId, "leaderId");
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(entries, "entries")));
        this.leaderCommit = leaderCommit;
    }

    public static AppendEntriesRequest of(long term, String leaderId, long prevLogIndex, long prevLogTerm,
            List<LogEntry> entries, long leaderCommit) {
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    public long term() {
        return term;
    }

    public String leaderId() {
        return leaderId;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public long prevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> entries() {
        return entries;
    }

    public long leaderCommit() {
        return leaderCommit;
    }
}
