package com.ledgerkv.metrics;

import java.util.Objects;

/**
 * A point-in-time view of one Raft member, for the {@code /metrics} endpoint.
 *
 * <p>Commit and applied index are both here on purpose. Commit index says what consensus has
 * agreed; applied index says what this member has actually executed. A member whose applied index
 * trails its commit index is behind on work, not behind on replication, and only the pair tells
 * those apart during an incident.
 */
public final class RaftStatus {

    private final String role;
    private final String leaderId;
    private final long term;
    private final long commitIndex;
    private final long appliedIndex;
    private final long lastIncludedIndex;

    public RaftStatus(String role, String leaderId, long term, long commitIndex,
            long appliedIndex, long lastIncludedIndex) {
        this.role = Objects.requireNonNull(role, "role");
        this.leaderId = leaderId == null ? "" : leaderId;
        this.term = term;
        this.commitIndex = commitIndex;
        this.appliedIndex = appliedIndex;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public String role() {
        return role;
    }

    /** The leader this member recognizes, or empty during an election. */
    public String leaderId() {
        return leaderId;
    }

    public long term() {
        return term;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long appliedIndex() {
        return appliedIndex;
    }

    /** The log base: everything at or below it lives in a snapshot rather than the log. */
    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }
}
