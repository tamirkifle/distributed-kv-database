package com.ledgerkv.raft;

/**
 * AppendEntries RPC result (Raft paper §5.3). On success, matchIndex is the highest index now
 * known-replicated on the follower. On failure, conflictIndex hints where the leader should retry
 * from (fast backtracking — the index the follower wants the leader to back up to).
 */
public final class AppendEntriesResponse {

    private final long term;
    private final boolean success;
    private final long matchIndex;
    private final long conflictIndex;

    private AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.conflictIndex = conflictIndex;
    }

    public static AppendEntriesResponse success(long term, long matchIndex) {
        return new AppendEntriesResponse(term, true, matchIndex, 0);
    }

    public static AppendEntriesResponse failure(long term, long conflictIndex) {
        return new AppendEntriesResponse(term, false, 0, conflictIndex);
    }

    public long term() {
        return term;
    }

    public boolean success() {
        return success;
    }

    public long matchIndex() {
        return matchIndex;
    }

    public long conflictIndex() {
        return conflictIndex;
    }
}
