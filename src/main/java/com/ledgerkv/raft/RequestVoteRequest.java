package com.ledgerkv.raft;

import java.util.Objects;

/** RequestVote RPC arguments (Raft paper §5.2). */
public final class RequestVoteRequest {

    private final long term;
    private final String candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;

    private RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = Objects.requireNonNull(candidateId, "candidateId");
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public static RequestVoteRequest of(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
    }

    public long term() {
        return term;
    }

    public String candidateId() {
        return candidateId;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public long lastLogTerm() {
        return lastLogTerm;
    }
}
