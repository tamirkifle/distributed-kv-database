package com.ledgerkv.raft;

/** RequestVote RPC result (Raft paper §5.2). */
public final class RequestVoteResponse {

    private final long term;
    private final boolean voteGranted;

    private RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public static RequestVoteResponse of(long term, boolean voteGranted) {
        return new RequestVoteResponse(term, voteGranted);
    }

    public long term() {
        return term;
    }

    public boolean voteGranted() {
        return voteGranted;
    }
}
