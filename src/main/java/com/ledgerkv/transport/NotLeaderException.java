package com.ledgerkv.transport;

/**
 * Thrown by a Raft-mode coordinator on a node that is not the leader, so the request was not
 * served. Carries the leader's id and endpoint when this node knows them, which is what lets a
 * client retry in one hop instead of walking the member list.
 *
 * <p>Both fields are null while an election is in flight. That is a real state, not a defect: no
 * node can name a leader between the old one going away and a new one heartbeating.
 */
public final class NotLeaderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String leaderId;
    private final String leaderEndpoint;

    public NotLeaderException(String leaderId, String leaderEndpoint) {
        super(leaderId == null
                ? "no leader is known yet"
                : "not the leader; " + leaderId + " is, at " + leaderEndpoint);
        this.leaderId = leaderId;
        this.leaderEndpoint = leaderEndpoint;
    }

    /** The current leader's node id, or null if unknown. */
    public String leaderId() {
        return leaderId;
    }

    /** The current leader's client endpoint as {@code host:port}, or null if unknown. */
    public String leaderEndpoint() {
        return leaderEndpoint;
    }
}
