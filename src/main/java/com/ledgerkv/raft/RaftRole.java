package com.ledgerkv.raft;

/** The three Raft roles a node can occupy. */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
