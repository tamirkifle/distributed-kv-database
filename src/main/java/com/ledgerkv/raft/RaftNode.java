package com.ledgerkv.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntSupplier;

/**
 * A single Raft consensus node: owns the role, term, vote, replicated log, and commit progress, and
 * implements the canonical Raft rules for leader election and log replication (Raft paper §5).
 * Timing is driven by a logical clock — {@link #tick()} advances election/heartbeat counters and
 * randomized election timeouts come from an injectable {@link IntSupplier} — so tests are fully
 * deterministic with no wall-clock dependency. In-process only in sub-plan 3a (WAL durability is 3b).
 *
 * <p>Not thread-safe across threads in the sense of fairness, but inbound handlers are synchronized;
 * a node is driven by a single test/event thread and the in-process peer routes RPCs synchronously.
 */
public final class RaftNode {

    private final String nodeId;
    private final List<String> peerIds;
    private final StateMachine stateMachine;
    private final IntSupplier electionTimeoutSource;

    // Persistent state (WAL-backed in 3b).
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog log = new RaftLog();

    // Volatile state.
    private RaftRole role = RaftRole.FOLLOWER;
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Election/heartbeat clock (logical).
    private int electionElapsed = 0;
    private int electionTimeout;

    // Candidate state.
    private final Set<String> votesReceived = new HashSet<>();

    // Leader state.
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, RaftPeer> peers = new HashMap<>();

    public RaftNode(String nodeId, List<String> peerIds, StateMachine stateMachine,
            IntSupplier electionTimeoutSource) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.peerIds = new ArrayList<>(Objects.requireNonNull(peerIds, "peerIds"));
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.electionTimeoutSource = Objects.requireNonNull(electionTimeoutSource, "electionTimeoutSource");
        this.electionTimeout = electionTimeoutSource.getAsInt();
    }

    public String nodeId() {
        return nodeId;
    }

    public RaftRole role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public String votedFor() {
        return votedFor;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public RaftLog log() {
        return log;
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        boolean grant = false;
        if (req.term() >= currentTerm
                && (votedFor == null || votedFor.equals(req.candidateId()))
                && candidateLogIsUpToDate(req.lastLogIndex(), req.lastLogTerm())) {
            grant = true;
            votedFor = req.candidateId();
            resetElectionTimer();
        }
        return RequestVoteResponse.of(currentTerm, grant);
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term() < currentTerm) {
            return AppendEntriesResponse.failure(currentTerm, 0);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        // Recognize the leader for this term.
        role = RaftRole.FOLLOWER;
        resetElectionTimer();

        if (!log.matches(req.prevLogIndex(), req.prevLogTerm())) {
            long conflict = Math.min(req.prevLogIndex(), log.lastIndex() + 1);
            return AppendEntriesResponse.failure(currentTerm, conflict);
        }

        log.appendAll(req.prevLogIndex(), req.entries());

        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), log.lastIndex());
            applyCommitted();
        }
        return AppendEntriesResponse.success(currentTerm, log.lastIndex());
    }

    private boolean candidateLogIsUpToDate(long lastLogIndex, long lastLogTerm) {
        long localTerm = log.lastTerm();
        long localIndex = log.lastIndex();
        if (lastLogTerm != localTerm) {
            return lastLogTerm > localTerm;
        }
        return lastLogIndex >= localIndex;
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        votesReceived.clear();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            stateMachine.apply(log.entryAt(lastApplied).command());
        }
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeout = electionTimeoutSource.getAsInt();
    }

    private static final int HEARTBEAT_INTERVAL = 1;
    private int heartbeatElapsed = 0;

    public void registerPeer(RaftPeer peer) {
        peers.put(peer.nodeId(), peer);
    }

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public synchronized void tick() {
        if (role == RaftRole.LEADER) {
            heartbeatElapsed++;
            if (heartbeatElapsed >= HEARTBEAT_INTERVAL) {
                heartbeatElapsed = 0;
                sendHeartbeats();
            }
            return;
        }
        electionElapsed++;
        if (electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        votesReceived.clear();
        votesReceived.add(nodeId);
        resetElectionTimer();

        RequestVoteRequest req =
                RequestVoteRequest.of(currentTerm, nodeId, log.lastIndex(), log.lastTerm());
        for (RaftPeer peer : peers.values()) {
            try {
                RequestVoteResponse resp = peer.requestVote(req);
                if (resp.term() > currentTerm) {
                    stepDown(resp.term());
                    return;
                }
                if (resp.term() == currentTerm && resp.voteGranted()) {
                    votesReceived.add(peer.nodeId());
                }
            } catch (RuntimeException unreachable) {
                // dropped RequestVote: no vote from this peer.
            }
        }
        if (role == RaftRole.CANDIDATE && hasMajority(votesReceived.size())) {
            becomeLeader();
        }
    }

    private boolean hasMajority(int votes) {
        int clusterSize = peerIds.size() + 1;
        return votes > clusterSize / 2;
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        heartbeatElapsed = HEARTBEAT_INTERVAL; // send a heartbeat promptly
        nextIndex.clear();
        matchIndex.clear();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, log.lastIndex() + 1);
            matchIndex.put(peerId, 0L);
        }
        sendHeartbeats();
    }

    private void sendHeartbeats() {
        for (RaftPeer peer : peers.values()) {
            replicateTo(peer);
        }
    }

    private void replicateTo(RaftPeer peer) {
        long ni = nextIndex.getOrDefault(peer.nodeId(), log.lastIndex() + 1);
        long prevLogIndex = ni - 1;
        long prevLogTerm = log.termAt(prevLogIndex);
        List<LogEntry> entries = log.from(ni);
        AppendEntriesRequest req = AppendEntriesRequest.of(
                currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, commitIndex);
        try {
            AppendEntriesResponse resp = peer.appendEntries(req);
            if (resp.term() > currentTerm) {
                stepDown(resp.term());
                return;
            }
            if (resp.success()) {
                matchIndex.put(peer.nodeId(), resp.matchIndex());
                nextIndex.put(peer.nodeId(), resp.matchIndex() + 1);
            } else {
                nextIndex.put(peer.nodeId(), Math.max(1, resp.conflictIndex()));
            }
        } catch (RuntimeException unreachable) {
            // dropped AppendEntries: retry on the next heartbeat.
        }
    }
}
