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

    // Durable persistence sink (nullable: in-memory mode for 3a tests).
    private final RaftPersistence persistence;

    public RaftNode(String nodeId, List<String> peerIds, StateMachine stateMachine,
            IntSupplier electionTimeoutSource) {
        this(nodeId, peerIds, stateMachine, electionTimeoutSource, null,
                new RaftState(0, null, java.util.Collections.emptyList()));
    }

    public RaftNode(String nodeId, List<String> peerIds, StateMachine stateMachine,
            IntSupplier electionTimeoutSource, RaftPersistence persistence, RaftState recovered) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.peerIds = new ArrayList<>(Objects.requireNonNull(peerIds, "peerIds"));
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.electionTimeoutSource = Objects.requireNonNull(electionTimeoutSource, "electionTimeoutSource");
        this.electionTimeout = electionTimeoutSource.getAsInt();
        this.persistence = persistence;
        this.currentTerm = recovered.currentTerm();
        this.votedFor = recovered.votedFor();
        this.log.replace(recovered.entries());
    }

    /** Closes the durable persistence handle if any (no-op in in-memory mode). */
    public void closePersistence() throws java.io.IOException {
        if (persistence != null) {
            persistence.close();
        }
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
            persistVote();
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

        persistAppend(req.prevLogIndex(), req.entries());

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
        persistTerm();
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
        persistTerm();
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
                advanceCommitIndex();
            } else {
                nextIndex.put(peer.nodeId(), Math.max(1, resp.conflictIndex()));
            }
        } catch (RuntimeException unreachable) {
            // dropped AppendEntries: retry on the next heartbeat.
        }
    }

    public long lastApplied() {
        return lastApplied;
    }

    public synchronized boolean propose(byte[] command) {
        if (role != RaftRole.LEADER) {
            return false;
        }
        long index = log.lastIndex() + 1;
        log.append(LogEntry.of(currentTerm, index, command));
        persistEntry(log.entryAt(index));
        matchIndex.put(nodeId, index); // leader trivially has it
        sendHeartbeats();
        advanceCommitIndex();
        return true;
    }

    /**
     * Recompute commitIndex from the majority matchIndex, honoring the Raft §5.4.2 rule: a leader
     * only commits entries from its current term via match-count (older-term entries commit only
     * indirectly, once a current-term entry above them commits).
     */
    private void advanceCommitIndex() {
        int clusterSize = peerIds.size() + 1;
        for (long candidate = log.lastIndex(); candidate > commitIndex; candidate--) {
            if (log.termAt(candidate) != currentTerm) {
                continue; // never commit a prior-term entry by match-count alone
            }
            int replicas = 1; // leader itself
            for (String peerId : peerIds) {
                if (matchIndex.getOrDefault(peerId, 0L) >= candidate) {
                    replicas++;
                }
            }
            if (replicas > clusterSize / 2) {
                commitIndex = candidate;
                applyCommitted();
                break;
            }
        }
    }

    /**
     * Merge a leader's entries while persisting them durably first: a divergent suffix writes a
     * TRUNCATE record before the in-memory truncation, then every entry in the resulting suffix is
     * recorded. Re-recording identical entries is idempotent on replay (overwrite at the same index),
     * which keeps this simple and crash-correct without per-entry diffing (YAGNI).
     */
    private void persistAppend(long prevLogIndex, List<LogEntry> incoming) {
        if (persistence != null) {
            long index = prevLogIndex + 1;
            for (LogEntry entry : incoming) {
                if (log.hasEntryAt(index) && log.termAt(index) != entry.term()) {
                    try {
                        persistence.recordTruncate(index);
                    } catch (java.io.IOException e) {
                        throw new RuntimeException("raft persistence failed", e);
                    }
                }
                index++;
            }
        }
        log.appendAll(prevLogIndex, incoming);
        if (persistence != null) {
            for (long i = prevLogIndex + 1; i <= log.lastIndex(); i++) {
                try {
                    persistence.recordEntry(log.entryAt(i));
                } catch (java.io.IOException e) {
                    throw new RuntimeException("raft persistence failed", e);
                }
            }
        }
    }

    private void persistTerm() {
        if (persistence == null) {
            return;
        }
        try {
            persistence.recordTerm(currentTerm, votedFor);
        } catch (java.io.IOException e) {
            throw new RuntimeException("raft persistence failed", e);
        }
    }

    private void persistVote() {
        if (persistence == null) {
            return;
        }
        try {
            persistence.recordVote(votedFor);
        } catch (java.io.IOException e) {
            throw new RuntimeException("raft persistence failed", e);
        }
    }

    private void persistEntry(LogEntry entry) {
        if (persistence == null) {
            return;
        }
        try {
            persistence.recordEntry(entry);
        } catch (java.io.IOException e) {
            throw new RuntimeException("raft persistence failed", e);
        }
    }
}
