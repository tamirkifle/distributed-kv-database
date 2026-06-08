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

    // Log-compaction trigger (injectable): compact once this many physical applied entries pile up.
    private int compactionThreshold = Integer.MAX_VALUE; // never auto-compacts by default

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
        Snapshot recoveredSnapshot = recovered.snapshot();
        if (recoveredSnapshot != null) {
            // Set the log base from the snapshot, load the recovered tail above it, restore the
            // state machine, and mark everything through the base committed+applied.
            this.log.resetToSnapshot(recoveredSnapshot.lastIncludedIndex(),
                    recoveredSnapshot.lastIncludedTerm());
            this.log.replace(recovered.entries());
            this.stateMachine.restore(recoveredSnapshot.data(),
                    recoveredSnapshot.lastIncludedIndex(), recoveredSnapshot.lastIncludedTerm());
            this.commitIndex = recoveredSnapshot.lastIncludedIndex();
            this.lastApplied = recoveredSnapshot.lastIncludedIndex();
        } else {
            this.log.replace(recovered.entries());
        }
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

    /**
     * Follower side of InstallSnapshot (Raft §7): a leader ships a snapshot when the entries this
     * follower needs have been compacted away. Reject a stale term; otherwise discard the local log
     * (replaced by the snapshot), restore the state machine, advance commit/applied to the base, and
     * persist the snapshot. Idempotent: a snapshot we already cover is ignored.
     */
    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest req) {
        if (req.term() < currentTerm) {
            return InstallSnapshotResponse.of(currentTerm);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        role = RaftRole.FOLLOWER;
        resetElectionTimer();

        if (req.lastIncludedIndex() <= log.lastIncludedIndex()) {
            return InstallSnapshotResponse.of(currentTerm); // already covered
        }

        log.resetToSnapshot(req.lastIncludedIndex(), req.lastIncludedTerm());
        stateMachine.restore(req.data(), req.lastIncludedIndex(), req.lastIncludedTerm());
        commitIndex = req.lastIncludedIndex();
        lastApplied = req.lastIncludedIndex();
        if (persistence != null) {
            try {
                persistence.recordSnapshot(
                        Snapshot.of(req.lastIncludedIndex(), req.lastIncludedTerm(), req.data()));
            } catch (java.io.IOException e) {
                throw new RuntimeException("raft persistence failed", e);
            }
        }
        return InstallSnapshotResponse.of(currentTerm);
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
        if (ni <= log.lastIncludedIndex()) {
            // The entries this follower needs are compacted away — ship the snapshot instead.
            sendSnapshot(peer);
            return;
        }
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

    private void sendSnapshot(RaftPeer peer) {
        long base = log.lastIncludedIndex();
        InstallSnapshotRequest req = InstallSnapshotRequest.of(
                currentTerm, nodeId, base, log.lastIncludedTerm(), stateMachine.snapshot());
        try {
            InstallSnapshotResponse resp = peer.installSnapshot(req);
            if (resp.term() > currentTerm) {
                stepDown(resp.term());
                return;
            }
            matchIndex.put(peer.nodeId(), base);
            nextIndex.put(peer.nodeId(), base + 1);
            advanceCommitIndex();
        } catch (RuntimeException unreachable) {
            // dropped InstallSnapshot: retry on the next heartbeat.
        }
    }

    public long lastApplied() {
        return lastApplied;
    }

    /** Injectable compaction trigger: compact once this many physical applied entries accumulate. */
    void setCompactionThreshold(int threshold) {
        this.compactionThreshold = threshold;
    }

    public long lastIncludedIndex() {
        return log.lastIncludedIndex();
    }

    /**
     * Snapshot + compact if the applied, still-physical log prefix has grown past the threshold.
     * Snapshots through {@code lastApplied} (never an unapplied suffix), persists the snapshot,
     * then truncates the in-memory log prefix. Idempotent and safe to call after every apply.
     */
    public synchronized void maybeCompact() {
        long base = log.lastIncludedIndex();
        long appliedPhysical = lastApplied - base;
        if (appliedPhysical < compactionThreshold || lastApplied <= base) {
            return;
        }
        long term = log.termAt(lastApplied);
        Snapshot snapshot = Snapshot.of(lastApplied, term, stateMachine.snapshot());
        if (persistence != null) {
            try {
                persistence.recordSnapshot(snapshot);
            } catch (java.io.IOException e) {
                throw new RuntimeException("raft persistence failed", e);
            }
        }
        log.compactThrough(lastApplied, term);
    }

    /**
     * Append a command to the leader's log and replicate it. Returns the assigned 1-based log
     * index (so a caller can wait for {@link #lastApplied()} {@code >= index}), or {@code 0} if
     * this node is not the leader.
     */
    public synchronized long propose(byte[] command) {
        if (role != RaftRole.LEADER) {
            return 0;
        }
        long index = log.lastIndex() + 1;
        log.append(LogEntry.of(currentTerm, index, command));
        persistEntry(log.entryAt(index));
        matchIndex.put(nodeId, index); // leader trivially has it
        sendHeartbeats();
        advanceCommitIndex();
        return index;
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
