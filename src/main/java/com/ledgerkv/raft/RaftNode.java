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

    /**
     * Who this node believes leads the current term, or null when it does not know. Learned from
     * whichever leader's AppendEntries/InstallSnapshot resets the election timer, and cleared on
     * every step down because the next term's leader is not known yet. Volatile so a client-facing
     * thread can read it for leader routing without taking this node's monitor.
     */
    private volatile String leaderId;

    // Election/heartbeat clock (logical).
    private int electionElapsed = 0;
    private int electionTimeout;

    /**
     * Ticks since this node started, and the tick at which each peer last acknowledged this leader.
     * Used only to answer "can this node still serve?" for the readiness endpoint — never to make a
     * consensus decision — so it stays on the logical clock rather than introducing a wall clock.
     */
    private long ticks = 0;
    private final Map<String, Long> lastAckTick = new HashMap<>();

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

    /**
     * Mirrors {@code lastApplied} for readers that must not acquire this node's monitor. A client
     * waiting for its proposal to be applied blocks on {@code appliedMonitor}; {@code applyCommitted}
     * holds the node monitor and then briefly takes {@code appliedMonitor} to publish. The waiter
     * never takes the node monitor, so the two can never deadlock against each other.
     */
    private final java.util.concurrent.atomic.AtomicLong appliedWatermark =
            new java.util.concurrent.atomic.AtomicLong();
    private final Object appliedMonitor = new Object();

    /**
     * When true, {@link #tick()} no longer performs replication itself — a {@link
     * RaftReplicationDriver} owns per-peer replication on its own threads. Election timeouts still
     * run on the tick.
     */
    private volatile boolean replicationDelegated = false;

    /**
     * The immutable snapshot covering the current log base, retained so InstallSnapshot ships bytes
     * that actually match the {@code (lastIncludedIndex, lastIncludedTerm)} they declare. Capturing
     * {@code stateMachine.snapshot()} at send time instead would describe a later point in history
     * once further commands were applied. Null iff the base is 0.
     */
    private Snapshot lastSnapshot;

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
            this.lastSnapshot = recoveredSnapshot;
            this.stateMachine.restore(recoveredSnapshot.data(),
                    recoveredSnapshot.lastIncludedIndex(), recoveredSnapshot.lastIncludedTerm());
            this.commitIndex = recoveredSnapshot.lastIncludedIndex();
            this.lastApplied = recoveredSnapshot.lastIncludedIndex();
        } else {
            this.log.replace(recovered.entries());
        }
        this.appliedWatermark.set(this.lastApplied);
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

    /** The leader of the current term as far as this node knows, or null. */
    public String leaderId() {
        return leaderId;
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
        leaderId = req.leaderId();
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
        // Acknowledge the last index THIS request established, not the local tail. A follower may
        // hold a longer uncommitted suffix that the leader never verified; reporting it would let
        // the leader advance nextIndex past the end of its own log.
        return AppendEntriesResponse.success(currentTerm, req.prevLogIndex() + req.entries().size());
    }

    /**
     * Follower side of InstallSnapshot (Raft §7): a leader ships a snapshot when the entries this
     * follower needs have been compacted away. Reject a stale term, then ignore any snapshot that
     * does not advance past what is already committed here — a delayed snapshot must never roll
     * committed/applied state backward. For a snapshot that does advance:
     * if the local log already contains a matching entry at {@code lastIncludedIndex}, retain the
     * suffix above it (§7's "retain log entries following it"); otherwise discard the log entirely.
     * Either way restore the state machine, advance commit/applied to the base, and persist.
     */
    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest req) {
        if (req.term() < currentTerm) {
            return InstallSnapshotResponse.of(currentTerm);
        }
        if (req.term() > currentTerm) {
            stepDown(req.term());
        }
        role = RaftRole.FOLLOWER;
        leaderId = req.leaderId();
        resetElectionTimer();

        // commitIndex >= lastApplied always, so this one guard prevents both regressions. It also
        // subsumes the old "already compacted" check, since commitIndex >= lastIncludedIndex.
        if (req.lastIncludedIndex() <= log.lastIncludedIndex()
                || req.lastIncludedIndex() <= commitIndex) {
            return InstallSnapshotResponse.of(currentTerm); // already covered
        }

        if (log.matches(req.lastIncludedIndex(), req.lastIncludedTerm())) {
            log.compactThrough(req.lastIncludedIndex(), req.lastIncludedTerm());
        } else {
            log.resetToSnapshot(req.lastIncludedIndex(), req.lastIncludedTerm());
        }
        stateMachine.restore(req.data(), req.lastIncludedIndex(), req.lastIncludedTerm());
        commitIndex = req.lastIncludedIndex();
        lastApplied = req.lastIncludedIndex();
        publishApplied();
        // Retain the exact bytes for this base so a later relay of this snapshot still matches its
        // declared metadata.
        lastSnapshot = Snapshot.of(req.lastIncludedIndex(), req.lastIncludedTerm(), req.data());
        if (persistence != null) {
            try {
                persistence.recordSnapshot(lastSnapshot);
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
        leaderId = null;
        votesReceived.clear();
        persistTerm();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            byte[] command = log.entryAt(lastApplied).command();
            if (command.length == 0) {
                continue; // §8 no-op barrier: a log record only, never a state-machine command
            }
            stateMachine.apply(command, lastApplied);
        }
        publishApplied();
    }

    /** Publishes {@code lastApplied} to waiters that are not holding this node's monitor. */
    private void publishApplied() {
        if (appliedWatermark.getAndSet(lastApplied) != lastApplied) {
            synchronized (appliedMonitor) {
                appliedMonitor.notifyAll();
            }
        }
    }

    /**
     * Blocks until this node has applied {@code index}, or the timeout elapses. Does not hold the
     * node monitor, so replication and inbound RPCs continue while a client waits here.
     */
    public boolean awaitApplied(long index, java.time.Duration timeout) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        synchronized (appliedMonitor) {
            while (appliedWatermark.get() < index) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                appliedMonitor.wait(Math.max(1L, remaining / 1_000_000L));
            }
        }
        return true;
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
        ticks++;
        if (role == RaftRole.LEADER) {
            if (replicationDelegated) {
                return; // the driver's per-peer threads own replication
            }
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

    /**
     * The local half of starting an election: bump the term, vote for self, persist, and build the
     * RequestVote to send. Returns null when there is nothing left to solicit — either a single-node
     * group that won on its own vote, or a node that is no longer a candidate.
     *
     * <p>Split out from the fan-out so a caller can perform the vote RPCs with this node's monitor
     * released. LogCabin does the same thing by handing its {@code lockGuard} into
     * {@code requestVote()}; holding the lock across peer I/O blocks every inbound RPC for as long
     * as the slowest unreachable peer takes to time out.
     */
    private RequestVoteRequest beginElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        leaderId = null; // the term we are contesting has no leader we know of
        persistTerm();
        votesReceived.clear();
        votesReceived.add(nodeId);
        resetElectionTimer();

        if (hasMajority(votesReceived.size())) {
            becomeLeader(); // single-node group: the self-vote is already a majority
            return null;
        }
        return RequestVoteRequest.of(currentTerm, nodeId, log.lastIndex(), log.lastTerm());
    }

    /**
     * Folds one peer's vote reply into the election started at {@code electionTerm}, promoting this
     * node as soon as a majority has granted. Returns true while the election is still worth
     * soliciting, false once this node has won, stepped down, or moved on to another term.
     *
     * <p>Safe to call from any thread, so the fan-out can run on the replication driver's threads.
     */
    public synchronized boolean recordVote(
            String peerId, long electionTerm, RequestVoteResponse resp) {
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return false;
        }
        if (role != RaftRole.CANDIDATE || currentTerm != electionTerm) {
            return false; // this election is already over
        }
        if (resp.term() != electionTerm) {
            return true; // reply from an older term: ignore it, but keep soliciting the rest
        }
        if (resp.voteGranted()) {
            votesReceived.add(peerId);
            if (hasMajority(votesReceived.size())) {
                becomeLeader();
                return false;
            }
        }
        return true;
    }

    /**
     * The {@link #tick()} a {@link RaftReplicationDriver} calls instead: it advances the election
     * clock and, when the timer fires, performs only the local half of starting an election. The
     * returned RequestVote is the driver's to fan out off-monitor, feeding each reply back through
     * {@link #recordVote}. Returns null when no election started.
     */
    public synchronized RequestVoteRequest tickForDriver() {
        ticks++;
        if (role == RaftRole.LEADER) {
            return null; // the driver's peer threads own the leader's heartbeats
        }
        electionElapsed++;
        if (electionElapsed < electionTimeout) {
            return null;
        }
        return beginElection();
    }

    private void startElection() {
        RequestVoteRequest req = beginElection();
        if (req == null) {
            return;
        }
        for (RaftPeer peer : peers.values()) {
            try {
                if (!recordVote(peer.nodeId(), req.term(), peer.requestVote(req))) {
                    return; // won, stepped down, or superseded: stop soliciting
                }
            } catch (RuntimeException unreachable) {
                // dropped RequestVote: no vote from this peer.
            }
        }
    }

    private boolean hasMajority(int votes) {
        int clusterSize = peerIds.size() + 1;
        return votes > clusterSize / 2;
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = nodeId;
        heartbeatElapsed = HEARTBEAT_INTERVAL; // send a heartbeat promptly
        nextIndex.clear();
        matchIndex.clear();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, log.lastIndex() + 1);
            matchIndex.put(peerId, 0L);
        }
        // Raft §8: a new leader cannot trust its commit index until it has committed an entry from
        // its own term, because §5.4.2 forbids committing a prior-term entry by match count alone.
        // Appending an empty barrier entry makes that true immediately, which is what lets
        // readIndex() serve a linearizable read without waiting for the term's first write.
        long barrier = log.lastIndex() + 1;
        log.append(LogEntry.of(currentTerm, barrier, new byte[0]));
        persistEntry(log.entryAt(barrier));
        matchIndex.put(nodeId, barrier);
        if (!replicationDelegated) {
            sendHeartbeats();
        }
        advanceCommitIndex();
    }

    private void sendHeartbeats() {
        for (RaftPeer peer : peers.values()) {
            replicateTo(peer);
        }
    }

    /**
     * Establishes that this node is still the leader by exchanging one replication round with the
     * cluster and requiring a majority of positive acknowledgements (Raft §8 / the ReadIndex
     * protocol). An isolated former leader reaches nobody, counts only itself, and fails here.
     */
    private boolean confirmLeadership() {
        int acknowledgements = 1; // the leader counts itself
        for (RaftPeer peer : peers.values()) {
            if (replicateTo(peer)) {
                acknowledgements++;
            }
        }
        return role == RaftRole.LEADER && hasMajority(acknowledgements);
    }

    /**
     * The commit index a linearizable read may be served at, or {@code -1} when this node cannot
     * safely answer — it is not the leader, it has not yet committed an entry from its own term, or
     * it could not confirm leadership against a majority. Callers must wait for
     * {@link #lastApplied()} to reach the returned index before reading the state machine.
     */
    public synchronized long readIndex() {
        long index = readIndexCandidate();
        if (index < 0) {
            return -1;
        }
        if (!confirmLeadership()) {
            return -1;
        }
        return index;
    }

    /**
     * Steps 1-2 of the ReadIndex protocol (Ongaro §6.4): the commit index a linearizable read could
     * be served at, or {@code -1} when this node must not answer. Performs no I/O, so a caller can
     * run step 3 — the majority heartbeat round — with this node's monitor released. The index is
     * only usable if that round succeeds and this node is still leader of the same term afterwards.
     */
    public synchronized long readIndexCandidate() {
        if (role != RaftRole.LEADER) {
            return -1;
        }
        if (log.termAt(commitIndex) != currentTerm) {
            return -1; // no current-term commit yet: the commit index is not yet trustworthy
        }
        return commitIndex;
    }

    /**
     * An empty AppendEntries for {@code peerId}, i.e. the heartbeat ReadIndex step 3 counts
     * acknowledgements of. Null when this node is not the leader.
     *
     * <p>It anchors on {@code matchIndex}, not {@code nextIndex}, and that choice is load-bearing:
     * matchIndex is a prefix the follower has already confirmed, so the probe always passes the
     * follower's log check. Building it from nextIndex the way {@link #nextReplicationRequest}
     * does would ship a whole InstallSnapshot to a lagging follower merely to confirm leadership.
     */
    public synchronized AppendEntriesRequest heartbeatProbe(String peerId) {
        if (role != RaftRole.LEADER) {
            return null;
        }
        long matched = Math.max(matchIndex.getOrDefault(peerId, 0L), log.lastIncludedIndex());
        return AppendEntriesRequest.of(currentTerm, nodeId, matched, log.termAt(matched),
                java.util.Collections.emptyList(), commitIndex);
    }

    /**
     * Replicates to one peer inline; returns true iff the peer positively acknowledged this round.
     * This is the synchronous, tick-driven path used by the deterministic harnesses. A
     * {@link RaftReplicationDriver} performs the same exchange with the monitor released, using
     * {@link #nextReplicationRequest(String)} and the {@code apply*Response} handlers below.
     */
    private boolean replicateTo(RaftPeer peer) {
        ReplicationRequest request = nextReplicationRequest(peer.nodeId());
        if (request == null) {
            return false;
        }
        try {
            if (request.isSnapshot()) {
                InstallSnapshotResponse resp = peer.installSnapshot(request.snapshot());
                applyInstallSnapshotResponse(
                        peer.nodeId(), resp, request.snapshot().lastIncludedIndex());
                return role == RaftRole.LEADER;
            }
            AppendEntriesResponse resp = peer.appendEntries(request.entries());
            applyAppendEntriesResponse(peer.nodeId(), resp);
            return resp.success() && role == RaftRole.LEADER;
        } catch (RuntimeException unreachable) {
            // dropped RPC: retry on the next heartbeat.
            return false;
        }
    }

    /**
     * The next replication message owed to {@code peerId}, or null if this node is not the leader.
     * Built under the node monitor so the caller can perform the RPC without holding it.
     */
    public synchronized ReplicationRequest nextReplicationRequest(String peerId) {
        if (role != RaftRole.LEADER) {
            return null;
        }
        long ni = nextIndex.getOrDefault(peerId, log.lastIndex() + 1);
        if (ni <= log.lastIncludedIndex()) {
            // The entries this follower needs are compacted away — ship the snapshot instead.
            long base = log.lastIncludedIndex();
            if (lastSnapshot == null || lastSnapshot.lastIncludedIndex() != base) {
                // The log base and the retained snapshot are set together; a mismatch means a
                // caller moved the base without capturing its bytes. Fail loudly rather than ship
                // a snapshot whose contents disagree with its metadata.
                throw new IllegalStateException("no retained snapshot for log base " + base);
            }
            return ReplicationRequest.snapshot(InstallSnapshotRequest.of(
                    currentTerm, nodeId, base, lastSnapshot.lastIncludedTerm(),
                    lastSnapshot.data()));
        }
        long prevLogIndex = ni - 1;
        return ReplicationRequest.entries(AppendEntriesRequest.of(currentTerm, nodeId, prevLogIndex,
                log.termAt(prevLogIndex), log.from(ni), commitIndex));
    }

    /** Folds a peer's AppendEntries reply into leader progress. Safe to call from any thread. */
    public synchronized void applyAppendEntriesResponse(String peerId, AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return;
        }
        if (role != RaftRole.LEADER || resp.term() < currentTerm) {
            return; // stale reply from an earlier term or role
        }
        if (resp.success()) {
            lastAckTick.put(peerId, ticks);
            // Replies can arrive out of order once peers run on their own threads, so match
            // progress only ever moves forward within a term.
            long matched = Math.max(matchIndex.getOrDefault(peerId, 0L), resp.matchIndex());
            matchIndex.put(peerId, matched);
            nextIndex.put(peerId, matched + 1);
            advanceCommitIndex();
        } else {
            nextIndex.put(peerId, Math.max(1, resp.conflictIndex()));
        }
    }

    /** Folds a peer's InstallSnapshot reply into leader progress. Safe to call from any thread. */
    public synchronized void applyInstallSnapshotResponse(
            String peerId, InstallSnapshotResponse resp, long lastIncludedIndex) {
        if (resp.term() > currentTerm) {
            stepDown(resp.term());
            return;
        }
        if (role != RaftRole.LEADER) {
            return;
        }
        lastAckTick.put(peerId, ticks);
        long matched = Math.max(matchIndex.getOrDefault(peerId, 0L), lastIncludedIndex);
        matchIndex.put(peerId, matched);
        nextIndex.put(peerId, matched + 1);
        advanceCommitIndex();
    }

    public long lastApplied() {
        return lastApplied;
    }

    /**
     * Whether this node could serve or usefully redirect a client request right now, where
     * {@code staleTicks} is how long contact may lapse before it stops counting — one election
     * timeout is the natural setting.
     *
     * <p>A leader needs a majority to have acknowledged it recently. Knowing its own role is not
     * enough: a leader cut off from the cluster keeps the title until it hears a higher term, and
     * in the meantime it can neither commit a write nor confirm a read.
     *
     * <p>A follower only needs to have heard from a leader recently, and that is weaker than it
     * looks. A follower stranded in a minority alongside its leader keeps receiving heartbeats, so
     * it keeps reporting itself able to redirect, to a leader that will refuse. Closing that gap
     * means asking the leader whether it still holds a majority, which is a round trip this check
     * exists to avoid — etcd pays it, running a linearizable read on every {@code /readyz}. The
     * consequence is bounded: the leader itself reports not-ready and drops out, so the redirect
     * costs a client one wasted hop before it fails, rather than returning stale data.
     *
     * <p>Never used for a consensus decision, only for reporting.
     */
    public synchronized boolean canServe(int staleTicks) {
        if (role == RaftRole.LEADER) {
            int fresh = 1; // the leader counts itself
            for (String peerId : peerIds) {
                if (ticks - lastAckTick.getOrDefault(peerId, Long.MIN_VALUE) <= staleTicks) {
                    fresh++;
                }
            }
            return hasMajority(fresh);
        }
        return leaderId != null && electionElapsed < electionTimeout;
    }

    /** Injectable compaction trigger: compact once this many physical applied entries accumulate. */
    public void setCompactionThreshold(int threshold) {
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
        // Capture the bytes and the position they cover in one step, then keep them: this exact
        // pair is what InstallSnapshot later ships.
        Snapshot snapshot = Snapshot.of(lastApplied, term, stateMachine.snapshot());
        if (persistence != null) {
            try {
                persistence.recordSnapshot(snapshot);
            } catch (java.io.IOException e) {
                throw new RuntimeException("raft persistence failed", e);
            }
        }
        log.compactThrough(lastApplied, term);
        lastSnapshot = snapshot;
    }

    /**
     * Append a command to the leader's log and replicate it. Returns the assigned 1-based log
     * index (so a caller can wait for {@link #lastApplied()} {@code >= index}), or {@code 0} if
     * this node is not the leader.
     */
    public synchronized long propose(byte[] command) {
        long index = proposeLocal(command);
        if (index == 0) {
            return 0;
        }
        sendHeartbeats();
        advanceCommitIndex();
        return index;
    }

    /**
     * Appends and durably records a command on the leader <em>without</em> contacting any peer, and
     * returns its assigned index (0 if not leader). This is the half of {@link #propose(byte[])} a
     * {@link RaftReplicationDriver} uses: the caller then wakes the peer threads and waits on
     * {@link #awaitApplied(long, java.time.Duration)}, so no peer I/O happens on its thread.
     */
    public synchronized long proposeLocal(byte[] command) {
        if (role != RaftRole.LEADER) {
            return 0;
        }
        long index = log.lastIndex() + 1;
        log.append(LogEntry.of(currentTerm, index, command));
        persistEntry(log.entryAt(index));
        matchIndex.put(nodeId, index); // leader trivially has it
        advanceCommitIndex(); // a single-node cluster commits immediately
        return index;
    }

    /** Hands per-peer replication to a {@link RaftReplicationDriver}; ticks then only run elections. */
    void delegateReplication(boolean delegated) {
        this.replicationDelegated = delegated;
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
