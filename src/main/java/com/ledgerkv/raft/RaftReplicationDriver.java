package com.ledgerkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a leader's replication off the caller's thread: one thread per peer, plus a ticker thread
 * for election timeouts. This is the LogCabin arrangement (a dedicated peer thread per follower)
 * rather than etcd/raft's fully I/O-free core, chosen because it keeps the existing
 * {@link RaftPeer} transport seam and the deterministic tick-driven harnesses intact.
 *
 * <p>It exists to break three couplings in the earlier synchronous path:
 *
 * <ul>
 *   <li>A proposal completes when <em>its own entry</em> is committed and applied, not when the
 *       last peer in an iteration order finally answers. A slow minority peer no longer holds the
 *       caller after the majority has already made the write durable.
 *   <li>Peer I/O happens with the node monitor released. A blocked peer therefore cannot stall
 *       inbound RequestVote/AppendEntries handling on this node.
 *   <li>Replication that has not finished continues on its own thread instead of being abandoned.
 * </ul>
 *
 * <p><b>Scope.</b> The driver owns replication, the election clock, elections themselves, and the
 * ReadIndex confirmation round. All four keep peer I/O off the node monitor: the node builds each
 * message under its lock, the driver sends it without one, and the reply is folded back under the
 * lock. {@link RaftNode#tick()} and {@link RaftNode#readIndex()} keep their inline equivalents for
 * the deterministic harnesses, which have no threads to fan out onto.
 */
public final class RaftReplicationDriver implements AutoCloseable {

    /** How long a peer thread sleeps between rounds when nothing has woken it (the heartbeat). */
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofMillis(50);

    private final RaftNode node;
    private final List<RaftPeer> peers;
    private final Duration heartbeat;
    private final ExecutorService threads;
    /**
     * Separate from {@link #threads}, whose every slot is permanently occupied by a replication or
     * ticker loop. Elections and ReadIndex rounds are bursts of one RPC per peer that must run
     * concurrently with those loops, not queue behind them.
     *
     * <p>Fixed, not cached: a ReadIndex round costs one thread per peer, so an unbounded pool would
     * grow a thread per peer per concurrent read. The cap makes heavy read load queue instead,
     * which the round's own deadline still bounds. etcd avoids the tradeoff by amortizing a single
     * heartbeat round across every read queued at that moment; this driver confirms per read.
     */
    private final ExecutorService fanout;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object wake = new Object();

    public RaftReplicationDriver(RaftNode node, List<RaftPeer> peers) {
        this(node, peers, DEFAULT_HEARTBEAT);
    }

    public RaftReplicationDriver(RaftNode node, List<RaftPeer> peers, Duration heartbeat) {
        this.node = Objects.requireNonNull(node, "node");
        this.peers = new ArrayList<>(Objects.requireNonNull(peers, "peers"));
        this.heartbeat = Objects.requireNonNull(heartbeat, "heartbeat");
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "raft-repl-" + node.nodeId());
            thread.setDaemon(true);
            return thread;
        };
        ThreadFactory fanoutFactory = runnable -> {
            Thread thread = new Thread(runnable, "raft-fanout-" + node.nodeId());
            thread.setDaemon(true);
            return thread;
        };
        this.threads = Executors.newFixedThreadPool(this.peers.size() + 1, factory);
        this.fanout = Executors.newFixedThreadPool(
                Math.max(2, this.peers.size() * 4), fanoutFactory);
    }

    /** The node this driver replicates for. */
    public String nodeId() {
        return node.nodeId();
    }

    /** Starts the per-peer replication threads and the election ticker. */
    public RaftReplicationDriver start() {
        node.delegateReplication(true);
        for (RaftPeer peer : peers) {
            threads.submit(() -> replicationLoop(peer));
        }
        threads.submit(this::tickLoop);
        return this;
    }

    /**
     * Proposes a command and returns once <em>this</em> entry is committed and applied, or throws
     * on timeout. The calling thread performs no peer I/O and holds no node lock while waiting.
     */
    public long propose(byte[] command, Duration timeout) throws InterruptedException {
        long index = node.proposeLocal(command);
        if (index == 0) {
            throw new IllegalStateException("target node is not the leader");
        }
        signal(); // replicate now rather than at the next heartbeat
        if (!node.awaitApplied(index, timeout)) {
            throw new IllegalStateException(
                    "entry " + index + " was not committed within " + timeout);
        }
        return index;
    }

    /**
     * The full ReadIndex protocol (Ongaro §6.4) run off the node monitor: take the candidate commit
     * index, confirm leadership with a majority heartbeat round, re-check that leadership survived
     * the round, then wait for the state machine to catch up to the index. Returns the index a
     * linearizable read may be served at, or {@code -1} when this node must refuse.
     *
     * <p>An isolated former leader reaches nobody, counts only its own vote, and fails the majority
     * check — which is the whole point: it cannot know a newer leader has superseded its state.
     */
    public long readIndex(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        // Read the term before the index: if the term moves between the two, the re-check below
        // compares against the older term and refuses. The other order could let it slip through.
        long term = node.currentTerm();

        // Step 1. A leader may not trust its commit index until an entry from its own term has
        // committed; becomeLeader() appends the no-op barrier that makes that true within a
        // replication round. Wait for it, as §6.4 step 1 says to, instead of failing every read
        // issued in the first moments of a term.
        long index = node.readIndexCandidate();
        while (index < 0) {
            if (!node.isLeader() || System.nanoTime() >= deadline) {
                return -1;
            }
            Thread.sleep(Math.max(1L, heartbeat.toMillis() / 4));
            index = node.readIndexCandidate();
        }

        Duration left = remaining(deadline);
        if (left.isZero() || !confirmLeadership(left)) {
            return -1;
        }
        // The round can overlap a step down, so re-check rather than trusting the count alone.
        if (!node.isLeader() || node.currentTerm() != term) {
            return -1;
        }
        left = remaining(deadline);
        return !left.isZero() && node.awaitApplied(index, left) ? index : -1;
    }

    /** Time left before {@code deadline}, never negative. */
    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        return nanos <= 0 ? Duration.ZERO : Duration.ofNanos(nanos);
    }

    /**
     * ReadIndex step 3: one heartbeat per peer, in parallel, bounded by {@code timeout}. True once
     * a majority including this node has acknowledged within the deadline.
     */
    private boolean confirmLeadership(Duration timeout) throws InterruptedException {
        List<Callable<Boolean>> probes = new ArrayList<>();
        for (RaftPeer peer : peers) {
            probes.add(() -> {
                AppendEntriesRequest probe = node.heartbeatProbe(peer.nodeId());
                if (probe == null) {
                    return false;
                }
                AppendEntriesResponse response = peer.appendEntries(probe);
                node.applyAppendEntriesResponse(peer.nodeId(), response);
                return response.success();
            });
        }
        int acknowledgements = 1; // the leader counts itself
        for (Future<Boolean> probe : invokeAllBounded(probes, timeout)) {
            if (completedTrue(probe)) {
                acknowledgements++;
            }
        }
        return acknowledgements > (peers.size() + 1) / 2;
    }

    /**
     * Fans a candidate's RequestVote out to every peer, one task each, and returns immediately.
     * Each reply folds itself in through {@link RaftNode#recordVote}, which promotes the candidate
     * as soon as a majority grants.
     *
     * <p>Deliberately not blocking: the tick loop is the election clock, so waiting for a round
     * here would stop the clock for as long as the slowest unreachable peer takes to time out.
     * A round that wins no majority simply expires, and the next timeout starts a fresh term —
     * votes from the stale round are then ignored on their election term.
     */
    private void solicitVotes(RequestVoteRequest request) {
        for (RaftPeer peer : peers) {
            fanout.submit(() -> {
                try {
                    node.recordVote(peer.nodeId(), request.term(), peer.requestVote(request));
                } catch (RuntimeException unreachable) {
                    // dropped RequestVote: no vote from this peer.
                }
            });
        }
    }

    private <T> List<Future<T>> invokeAllBounded(List<Callable<T>> tasks, Duration timeout)
            throws InterruptedException {
        return fanout.invokeAll(tasks, Math.max(1L, timeout.toMillis()), TimeUnit.MILLISECONDS);
    }

    private static boolean completedTrue(Future<Boolean> future) {
        if (future.isCancelled()) {
            return false; // the deadline cut this peer off
        }
        try {
            return Boolean.TRUE.equals(future.get());
        } catch (ExecutionException | InterruptedException e) {
            return false; // an unreachable peer is an absent acknowledgement, not an error
        }
    }

    /** Wakes every peer thread for an immediate replication round. */
    public void signal() {
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    private void replicationLoop(RaftPeer peer) {
        while (running.get()) {
            try {
                replicateOnce(peer);
            } catch (RuntimeException e) {
                // A peer thread must never die: an unreachable peer is normal, and a state error
                // here would otherwise silently strand this follower.
            }
            synchronized (wake) {
                if (!running.get()) {
                    return;
                }
                try {
                    wake.wait(heartbeat.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void replicateOnce(RaftPeer peer) {
        ReplicationRequest request = node.nextReplicationRequest(peer.nodeId());
        if (request == null) {
            return; // not the leader right now
        }
        // The RPC below runs with the node monitor released — that is the whole point of the split.
        if (request.isSnapshot()) {
            InstallSnapshotResponse response = peer.installSnapshot(request.snapshot());
            node.applyInstallSnapshotResponse(
                    peer.nodeId(), response, request.snapshot().lastIncludedIndex());
            return;
        }
        AppendEntriesResponse response = peer.appendEntries(request.entries());
        node.applyAppendEntriesResponse(peer.nodeId(), response);
    }

    private void tickLoop() {
        while (running.get()) {
            try {
                boolean wasLeader = node.isLeader();
                RequestVoteRequest election = node.tickForDriver();
                if (election != null) {
                    solicitVotes(election);
                }
                if (!wasLeader && node.isLeader()) {
                    // A round just won: heartbeat now rather than waiting out a peer sleep.
                    signal();
                }
                node.maybeCompact();
            } catch (RuntimeException e) {
                // keep the election clock alive across a transport failure
            }
            try {
                Thread.sleep(heartbeat.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        node.delegateReplication(false);
        signal();
        threads.shutdownNow();
        fanout.shutdownNow();
        try {
            threads.awaitTermination(5, TimeUnit.SECONDS);
            fanout.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
