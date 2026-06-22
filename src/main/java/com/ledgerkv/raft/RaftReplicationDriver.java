package com.ledgerkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * <p><b>Scope.</b> The driver owns replication and the election clock. Leader election itself still
 * runs inline on the ticker thread, and the ReadIndex confirmation round in
 * {@link RaftNode#readIndex()} is likewise inline; both are bounded by the
 * transport's own RPC deadline rather than by this class.
 */
public final class RaftReplicationDriver implements AutoCloseable {

    /** How long a peer thread sleeps between rounds when nothing has woken it (the heartbeat). */
    private static final Duration DEFAULT_HEARTBEAT = Duration.ofMillis(50);

    private final RaftNode node;
    private final List<RaftPeer> peers;
    private final Duration heartbeat;
    private final ExecutorService threads;
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
        this.threads = Executors.newFixedThreadPool(this.peers.size() + 1, factory);
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
                node.tick();
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
        try {
            threads.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
