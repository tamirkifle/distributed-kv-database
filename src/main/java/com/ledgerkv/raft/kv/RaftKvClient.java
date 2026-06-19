package com.ledgerkv.raft.kv;

import com.ledgerkv.raft.RaftNode;
import java.util.Objects;

/**
 * A thin, leader-pinned facade that turns the Raft replicated log into a <b>linearizable</b>
 * key-value register set. A write encodes a {@link KvCommand}, proposes it to the leader, then
 * deterministically drives the group (the {@code driveToCommit} pump) until the assigned log index
 * has been applied at the leader — so the write has taken effect on a majority before the call
 * returns. A read drives the leader's apply pipeline to the commit index and then serves the value
 * off the leader's applied state, giving a leader read that reflects every committed write
 * (linearizable for the single-leader, deterministically-driven harness used in 3c/3e).
 *
 * <p>Each client owns a monotonically increasing sequence number; combined with the state machine's
 * dedup table this gives at-most-once write semantics under retries. Leader redirection and a full
 * leader-lease / read-index read path are out of scope for 3c (the natural next step) — the harness
 * always targets the current leader.
 */
public final class RaftKvClient {

    private final String clientId;
    private final RaftNode leader;
    private final RaftKvStateMachine leaderStateMachine;
    private final Runnable driveToCommit;
    private long nextSequence = 1;

    public RaftKvClient(String clientId, RaftNode leader, RaftKvStateMachine leaderStateMachine,
            Runnable driveToCommit) {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.leader = Objects.requireNonNull(leader, "leader");
        this.leaderStateMachine = Objects.requireNonNull(leaderStateMachine, "leaderStateMachine");
        this.driveToCommit = Objects.requireNonNull(driveToCommit, "driveToCommit");
    }

    public byte[] put(String key, byte[] value) {
        return putWithSequence(nextSequence++, key, value);
    }

    public byte[] delete(String key) {
        return deleteWithSequence(nextSequence++, key);
    }

    /** Put at an explicit client sequence (used to exercise at-most-once retry behavior). */
    public byte[] putWithSequence(long sequence, String key, byte[] value) {
        return submit(KvCommand.put(clientId, sequence, key, value));
    }

    /** Delete at an explicit client sequence. */
    public byte[] deleteWithSequence(long sequence, String key) {
        return submit(KvCommand.delete(clientId, sequence, key));
    }

    private byte[] submit(KvCommand cmd) {
        long index = leader.propose(cmd.encode());
        if (index == 0) {
            throw new IllegalStateException("target node is not the leader");
        }
        driveUntilApplied(index);
        // The leader applied this exact index; the dedup table caches the per-command result
        // (PUT: the stored value; DELETE: the previous value or empty array).
        return leaderStateMachine.resultFor(clientId, cmd.sequenceNumber());
    }

    /**
     * Linearizable leader read via the ReadIndex barrier (Raft §8): establish a read index — which
     * requires this node to be the leader, to have committed an entry from its current term, and to
     * confirm leadership against a majority — then wait for the apply pipeline to reach it before
     * serving off applied state.
     *
     * <p>Refuses rather than answering from stale local state when leadership cannot be
     * established: an isolated former leader has no way to learn that a newer leader has already
     * superseded the value it holds.
     */
    public byte[] get(String key) {
        long readIndex = leader.readIndex();
        if (readIndex < 0) {
            throw new IllegalStateException(
                    "leadership could not be confirmed for a linearizable read");
        }
        driveUntilApplied(readIndex);
        return leaderStateMachine.get(key);
    }

    private void driveUntilApplied(long index) {
        int guard = 0;
        while (leader.lastApplied() < index) {
            driveToCommit.run();
            if (++guard > 1000) {
                throw new IllegalStateException(
                        "commit did not advance to index " + index + " (leader lost?)");
            }
        }
    }
}
