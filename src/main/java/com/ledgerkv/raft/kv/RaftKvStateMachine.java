package com.ledgerkv.raft.kv;

import com.ledgerkv.raft.StateMachine;
import java.util.HashMap;
import java.util.Map;

/**
 * A deterministic in-memory key-value state machine driven by committed Raft commands. In-memory is
 * the locked default (Phase 3 spec §Architecture) so the consensus concern stays separate from the
 * LSM storage concern exercised by the quorum path.
 *
 * <p><b>Determinism:</b> {@link #apply(byte[])} depends only on the command bytes and the prior
 * applied state — no clock, no randomness — so applying the same log on every replica yields the
 * same state and the same per-command result.
 *
 * <p><b>At-most-once:</b> a per-client dedup table records the last applied sequence number and its
 * result. A command whose sequence is not strictly greater than the client's last applied sequence
 * is a duplicate: it is NOT re-applied and the cached result is returned. This makes a retried
 * client request safe even though Raft may deliver it more than once.
 */
public final class RaftKvStateMachine implements StateMachine {

    private static final byte[] EMPTY = new byte[0];

    private final Map<String, byte[]> store = new HashMap<>();

    private static final class Dedup {
        long lastSequence;
        byte[] lastResult;
    }

    private final Map<String, Dedup> dedupByClient = new HashMap<>();

    @Override
    public synchronized byte[] apply(byte[] command) {
        KvCommand cmd = KvCommand.decode(command);
        Dedup dedup = dedupByClient.get(cmd.clientId());
        if (dedup != null && cmd.sequenceNumber() <= dedup.lastSequence) {
            return dedup.lastResult; // at-most-once: already applied (or stale) — return cached
        }

        byte[] result;
        if (cmd.op() == KvCommand.Op.PUT) {
            store.put(cmd.key(), cmd.value());
            result = cmd.value();
        } else { // DELETE
            byte[] previous = store.remove(cmd.key());
            result = previous == null ? EMPTY : previous;
        }

        if (dedup == null) {
            dedup = new Dedup();
            dedupByClient.put(cmd.clientId(), dedup);
        }
        dedup.lastSequence = cmd.sequenceNumber();
        dedup.lastResult = result;
        return result;
    }

    /** Leader-side linearizable read off the applied state (null if absent). */
    public synchronized byte[] get(String key) {
        byte[] v = store.get(key);
        return v == null ? null : v.clone();
    }

    /** Last applied sequence number for a client (0 if the client has applied nothing). */
    public synchronized long lastAppliedSequence(String clientId) {
        Dedup d = dedupByClient.get(clientId);
        return d == null ? 0 : d.lastSequence;
    }

    /** The cached apply result for a client's given sequence (null if not that client's last). */
    public synchronized byte[] resultFor(String clientId, long sequence) {
        Dedup d = dedupByClient.get(clientId);
        if (d == null || d.lastSequence != sequence) {
            return null;
        }
        return d.lastResult == null ? null : d.lastResult.clone();
    }
}
