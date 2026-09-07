package com.ledgerkv.raft.kv;

import com.ledgerkv.raft.Snapshot;
import com.ledgerkv.raft.StateMachine;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A deterministic in-memory key-value state machine driven by committed Raft commands. In-memory is
 * the locked default (Phase 3 spec §Architecture) so the consensus concern stays separate from the
 * LSM storage concern exercised by the quorum path.
 *
 * <p><b>Determinism:</b> {@link #apply(byte[])} depends only on the command bytes and the prior
 * applied state — no clock, no randomness — so applying the same log on every replica yields the
 * same state and the same per-command result.
 *
 * <p><b>Client sessions (Ongaro §6.3):</b> each client id gets a session holding its last applied
 * sequence, that command's fingerprint, and the result. A re-delivery of the same command returns
 * the cached result instead of applying twice. A <em>different</em> command reusing that sequence,
 * or a sequence the client has already moved past, is refused rather than absorbed — silently
 * treating either as a duplicate loses a write with no trace.
 *
 * <p><b>Session expiry:</b> sessions are capped and evicted least-recently-applied. The thesis is
 * explicit that expiry must be deterministic, because a replica that evicts a session and then
 * re-applies that client's duplicates diverges from one that kept it. So recency here is driven
 * only by {@link #apply}, never by a read, and the snapshot preserves eviction order rather than
 * sorting it away. Every replica must run the same {@code maxSessions}: it is part of the
 * deterministic state machine, not a local tuning knob.
 */
public final class RaftKvStateMachine implements StateMachine {

    /**
     * Sessions retained before the least-recently-applied is dropped. Sized so an ordinary client
     * population never reaches it; a client whose session is evicted while still live gets
     * {@link KvOutcome.Status#NOT_APPLIED} rather than a silent second apply.
     */
    public static final int DEFAULT_MAX_SESSIONS = 4096;

    private static final byte[] EMPTY = new byte[0];

    /** A stored value and the log index of the command that wrote it, which is its version. */
    private static final class Entry {
        final byte[] value;
        final long index;

        Entry(byte[] value, long index) {
            this.value = value;
            this.index = index;
        }
    }

    private final Map<String, Entry> store = new java.util.HashMap<>();

    private static final class Session {
        long lastSequence;
        long fingerprint;
        long appliedIndex;
        byte[] lastResult;
    }

    /**
     * Insertion-ordered, not access-ordered: {@link LinkedHashMap}'s access order would be
     * perturbed by {@link #get} and {@link #outcomeFor}, which only ever run on the leader. That
     * would make the leader's eviction order differ from its followers'. Recency is instead moved
     * by hand in {@link #touch}, which only {@link #apply} calls.
     */
    private final LinkedHashMap<String, Session> sessions = new LinkedHashMap<>();

    private final int maxSessions;

    public RaftKvStateMachine() {
        this(DEFAULT_MAX_SESSIONS);
    }

    public RaftKvStateMachine(int maxSessions) {
        if (maxSessions < 1) {
            throw new IllegalArgumentException("maxSessions must be positive: " + maxSessions);
        }
        this.maxSessions = maxSessions;
    }

    @Override
    public synchronized byte[] apply(byte[] command) {
        return apply(command, 0L);
    }

    @Override
    public synchronized byte[] apply(byte[] command, long index) {
        KvCommand cmd = KvCommand.decode(command);
        Session session = sessions.get(cmd.clientId());

        if (session != null) {
            if (cmd.sequenceNumber() < session.lastSequence) {
                return EMPTY; // stale: this client has already moved past this sequence
            }
            if (cmd.sequenceNumber() == session.lastSequence) {
                if (session.fingerprint != cmd.fingerprint()) {
                    return EMPTY; // the sequence is taken by a different command
                }
                touch(cmd.clientId(), session);
                return session.lastResult; // genuine retry: answer from the record
            }
        } else if (cmd.sequenceNumber() != 1) {
            // No session and not a first request. Either this client's session was evicted or it
            // lost track of its own numbering; opening a session here would re-admit commands it
            // may already have applied. Refuse and let it start over under a fresh id.
            return EMPTY;
        } else {
            session = new Session();
        }

        byte[] result;
        if (cmd.op() == KvCommand.Op.PUT) {
            store.put(cmd.key(), new Entry(cmd.value(), index));
            result = cmd.value();
        } else { // DELETE
            Entry previous = store.remove(cmd.key());
            result = previous == null ? EMPTY : previous.value;
        }

        session.lastSequence = cmd.sequenceNumber();
        session.fingerprint = cmd.fingerprint();
        session.appliedIndex = index;
        session.lastResult = result;
        touch(cmd.clientId(), session);
        evictOverflow();
        return result;
    }

    /** Moves a session to the most-recent end. Called only from {@link #apply}. */
    private void touch(String clientId, Session session) {
        sessions.remove(clientId);
        sessions.put(clientId, session);
    }

    private void evictOverflow() {
        Iterator<Map.Entry<String, Session>> oldest = sessions.entrySet().iterator();
        while (sessions.size() > maxSessions && oldest.hasNext()) {
            oldest.next();
            oldest.remove();
        }
    }

    /**
     * What became of {@code (clientId, sequence)} for a command with this {@code fingerprint}.
     * The caller passes its own command's fingerprint so a sequence reused by someone else's
     * command cannot be mistaken for its own success.
     */
    public synchronized KvOutcome outcomeFor(String clientId, long sequence, long fingerprint) {
        Session session = sessions.get(clientId);
        if (session == null || session.lastSequence < sequence) {
            return KvOutcome.of(KvOutcome.Status.NOT_APPLIED);
        }
        if (session.lastSequence > sequence) {
            return KvOutcome.of(KvOutcome.Status.STALE_SEQUENCE);
        }
        if (session.fingerprint != fingerprint) {
            return KvOutcome.of(KvOutcome.Status.SEQUENCE_CONFLICT);
        }
        return KvOutcome.of(KvOutcome.Status.APPLIED, session.lastResult, session.appliedIndex);
    }

    /** Leader-side linearizable read off the applied state (null if absent). */
    public synchronized byte[] get(String key) {
        Entry e = store.get(key);
        return e == null ? null : e.value.clone();
    }

    /**
     * The log index of the command that last wrote {@code key}, or 0 if absent. This is the version
     * a read reports: it identifies the write, so it only changes when the value does. The read's
     * own ReadIndex would move on every read and tell a client nothing about the value it got.
     */
    public synchronized long versionOf(String key) {
        Entry e = store.get(key);
        return e == null ? 0L : e.index;
    }

    /** Last applied sequence number for a client (0 if the client has applied nothing). */
    public synchronized long lastAppliedSequence(String clientId) {
        Session s = sessions.get(clientId);
        return s == null ? 0 : s.lastSequence;
    }

    /** The cached apply result for a client's given sequence (null if not that client's last). */
    public synchronized byte[] resultFor(String clientId, long sequence) {
        Session s = sessions.get(clientId);
        if (s == null || s.lastSequence != sequence) {
            return null;
        }
        return s.lastResult == null ? null : s.lastResult.clone();
    }

    /** Sessions currently retained. */
    public synchronized int sessionCount() {
        return sessions.size();
    }

    /**
     * Serialize the store + the client session table deterministically, so every replica produces
     * byte-identical snapshots from identical applied state. Including the sessions is essential:
     * a snapshot that dropped them would let an already-applied client retry re-execute after a
     * restore, breaking at-most-once.
     *
     * <p>The store is written key-sorted, since a map has no inherent order to preserve. The
     * sessions are written in <em>eviction</em> order, not sorted — sorting them would reset every
     * restored replica's LRU order to alphabetical, so which session it drops next would depend on
     * whether it had restarted.
     */
    @Override
    public synchronized byte[] snapshot() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            TreeMap<String, Entry> sortedStore = new TreeMap<>(store);
            out.writeInt(sortedStore.size());
            for (Map.Entry<String, Entry> e : sortedStore.entrySet()) {
                writeString(out, e.getKey());
                out.writeLong(e.getValue().index);
                out.writeInt(e.getValue().value.length);
                out.write(e.getValue().value);
            }
            out.writeInt(sessions.size());
            for (Map.Entry<String, Session> e : sessions.entrySet()) {
                writeString(out, e.getKey());
                out.writeLong(e.getValue().lastSequence);
                out.writeLong(e.getValue().fingerprint);
                out.writeLong(e.getValue().appliedIndex);
                byte[] r = e.getValue().lastResult;
                if (r == null) {
                    out.writeInt(-1);
                } else {
                    out.writeInt(r.length);
                    out.write(r);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot encode failed", e);
        }
        return baos.toByteArray();
    }

    @Override
    public synchronized void restore(byte[] data, long lastIncludedIndex, long lastIncludedTerm) {
        store.clear();
        sessions.clear();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            int storeSize = in.readInt();
            for (int i = 0; i < storeSize; i++) {
                String key = readString(in);
                long index = in.readLong();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                store.put(key, new Entry(value, index));
            }
            int sessionCount = in.readInt();
            for (int i = 0; i < sessionCount; i++) {
                String client = readString(in);
                Session s = new Session();
                s.lastSequence = in.readLong();
                s.fingerprint = in.readLong();
                s.appliedIndex = in.readLong();
                int rlen = in.readInt();
                if (rlen >= 0) {
                    byte[] r = new byte[rlen];
                    in.readFully(r);
                    s.lastResult = r;
                }
                sessions.put(client, s); // read back in the eviction order it was written in
            }
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot decode failed", e);
        }
    }

    /** Capture the current applied state as a {@link Snapshot} at the given log position. */
    public synchronized Snapshot toSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        return Snapshot.of(lastIncludedIndex, lastIncludedTerm, snapshot());
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
