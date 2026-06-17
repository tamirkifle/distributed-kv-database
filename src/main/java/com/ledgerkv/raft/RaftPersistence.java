package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ledgerkv.storage.wal.DurabilityMode;
import com.ledgerkv.storage.wal.WriteAheadLog;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable Raft persistent state (currentTerm, votedFor, log[]) over the Phase-1 {@link WriteAheadLog}.
 * There is exactly one on-disk log format: every record is an opaque WAL payload tagged by a leading
 * kind byte (TERM / VOTE / ENTRY / TRUNCATE). No snapshotting — the WAL grows unbounded (acknowledged
 * Phase-3 scope; the natural next step is log compaction).
 */
public final class RaftPersistence implements Closeable {

    private static final byte TERM = 1;
    private static final byte VOTE = 2;
    private static final byte ENTRY = 3;
    private static final byte TRUNCATE = 4;
    private static final byte SNAPSHOT = 5;

    private final WriteAheadLog wal;

    private RaftPersistence(WriteAheadLog wal) {
        this.wal = wal;
    }

    public static RaftPersistence open(Path dir) throws IOException {
        Files.createDirectories(dir);
        return new RaftPersistence(new WriteAheadLog(dir.resolve("raft.wal"), DurabilityMode.SYNC));
    }

    public void recordTerm(long term, String votedFor) throws IOException {
        byte[] v = votedFor == null ? new byte[0] : votedFor.getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 4 + v.length);
        buf.put(TERM).putLong(term).putInt(v.length).put(v);
        wal.append(buf.array());
    }

    public void recordVote(String votedFor) throws IOException {
        byte[] v = votedFor == null ? new byte[0] : votedFor.getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + v.length);
        buf.put(VOTE).putInt(v.length).put(v);
        wal.append(buf.array());
    }

    public void recordEntry(LogEntry entry) throws IOException {
        byte[] cmd = entry.command();
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 8 + 4 + cmd.length);
        buf.put(ENTRY).putLong(entry.term()).putLong(entry.index()).putInt(cmd.length).put(cmd);
        wal.append(buf.array());
    }

    public void recordTruncate(long fromIndexInclusive) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1 + 8);
        buf.put(TRUNCATE).putLong(fromIndexInclusive);
        wal.append(buf.array());
    }

    /**
     * Append a snapshot record (one on-disk format: opaque WAL payload tagged SNAPSHOT). The leader
     * also truncates its in-memory log prefix; on replay this record sets the recovered base and
     * discards entries it covers.
     */
    public void recordSnapshot(Snapshot snapshot) throws IOException {
        byte[] data = snapshot.data();
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 8 + 4 + data.length);
        buf.put(SNAPSHOT)
                .putLong(snapshot.lastIncludedIndex())
                .putLong(snapshot.lastIncludedTerm())
                .putInt(data.length)
                .put(data);
        wal.append(buf.array());
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }

    public static RaftState replay(Path dir) throws IOException {
        long[] term = {0};
        String[] votedFor = {null};
        List<LogEntry> entries = new ArrayList<>();
        Snapshot[] snapshot = {null};
        // The snapshot base currently in effect. Invariant: entries.get(i).index() == base + i + 1.
        long[] base = {0};
        WriteAheadLog.replayBytes(dir.resolve("raft.wal"), payload -> {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            byte kind = buf.get();
            switch (kind) {
                case TERM: {
                    term[0] = buf.getLong();
                    byte[] v = new byte[buf.getInt()];
                    buf.get(v);
                    votedFor[0] = v.length == 0 ? null : new String(v, UTF_8);
                    break;
                }
                case VOTE: {
                    byte[] v = new byte[buf.getInt()];
                    buf.get(v);
                    votedFor[0] = v.length == 0 ? null : new String(v, UTF_8);
                    break;
                }
                case ENTRY: {
                    long t = buf.getLong();
                    long idx = buf.getLong();
                    byte[] cmd = new byte[buf.getInt()];
                    buf.get(cmd);
                    if (idx <= base[0]) {
                        break; // already covered by a snapshot folded in earlier
                    }
                    int slot = (int) (idx - base[0] - 1);
                    if (slot > entries.size()) {
                        throw new IllegalStateException("raft WAL gap: entry index " + idx
                                + " leaves a hole after base " + base[0]
                                + " with " + entries.size() + " recovered entries");
                    }
                    while (entries.size() > slot) {
                        entries.remove(entries.size() - 1); // overwrite at this absolute index
                    }
                    entries.add(LogEntry.of(t, idx, cmd));
                    break;
                }
                case TRUNCATE: {
                    long from = buf.getLong();
                    // Absolute index -> physical slot. Anything at or below the base is already
                    // gone, so a truncation reaching into the base clears the whole tail.
                    int slot = from <= base[0] + 1 ? 0 : (int) (from - base[0] - 1);
                    while (entries.size() > slot) {
                        entries.remove(entries.size() - 1);
                    }
                    break;
                }
                case SNAPSHOT: {
                    long lastIncludedIndex = buf.getLong();
                    long lastIncludedTerm = buf.getLong();
                    byte[] data = new byte[buf.getInt()];
                    buf.get(data);
                    snapshot[0] = Snapshot.of(lastIncludedIndex, lastIncludedTerm, data);
                    // Drop the contiguous prefix the snapshot now covers. Entries are appended in
                    // ascending index order, so the covered entries form a prefix; counting then
                    // clearing it in one subList().clear() is O(N) total (a single arraycopy)
                    // instead of O(N^2) repeated remove(0).
                    int drop = 0;
                    while (drop < entries.size() && entries.get(drop).index() <= lastIncludedIndex) {
                        drop++;
                    }
                    if (drop > 0) {
                        entries.subList(0, drop).clear();
                    }
                    // Every later ENTRY/TRUNCATE index is absolute, so replay must keep translating
                    // through the new base. Comparing them against the shortened list's size
                    // was the bug.
                    base[0] = lastIncludedIndex;
                    break;
                }
                default:
                    throw new IllegalStateException("unknown raft record kind " + kind);
            }
        });
        return new RaftState(term[0], votedFor[0], entries, snapshot[0]);
    }
}
