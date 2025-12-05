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

    @Override
    public void close() throws IOException {
        wal.close();
    }

    public static RaftState replay(Path dir) throws IOException {
        long[] term = {0};
        String[] votedFor = {null};
        List<LogEntry> entries = new ArrayList<>();
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
                    while (entries.size() >= idx) {
                        entries.remove(entries.size() - 1); // overwrite at this index
                    }
                    entries.add(LogEntry.of(t, idx, cmd));
                    break;
                }
                case TRUNCATE: {
                    long from = buf.getLong();
                    while (entries.size() >= from) {
                        entries.remove(entries.size() - 1);
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("unknown raft record kind " + kind);
            }
        });
        return new RaftState(term[0], votedFor[0], entries);
    }
}
