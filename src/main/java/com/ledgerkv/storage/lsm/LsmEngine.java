package com.ledgerkv.storage.lsm;

import com.ledgerkv.storage.StorageEngine;
import com.ledgerkv.storage.compaction.CompactionContext;
import com.ledgerkv.storage.compaction.CompactionResult;
import com.ledgerkv.storage.compaction.Compactor;
import com.ledgerkv.storage.wal.WalRecord;
import com.ledgerkv.storage.wal.WriteAheadLog;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An embedded LSM key-value engine wiring a {@link WriteAheadLog}, an active {@link MemTable}, a
 * copy-on-write list of {@link SSTableHandle}s, and (when configured) a background {@link Compactor}.
 *
 * <p>Recency is resolved by sequence number: every mutation gets a monotonically increasing
 * sequence, so {@link #get} returns the highest-sequence candidate across the MemTable(s) and
 * SSTables regardless of list order. Reads take no locks — they read volatile snapshots.
 *
 * <p><b>Known limitation:</b> the background compactor deletes obsolete SSTable files after swapping
 * them out, so an in-flight reader holding an older snapshot can race with file reclamation. This
 * narrow race is left for Phase-3 hardening (reference counting); tests avoid it by disabling
 * compaction or quiescing it before reading.
 */
public final class LsmEngine implements StorageEngine, CompactionContext {

    private final Path directory;
    private final LsmEngineConfig config;
    private final WriteAheadLog wal;

    private final Object writeLock = new Object();    // serializes writes and flush
    private final Object sstablesLock = new Object();  // guards copy-on-write SSTable swaps

    private volatile MemTable active = new MemTable();
    private volatile MemTable flushing;                // non-null only mid-flush (set in Task 2)
    private volatile List<SSTableHandle> sstables = Collections.emptyList();

    private long sequence;                             // guarded by writeLock; monotonic entry seq
    private final AtomicLong nextSstableId = new AtomicLong();

    private Compactor compactor;                       // null when config.strategy == null (Task 5)
    private volatile boolean closed;

    private LsmEngine(Path directory, LsmEngineConfig config) throws IOException {
        this.directory = directory;
        this.config = config;
        Files.createDirectories(directory);
        // Recovery (load SSTables + replay WAL) is added in Task 3.
        this.wal = new WriteAheadLog(walPath(directory), config.durability);
        // Background compactor is wired in Task 5.
    }

    public static LsmEngine open(Path directory) throws IOException {
        return open(directory, LsmEngineConfig.defaults());
    }

    public static LsmEngine open(Path directory, LsmEngineConfig config) throws IOException {
        return new LsmEngine(directory, config);
    }

    @Override
    public void put(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        synchronized (writeLock) {
            ensureOpen();
            try {
                wal.append(WalRecord.put(key, value));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            active.put(key, value, ++sequence);
            // flush-on-threshold is added in Task 2.
        }
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key");
        synchronized (writeLock) {
            ensureOpen();
            try {
                wal.append(WalRecord.delete(key));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            active.delete(key, ++sequence);
            // flush-on-threshold is added in Task 2.
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key");
        ensureOpen();
        Entry best = active.get(key);
        MemTable f = flushing;
        if (f != null) {
            best = newer(f.get(key), best);
        }
        for (SSTableHandle h : sstables) {
            best = newer(h.table().get(key).orElse(null), best);
        }
        if (best == null || best.isTombstone()) {
            return Optional.empty();
        }
        return Optional.of(best.value());
    }

    /** The higher-sequence of two candidates (either may be null). */
    private static Entry newer(Entry candidate, Entry current) {
        if (candidate == null) {
            return current;
        }
        if (current == null) {
            return candidate;
        }
        return candidate.sequence() > current.sequence() ? candidate : current;
    }

    // --- CompactionContext (driven by the background Compactor, wired in Task 5) ---

    @Override
    public List<SSTableHandle> currentTables() {
        return new ArrayList<>(sstables);
    }

    @Override
    public void apply(CompactionResult result) {
        synchronized (sstablesLock) {
            List<SSTableHandle> next = new ArrayList<>(sstables);
            next.removeAll(result.obsolete());
            next.addAll(result.added());
            sstables = Collections.unmodifiableList(next);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (compactor != null) {
            compactor.close();
        }
        for (SSTableHandle h : sstables) {
            try {
                h.close();
            } catch (IOException ignore) {
                // best-effort; we still close the WAL below
            }
        }
        wal.close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("engine is closed");
        }
    }

    private static Path walPath(Path dir) {
        return dir.resolve("wal.log");
    }
}
