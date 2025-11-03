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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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

        long maxId = loadExistingSstables();        // opens handles into `sstables`
        nextSstableId.set(maxId + 1);
        this.sequence = maxSequenceInSstables();     // high-water so WAL entries outrank SSTables
        WriteAheadLog.replay(walPath(directory), this::replayRecord);

        this.wal = new WriteAheadLog(walPath(directory), config.durability);
        if (config.strategy != null) {
            this.compactor = new Compactor(config.strategy, this, directory,
                    config.maxSstableBytes, nextSstableId, config.compactionPollMillis);
            this.compactor.start();
        }
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
            maybeFlush();
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
            maybeFlush();
        }
    }

    /** Flushes the active MemTable once it crosses the configured byte threshold. Caller holds writeLock. */
    private void maybeFlush() {
        if (active.approximateSizeBytes() >= config.memtableFlushBytes) {
            flushLocked();
        }
    }

    /** Forces a flush of the active MemTable (package-private test seam). */
    void flush() {
        synchronized (writeLock) {
            flushLocked();
        }
    }

    /** Number of live SSTables (package-private test seam). */
    int sstableCount() {
        return sstables.size();
    }

    /** The background compactor's last error, or null (package-private test seam). */
    IOException compactionError() {
        return compactor == null ? null : compactor.lastError();
    }

    /**
     * Seals the active MemTable, writes it to a new SSTable, swaps the SSTable in (copy-on-write),
     * then truncates the WAL. Caller holds writeLock, so no concurrent write can append to the WAL
     * between seal and truncate. Reads never block: they see the sealed table via {@code flushing}
     * until its SSTable is durable.
     */
    private void flushLocked() {
        MemTable sealed = active;
        if (sealed.isEmpty()) {
            return;
        }
        sealed.seal();
        flushing = sealed;          // readers can still find these keys
        active = new MemTable();
        try {
            List<Entry> entries = new ArrayList<>(sealed.entries());
            Path path = directory.resolve(sstFileName(nextSstableId.getAndIncrement()));
            try (SSTableWriter writer = new SSTableWriter(path, entries.size())) {
                for (Entry e : entries) {
                    writer.add(e);
                }
                writer.finish();
            }
            SSTableHandle handle = SSTableHandle.open(path, 0);
            synchronized (sstablesLock) {
                List<SSTableHandle> next = new ArrayList<>(sstables);
                next.add(handle);
                sstables = Collections.unmodifiableList(next);
            }
            flushing = null;        // now durable in the SSTable
            wal.truncate();         // all sealed data is in the SSTable; the WAL may be reset
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** SSTable filename, matching the format the Compactor uses so ids never collide. */
    private static String sstFileName(long id) {
        return String.format("sst-%010d.db", id);
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

    @Override
    public Iterator<Entry> scan(String fromInclusive, String toExclusive) {
        ensureOpen();
        List<Iterator<Entry>> runs = new ArrayList<>();
        runs.add(active.entries().iterator());
        MemTable f = flushing;
        if (f != null) {
            runs.add(f.entries().iterator());
        }
        for (SSTableHandle h : sstables) {
            runs.add(h.table().iterator());
        }
        // dropTombstones=true: the merge resolves newest-wins, so a winning tombstone means the
        // key is deleted and is correctly omitted from the live scan view.
        MergeIterator merged = new MergeIterator(runs, true);
        return new BoundedIterator(merged, fromInclusive, toExclusive);
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

    /** Opens every {@code sst-*.db} file in the directory at level 0; returns the max file id (or -1). */
    private long loadExistingSstables() throws IOException {
        List<SSTableHandle> loaded = new ArrayList<>();
        long maxId = -1L;
        List<Path> files = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> ds =
                     Files.newDirectoryStream(directory, "sst-*.db")) {
            for (Path p : ds) {
                files.add(p);
            }
        }
        files.sort(java.util.Comparator.comparing(p -> p.getFileName().toString()));
        for (Path p : files) {
            loaded.add(SSTableHandle.open(p, 0));
            maxId = Math.max(maxId, parseSstableId(p));
        }
        sstables = Collections.unmodifiableList(loaded);
        return maxId;
    }

    /** Highest entry sequence across all loaded SSTables (0 if none). Documented O(entries) scan. */
    private long maxSequenceInSstables() {
        long max = 0L;
        for (SSTableHandle h : sstables) {
            java.util.Iterator<Entry> it = h.table().iterator();
            while (it.hasNext()) {
                long s = it.next().sequence();
                if (s > max) {
                    max = s;
                }
            }
        }
        return max;
    }

    /** Applies one replayed WAL record to the active MemTable with a fresh (newer) sequence. */
    private void replayRecord(WalRecord record) {
        if (record.value() == null) {          // null value marks a DELETE
            active.delete(record.key(), ++sequence);
        } else {
            active.put(record.key(), record.value(), ++sequence);
        }
    }

    /** Parses the 10-digit id from {@code sst-0000000007.db}. */
    private static long parseSstableId(Path p) {
        String name = p.getFileName().toString();   // sst-##########.db
        return Long.parseLong(name.substring(4, name.length() - 3));
    }

    /**
     * Yields the ascending entries of {@code source} whose key is in {@code [from, to)}. Because the
     * source is ascending, it terminates at the first key &gt;= {@code to}. {@code null} bounds are open.
     */
    private static final class BoundedIterator implements Iterator<Entry> {
        private final Iterator<Entry> source;
        private final String from;
        private final String to;
        private Entry next;

        BoundedIterator(Iterator<Entry> source, String from, String to) {
            this.source = source;
            this.from = from;
            this.to = to;
            advance();
        }

        private void advance() {
            next = null;
            while (source.hasNext()) {
                Entry e = source.next();
                if (from != null && e.key().compareTo(from) < 0) {
                    continue;                       // below the lower bound; keep scanning
                }
                if (to != null && e.key().compareTo(to) >= 0) {
                    return;                         // reached the upper bound; ascending ⇒ done
                }
                next = e;
                return;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            Entry result = next;
            advance();
            return result;
        }
    }
}
