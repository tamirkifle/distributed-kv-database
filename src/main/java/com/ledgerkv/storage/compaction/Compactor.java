package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.MergeIterator;
import com.ledgerkv.storage.lsm.SSTableHandle;
import com.ledgerkv.storage.lsm.SSTableWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes compaction tasks: merges the input tables through a {@link MergeIterator} and writes
 * the surviving entries into one or more new SSTables at the task's output level, rolling over to a
 * fresh file once the current one passes {@code maxFileBytes}. Pure execution; the decision of what
 * to compact belongs to a {@link CompactionStrategy}.
 *
 * <p>When wired with a strategy and a {@link CompactionContext}, it can also drive compaction
 * automatically: {@link #runOnce()} plans and applies a single compaction, and {@link #start()}
 * runs that on a background daemon thread.
 */
public class Compactor implements Closeable {

    private final Path outputDir;
    private final long maxFileBytes;
    private final AtomicLong nextId;

    // Background-loop collaborators (null when constructed for one-off compact() use).
    private final CompactionStrategy strategy;
    private final CompactionContext context;
    private final long pollMillis;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile IOException lastError;

    /** Execution-only constructor: use {@link #compact(CompactionTask)} directly. */
    public Compactor(Path outputDir, long maxFileBytes, AtomicLong nextId) {
        this(null, null, outputDir, maxFileBytes, nextId, 0L);
    }

    /** Full constructor wiring a strategy and engine context for {@link #runOnce()} / {@link #start()}. */
    public Compactor(CompactionStrategy strategy, CompactionContext context, Path outputDir,
                     long maxFileBytes, AtomicLong nextId, long pollMillis) {
        this.strategy = strategy;
        this.context = context;
        this.outputDir = outputDir;
        this.maxFileBytes = maxFileBytes;
        this.nextId = nextId;
        this.pollMillis = pollMillis;
    }

    /** Merges {@code task.inputs()} into new SSTable(s) at {@code task.outputLevel()}. */
    public CompactionResult compact(CompactionTask task) throws IOException {
        List<Iterator<Entry>> runs = new ArrayList<>();
        int expected = 0;
        for (SSTableHandle h : task.inputs()) {
            runs.add(h.table().iterator());
            expected += h.table().entryCount();
        }
        MergeIterator merge = new MergeIterator(runs, task.dropTombstones());

        List<SSTableHandle> added = new ArrayList<>();
        SSTableWriter writer = null;
        Path currentPath = null;
        try {
            while (merge.hasNext()) {
                Entry e = merge.next();
                if (writer == null) {
                    currentPath = nextPath();
                    writer = new SSTableWriter(currentPath, Math.max(1, expected));
                }
                writer.add(e);
                if (writer.approximateSizeBytes() >= maxFileBytes) {
                    writer.finish();
                    writer.close();
                    added.add(SSTableHandle.open(currentPath, task.outputLevel()));
                    writer = null;
                }
            }
            if (writer != null) {
                writer.finish();
                writer.close();
                added.add(SSTableHandle.open(currentPath, task.outputLevel()));
                writer = null;
            }
        } finally {
            if (writer != null) {
                writer.close(); // partial output on failure; not added
            }
        }
        return new CompactionResult(added, task.inputs());
    }

    private Path nextPath() {
        return outputDir.resolve(String.format("sst-%010d.db", nextId.getAndIncrement()));
    }

    /**
     * Plans one compaction via the strategy, executes it, applies the resulting swap to the
     * context, then closes and deletes the obsolete input files. Returns false if nothing was
     * planned.
     */
    public boolean runOnce() throws IOException {
        List<SSTableHandle> tables = context.currentTables();
        Optional<CompactionTask> planned = strategy.planCompaction(tables);
        if (!planned.isPresent()) {
            return false;
        }
        CompactionTask task = planned.get();
        CompactionResult result = compact(task);
        context.apply(result);
        for (SSTableHandle obsolete : task.inputs()) {
            // Release the engine's live reference; the channel closes once the last in-flight
            // reader that pinned this handle has unpinned it. The file is unlinked now regardless.
            obsolete.unpin();
            Files.deleteIfExists(obsolete.path());
        }
        return true;
    }

    /** Runs {@link #runOnce()} on a daemon thread, sleeping {@code pollMillis} between idle polls. */
    public void start() {
        if (strategy == null || context == null) {
            throw new IllegalStateException("Compactor was not constructed with a strategy and context");
        }
        if (thread != null) {
            throw new IllegalStateException("already started");
        }
        running = true;
        thread = new Thread(this::loop, "ledgerkv-compactor");
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        while (running) {
            try {
                if (!runOnce()) {
                    Thread.sleep(pollMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                lastError = e;
                try {
                    Thread.sleep(pollMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** The last error caught by the background loop, or null if none. */
    public IOException lastError() {
        return lastError;
    }

    @Override
    public void close() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }
}
