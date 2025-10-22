package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.MergeIterator;
import com.ledgerkv.storage.lsm.SSTableHandle;
import com.ledgerkv.storage.lsm.SSTableWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes compaction tasks: merges the input tables through a {@link MergeIterator} and writes
 * the surviving entries into one or more new SSTables at the task's output level, rolling over to a
 * fresh file once the current one passes {@code maxFileBytes}. Pure execution; the decision of what
 * to compact belongs to a {@link CompactionStrategy}.
 */
public class Compactor {

    private final Path outputDir;
    private final long maxFileBytes;
    private final AtomicLong nextId;

    public Compactor(Path outputDir, long maxFileBytes, AtomicLong nextId) {
        this.outputDir = outputDir;
        this.maxFileBytes = maxFileBytes;
        this.nextId = nextId;
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
}
