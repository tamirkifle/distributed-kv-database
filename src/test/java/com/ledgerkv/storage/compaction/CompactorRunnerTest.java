package com.ledgerkv.storage.compaction;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.SSTableHandle;
import com.ledgerkv.storage.lsm.SSTableWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactorRunnerTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static SSTableHandle table(Path dir, String name, AtomicLong ids, int from, int to)
            throws IOException {
        Path path = dir.resolve(name);
        try (SSTableWriter w = new SSTableWriter(path, Math.max(1, to - from))) {
            for (int i = from; i < to; i++) {
                w.add(Entry.put(String.format("k%06d", i), b("v" + i), i));
            }
            w.finish();
        }
        return SSTableHandle.open(path, 0);
    }

    /** A minimal in-memory context: a synchronized mutable list of the live tables. */
    private static final class ListContext implements CompactionContext {
        private final List<SSTableHandle> tables = new ArrayList<>();

        synchronized void add(SSTableHandle h) {
            tables.add(h);
        }

        @Override
        public synchronized List<SSTableHandle> currentTables() {
            return new ArrayList<>(tables);
        }

        @Override
        public synchronized void apply(CompactionResult result) {
            tables.removeAll(result.obsolete());
            tables.addAll(result.added());
        }

        synchronized int size() {
            return tables.size();
        }
    }

    @Test
    void runOnceExecutesAndSwapsTables(@TempDir Path dir) throws IOException {
        AtomicLong ids = new AtomicLong();
        ListContext ctx = new ListContext();
        for (int i = 0; i < 4; i++) {
            ctx.add(table(dir, "in" + i + ".sst", ids, 0, 10));
        }
        Compactor compactor = new Compactor(
                new SizeTieredCompaction(4), ctx, dir, 1L << 30, ids, 10);

        assertTrue(compactor.runOnce());

        // 4 inputs replaced by 1 merged output.
        assertEquals(1, ctx.size());
        SSTableHandle out = ctx.currentTables().get(0);
        assertEquals(10, out.table().entryCount());
        // Obsolete input files are deleted from disk.
        for (int i = 0; i < 4; i++) {
            assertFalse(Files.exists(dir.resolve("in" + i + ".sst")), "input " + i + " should be deleted");
        }
        // Nothing left to do.
        assertFalse(compactor.runOnce());
    }

    @Test
    void backgroundThreadCompactsThenStops(@TempDir Path dir) throws Exception {
        AtomicLong ids = new AtomicLong();
        ListContext ctx = new ListContext();
        for (int i = 0; i < 4; i++) {
            ctx.add(table(dir, "in" + i + ".sst", ids, 0, 10));
        }
        Compactor compactor = new Compactor(
                new SizeTieredCompaction(4), ctx, dir, 1L << 30, ids, 5);

        compactor.start();
        // Deterministic bounded wait: poll until the swap is observed (or time out and fail).
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (ctx.size() != 1 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        compactor.close();

        assertEquals(1, ctx.size(), "background compactor should have merged the four tables");
        assertNull(compactor.lastError());
    }
}
