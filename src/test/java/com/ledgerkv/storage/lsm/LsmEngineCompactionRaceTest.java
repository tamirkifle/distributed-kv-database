package com.ledgerkv.storage.lsm;

import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineCompactionRaceTest {

    /**
     * Reproduces the read-vs-compaction channel-close race. A writer thread churns the SSTable set
     * (tiny flush threshold + size-tiered(4) so the background compactor is constantly
     * merging+closing tables) while a reader thread hammers get() on a hot key set that lives in
     * those SSTables. Before the fix the reader throws UncheckedIOException(ClosedChannelException)
     * when the compactor closes a table mid-read; the foreground thread re-raises it. The test
     * passes iff no read raced with a close. (Race reproduction is probabilistic, but with a
     * dedicated reader and heavy compaction it fails essentially every run pre-fix and passes every
     * run post-fix.)
     */
    @Test
    void foregroundReadsSurviveBackgroundCompaction(@TempDir Path dir) throws Exception {
        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.ASYNC, 64L * 1024, new SizeTieredCompaction(4),
                64L * 1024 * 1024, 1L);
        int hotKeys = 4000;
        int writes = 80_000;
        byte[] value = new byte[128];
        AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            for (int i = 0; i < hotKeys; i++) {
                engine.put(String.format("k%08d", i), value);
            }
            engine.flush();   // hot keys now live in SSTables, so reads do real block I/O

            AtomicBoolean done = new AtomicBoolean(false);
            Thread reader = new Thread(() -> {
                Random r = new Random(7);
                try {
                    while (!done.get()) {
                        engine.get(String.format("k%08d", r.nextInt(hotKeys)));
                    }
                } catch (Throwable t) {
                    readerFailure.set(t);
                }
            }, "race-reader");
            reader.start();

            for (int i = 0; i < writes; i++) {
                engine.put(String.format("k%08d", i % (hotKeys * 2)), value);
            }
            done.set(true);
            reader.join();
        }
        if (readerFailure.get() != null) {
            throw new AssertionError("a get() raced with compaction closing an SSTable",
                    readerFailure.get());
        }
    }
}
