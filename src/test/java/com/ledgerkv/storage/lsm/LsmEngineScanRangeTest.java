package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.CloseableIterator;
import com.ledgerkv.storage.compaction.SizeTieredCompaction;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineScanRangeTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static List<String> keysOf(CloseableIterator<Entry> it) {
        List<String> keys = new ArrayList<>();
        try (CloseableIterator<Entry> c = it) {
            while (c.hasNext()) {
                keys.add(c.next().key());
            }
        }
        return keys;
    }

    @Test
    void boundedScanMergesAcrossMemtableAndSstables(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            // Flush a batch to one SSTable, then another, then leave some in the MemTable.
            for (int i = 0; i < 30; i++) {
                engine.put(String.format("key%04d", i), b("old-" + i));
            }
            engine.flush();
            // Overwrite a few (newest-wins) and delete one, into a second SSTable.
            engine.put("key0010", b("new-10"));
            engine.delete("key0011");
            engine.flush();
            // Some live only in the MemTable.
            for (int i = 30; i < 40; i++) {
                engine.put(String.format("key%04d", i), b("mem-" + i));
            }

            List<String> got = keysOf(engine.scan("key0008", "key0014"));
            // key0011 deleted ⇒ absent; bounds inclusive-from/exclusive-to.
            assertEquals(List.of("key0008", "key0009", "key0010", "key0012", "key0013"), got);

            // Newest-wins for the overwritten key.
            try (CloseableIterator<Entry> it = engine.scan("key0010", "key0011")) {
                assertTrue(it.hasNext());
                Entry e = it.next();
                assertEquals("key0010", e.key());
                assertArrayEquals(b("new-10"), e.value());
                assertFalse(it.hasNext());
            }
        }
    }

    @Test
    void boundedScanOpenAndEmptyBounds(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir)) {
            for (int i = 0; i < 20; i++) {
                engine.put(String.format("key%04d", i), b("v" + i));
            }
            engine.flush();
            assertEquals(20, keysOf(engine.scan(null, null)).size());
            assertEquals(List.of("key0000", "key0001", "key0002"),
                    keysOf(engine.scan(null, "key0003")));
            assertTrue(keysOf(engine.scan("key0005", "key0005")).isEmpty());
        }
    }

    @Test
    void boundedScanIsRaceSafeAgainstCompaction(@TempDir Path dir) throws Exception {
        // Tiny flush threshold + size-tiered(4) so the background compactor churns tables.
        LsmEngineConfig cfg = new LsmEngineConfig(
                DurabilityMode.ASYNC, 64L * 1024, new SizeTieredCompaction(4),
                64L * 1024 * 1024, 1L);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (LsmEngine engine = LsmEngine.open(dir, cfg)) {
            // Seed the range we will scan.
            for (int i = 0; i < 2000; i++) {
                engine.put(String.format("key%04d", i), b("seed-" + i));
            }
            engine.flush();

            Thread writer = new Thread(() -> {
                try {
                    int n = 2000;
                    while (!stop.get()) {
                        engine.put(String.format("key%04d", n % 4000), b("churn-" + n));
                        n++;
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            writer.start();

            try {
                for (int r = 0; r < 300 && failure.get() == null; r++) {
                    List<String> keys = keysOf(engine.scan("key0500", "key1500"));
                    for (String k : keys) {
                        assertTrue(k.compareTo("key0500") >= 0 && k.compareTo("key1500") < 0,
                                "out-of-range key from bounded scan: " + k);
                    }
                }
            } finally {
                stop.set(true);
                writer.join();
            }
            if (failure.get() != null) {
                throw new AssertionError("writer thread failed", failure.get());
            }
        }
    }
}
