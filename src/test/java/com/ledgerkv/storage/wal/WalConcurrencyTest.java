package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalConcurrencyTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void asyncAppendsAreDurableAfterSync(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.ASYNC)) {
            wal.append(WalRecord.put("a", b("1")));
            wal.append(WalRecord.put("b", b("2")));
            wal.sync();
        }

        List<WalRecord> out = new ArrayList<>();
        WriteAheadLog.replay(path, out::add);
        assertEquals(2, out.size());
    }

    @Test
    void concurrentAppendsAreAllDurable(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("wal.log");
        int threads = 8;
        int perThread = 500;

        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        try {
                            wal.append(WalRecord.put(tid + ":" + i, b("x")));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();
        }

        AtomicInteger count = new AtomicInteger();
        WriteAheadLog.replay(path, r -> count.incrementAndGet());
        assertEquals(threads * perThread, count.get());
    }
}
