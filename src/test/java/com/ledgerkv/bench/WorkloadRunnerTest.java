package com.ledgerkv.bench;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.storage.lsm.LsmEngineConfig;
import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkloadRunnerTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static LsmEngineConfig noFlushNoCompaction() {
        return new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
    }

    @Test
    void insertThenReadRoundTrips(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            assertEquals(1, WorkloadRunner.execute(engine, Operation.insert("k", b("v"))));
            assertEquals(1, WorkloadRunner.execute(engine, Operation.read("k")));
            assertEquals(0, WorkloadRunner.execute(engine, Operation.read("absent")));
            assertArrayEquals(b("v"), engine.get("k").orElse(null));
        }
    }

    @Test
    void readModifyWriteUpdatesValue(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            WorkloadRunner.execute(engine, Operation.insert("k", b("v1")));
            int touched = WorkloadRunner.execute(engine, Operation.readModifyWrite("k", b("v2")));
            assertEquals(2, touched, "RMW touches the read hit plus the write");
            assertArrayEquals(b("v2"), engine.get("k").orElse(null));
        }
    }

    @Test
    void scanReturnsBoundedCount(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlushNoCompaction())) {
            for (int i = 0; i < 10; i++) {
                WorkloadRunner.execute(engine, Operation.insert(WorkloadGenerator.key(i), b("v")));
            }
            int seen = WorkloadRunner.execute(engine, Operation.scan(WorkloadGenerator.key(0), 3));
            assertEquals(3, seen, "scan must stop at scanLength");
        }
    }
}
