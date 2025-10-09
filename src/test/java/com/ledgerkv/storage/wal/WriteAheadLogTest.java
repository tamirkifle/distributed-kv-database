package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void appendThenReplayReturnsRecordsInOrder(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            wal.append(WalRecord.put("a", b("1")));
            wal.append(WalRecord.put("b", b("2")));
            wal.append(WalRecord.delete("a"));
        }

        List<WalRecord> out = new ArrayList<>();
        WriteAheadLog.replay(path, out::add);

        assertEquals(3, out.size());
        assertEquals(WalRecord.put("a", b("1")), out.get(0));
        assertEquals(WalRecord.put("b", b("2")), out.get(1));
        assertEquals(WalRecord.delete("a"), out.get(2));
    }

    @Test
    void replayOfMissingFileYieldsNothing(@TempDir Path dir) throws IOException {
        List<WalRecord> out = new ArrayList<>();
        WriteAheadLog.replay(dir.resolve("absent.log"), out::add);
        assertTrue(out.isEmpty());
    }

    @Test
    void appendReturnsMonotonicSequenceNumbers(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            assertEquals(1L, wal.append(WalRecord.put("a", b("1"))));
            assertEquals(2L, wal.append(WalRecord.put("b", b("2"))));
        }
    }
}
