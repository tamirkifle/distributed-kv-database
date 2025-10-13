package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalCheckpointTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void truncateDiscardsAllRecordsThenAcceptsNewOnes(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            wal.append(WalRecord.put("a", b("1")));
            wal.truncate();
            wal.append(WalRecord.put("b", b("2")));
        }

        List<WalRecord> out = new ArrayList<>();
        WriteAheadLog.replay(path, out::add);

        assertEquals(1, out.size());
        assertEquals(WalRecord.put("b", b("2")), out.get(0));
    }
}
