package com.ledgerkv.storage.wal;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalOpaquePayloadTest {

    @Test
    void roundTripsOpaquePayloads(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("opaque.wal");
        try (WriteAheadLog log = new WriteAheadLog(wal, DurabilityMode.SYNC)) {
            log.append(new byte[] {1, 2, 3});
            log.append("hello".getBytes());
            log.append(new byte[0]); // empty payload is valid
        }
        List<byte[]> seen = new ArrayList<>();
        WriteAheadLog.replayBytes(wal, p -> seen.add(p.clone()));
        assertEquals(3, seen.size());
        assertArrayEquals(new byte[] {1, 2, 3}, seen.get(0));
        assertArrayEquals("hello".getBytes(), seen.get(1));
        assertArrayEquals(new byte[0], seen.get(2));
    }

    @Test
    void tornTailPayloadIsTruncatedNotReplayed(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("torn.wal");
        try (WriteAheadLog log = new WriteAheadLog(wal, DurabilityMode.SYNC)) {
            log.append("complete".getBytes());
        }
        // Append a fake partial header (fewer than HEADER_BYTES) at the tail.
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                wal, java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.APPEND)) {
            ch.write(java.nio.ByteBuffer.wrap(new byte[] {0, 0, 0})); // 3 bytes < 8-byte header
        }
        List<byte[]> seen = new ArrayList<>();
        WriteAheadLog.replayBytes(wal, p -> seen.add(p.clone()));
        assertEquals(1, seen.size());
        assertArrayEquals("complete".getBytes(), seen.get(0));
    }
}
