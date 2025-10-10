package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalRecoveryTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void replayTruncatesTornTailWrite(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            wal.append(WalRecord.put("a", b("1")));
            wal.append(WalRecord.put("b", b("2")));
        }
        long goodLen = Files.size(path);

        // Simulate a torn write: a header promising a payload that is not fully present.
        try (FileChannel ch = FileChannel.open(path, WRITE)) {
            ByteBuffer torn = ByteBuffer.allocate(6);
            torn.putInt(9999);          // claims a 9999-byte payload
            torn.putShort((short) 0);   // but only 6 bytes total follow
            torn.flip();
            ch.write(torn, goodLen);
        }

        List<WalRecord> out = new ArrayList<>();
        WriteAheadLog.replay(path, out::add);

        assertEquals(2, out.size());
        assertEquals(goodLen, Files.size(path), "tail should be truncated to last good record");
    }

    @Test
    void replayThrowsOnMidFileCorruption(@TempDir Path dir) throws IOException {
        Path path = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(path, DurabilityMode.SYNC)) {
            wal.append(WalRecord.put("a", b("1")));
            wal.append(WalRecord.put("b", b("2")));
            wal.append(WalRecord.put("c", b("3")));
        }

        // Flip a byte inside the FIRST record's payload (offset 8 = first payload byte).
        try (FileChannel ch = FileChannel.open(path, READ, WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            readAt(ch, one, 8);
            byte v = one.get(0);
            ch.write(ByteBuffer.wrap(new byte[] {(byte) (v ^ 0xFF)}), 8);
        }

        assertThrows(WalCorruptionException.class,
                () -> WriteAheadLog.replay(path, r -> { }));
    }

    private static void readAt(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                break;
            }
            p += n;
        }
        buf.flip();
    }
}
