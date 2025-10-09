package com.ledgerkv.storage.wal;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.zip.CRC32;

public final class WriteAheadLog implements Closeable {

    static final int HEADER_BYTES = 8;
    static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private final FileChannel channel;
    private final DurabilityMode mode;
    private final Object writeLock = new Object();
    private final Object fsyncLock = new Object();

    private long writeOffset;
    private long writeSeq;
    private long syncedSeq;

    public WriteAheadLog(Path path, DurabilityMode mode) throws IOException {
        this.channel = FileChannel.open(path, CREATE, READ, WRITE);
        this.writeOffset = channel.size();
        this.mode = mode;
    }

    public long append(WalRecord record) throws IOException {
        byte[] payload = record.encode();
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + payload.length);
        buf.putInt(payload.length);
        CRC32 crc = new CRC32();
        crc.update(payload);
        buf.putInt((int) crc.getValue());
        buf.put(payload);
        buf.flip();

        long seq;
        synchronized (writeLock) {
            long p = writeOffset;
            while (buf.hasRemaining()) {
                p += channel.write(buf, p);
            }
            writeOffset = p;
            seq = ++writeSeq;
        }
        if (mode == DurabilityMode.SYNC) {
            groupCommit(seq);
        }
        return seq;
    }

    private void groupCommit(long mySeq) throws IOException {
        synchronized (fsyncLock) {
            if (syncedSeq >= mySeq) {
                return; // another thread already fsync'd past my write
            }
            channel.force(false);
            synchronized (writeLock) {
                syncedSeq = writeSeq;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            channel.force(true);
        } finally {
            channel.close();
        }
    }

    public static void replay(Path path, Consumer<WalRecord> consumer) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (FileChannel ch = FileChannel.open(path, READ, WRITE)) {
            long size = ch.size();
            long offset = 0;
            while (offset < size) {
                ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
                readFully(ch, header, offset);
                header.flip();
                int len = header.getInt();
                int crc = header.getInt();

                byte[] payload = new byte[len];
                ByteBuffer pbuf = ByteBuffer.wrap(payload);
                readFully(ch, pbuf, offset + HEADER_BYTES);

                CRC32 c = new CRC32();
                c.update(payload);
                if ((int) c.getValue() != crc) {
                    throw new WalCorruptionException("WAL CRC mismatch at offset " + offset);
                }
                consumer.accept(WalRecord.decode(payload));
                offset += HEADER_BYTES + len;
            }
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                throw new EOFException("Unexpected EOF reading WAL at " + p);
            }
            p += n;
        }
    }
}
