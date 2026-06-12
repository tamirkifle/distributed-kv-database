package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.ledgerkv.storage.bloom.BloomFilter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Builds an immutable SSTable file from entries supplied in strictly increasing key order:
 * ~fixed-size data blocks (each CRC32-checked), a serialized Bloom filter block, a sparse index
 * (one entry per block), and a fixed 28-byte footer.
 */
public final class SSTableWriter implements Closeable {

    private final FileChannel channel;
    private final int blockSize;
    private final BloomFilter bloom;

    private final ByteArrayOutputStream blockBuf = new ByteArrayOutputStream();
    private String blockFirstKey;
    private final List<String> indexKeys = new ArrayList<>();
    private final List<Long> indexOffsets = new ArrayList<>();
    private final List<Integer> indexLengths = new ArrayList<>();

    private long fileOffset;
    private int entryCount;
    private long maxSequence;
    private String lastKey;
    private boolean finished;

    public SSTableWriter(Path path, int expectedEntries) throws IOException {
        this(path, expectedEntries, SSTable.DEFAULT_BLOCK_SIZE);
    }

    public SSTableWriter(Path path, int expectedEntries, int blockSize) throws IOException {
        this.channel = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING);
        this.blockSize = blockSize;
        this.bloom = BloomFilter.create(Math.max(1, expectedEntries), SSTable.DEFAULT_FPP);
    }

    /** Adds the next entry. Keys MUST be strictly increasing (the MemTable guarantees this). */
    public void add(Entry entry) throws IOException {
        if (finished) {
            throw new IllegalStateException("writer already finished");
        }
        if (lastKey != null && entry.key().compareTo(lastKey) <= 0) {
            throw new IllegalArgumentException(
                    "entries must be added in strictly increasing key order: " + lastKey + " -> " + entry.key());
        }
        lastKey = entry.key();
        bloom.add(entry.key());

        byte[] record = encodeEntry(entry);
        if (blockBuf.size() == 0) {
            blockFirstKey = entry.key();
        }
        blockBuf.write(record, 0, record.length);
        entryCount++;
        if (entry.sequence() > maxSequence) {
            maxSequence = entry.sequence();
        }

        if (blockBuf.size() >= blockSize) {
            flushBlock();
        }
    }

    public void finish() throws IOException {
        if (finished) {
            return;
        }
        flushBlock();

        // Bloom filter block.
        long bloomOffset = fileOffset;
        byte[] bloomBytes = bloom.serialize();
        writeFully(ByteBuffer.wrap(bloomBytes), bloomOffset);
        fileOffset += bloomBytes.length;

        // Sparse index block: [blockCount:int] then per block [keyLen:int][key][offset:long][length:int].
        long indexOffset = fileOffset;
        ByteArrayOutputStream idx = new ByteArrayOutputStream();
        idx.write(intBytes(indexKeys.size()), 0, 4);
        for (int i = 0; i < indexKeys.size(); i++) {
            byte[] keyBytes = indexKeys.get(i).getBytes(UTF_8);
            ByteBuffer e = ByteBuffer.allocate(4 + keyBytes.length + 8 + 4);
            e.putInt(keyBytes.length);
            e.put(keyBytes);
            e.putLong(indexOffsets.get(i));
            e.putInt(indexLengths.get(i));
            idx.write(e.array(), 0, e.array().length);
        }
        byte[] indexBytes = idx.toByteArray();
        writeFully(ByteBuffer.wrap(indexBytes), indexOffset);
        fileOffset += indexBytes.length;

        // Footer: [bloomOffset:long][indexOffset:long][entryCount:int][maxSequence:long][magic:int][crc:int].
        ByteBuffer footer = ByteBuffer.allocate(SSTable.FOOTER_BYTES);
        footer.putLong(bloomOffset);
        footer.putLong(indexOffset);
        footer.putInt(entryCount);
        footer.putLong(maxSequence);
        footer.putInt(SSTable.MAGIC);
        CRC32 fcrc = new CRC32();
        fcrc.update(footer.array(), 0, SSTable.FOOTER_BYTES - 4);
        footer.putInt((int) fcrc.getValue());
        footer.flip();
        writeFully(footer, fileOffset);
        fileOffset += SSTable.FOOTER_BYTES;

        channel.force(true);
        finished = true;
    }

    private static byte[] encodeEntry(Entry entry) {
        byte[] keyBytes = entry.key().getBytes(UTF_8);
        byte[] valBytes = entry.isTombstone() ? new byte[0] : entry.value();
        int valLen = entry.isTombstone() ? -1 : valBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length + 8);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valLen);
        buf.put(valBytes);
        buf.putLong(entry.sequence());
        return buf.array();
    }

    private void flushBlock() throws IOException {
        if (blockBuf.size() == 0) {
            return;
        }
        byte[] payload = blockBuf.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(payload);

        ByteBuffer buf = ByteBuffer.allocate(payload.length + 4);
        buf.put(payload);
        buf.putInt((int) crc.getValue());
        buf.flip();

        long offset = fileOffset;
        writeFully(buf, offset);
        int length = payload.length + 4;

        indexKeys.add(blockFirstKey);
        indexOffsets.add(offset);
        indexLengths.add(length);

        fileOffset += length;
        blockBuf.reset();
        blockFirstKey = null;
    }

    private void writeFully(ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            p += channel.write(buf, p);
        }
    }

    private static byte[] intBytes(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    /**
     * Approximate bytes written so far: flushed file offset plus the entries buffered in the
     * current (not-yet-flushed) data block. Used by the compactor to roll over to a new output
     * file once it grows past a target size.
     */
    public long approximateSizeBytes() {
        return fileOffset + blockBuf.size();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!finished) {
                finish();
            }
        } finally {
            channel.close();
        }
    }
}
