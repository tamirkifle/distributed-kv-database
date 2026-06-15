package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;

import com.ledgerkv.storage.bloom.BloomFilter;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/** An immutable, on-disk sorted run produced by {@link SSTableWriter}. */
public final class SSTable implements Closeable {

    static final int MAGIC = 0x4C534D32; // "LSM2" — footer format v2 adds maxSequence
    static final int FOOTER_BYTES = 36;  // bloomOffset(8) + indexOffset(8) + entryCount(4) + maxSequence(8) + magic(4) + crc(4)
    static final int DEFAULT_BLOCK_SIZE = 4096;
    static final double DEFAULT_FPP = 0.01;
    /** Max data blocks cached per open SSTable. Small fixed bound: a full scan cannot pin the whole file. */
    static final int BLOCK_CACHE_CAPACITY = 32;

    private final FileChannel channel;
    private final BloomFilter bloom;
    private final int entryCount;
    private final long maxSequence;
    private final String[] firstKeys;
    private final long[] blockOffsets;
    private final int[] blockLengths;
    private final Map<Integer, List<Entry>> blockCache = new ConcurrentHashMap<>();
    private final AtomicInteger blocksRead = new AtomicInteger();
    private final AtomicInteger blockCacheHits = new AtomicInteger();

    private SSTable(FileChannel channel, BloomFilter bloom, int entryCount, long maxSequence,
                    String[] firstKeys, long[] blockOffsets, int[] blockLengths) {
        this.channel = channel;
        this.bloom = bloom;
        this.entryCount = entryCount;
        this.maxSequence = maxSequence;
        this.firstKeys = firstKeys;
        this.blockOffsets = blockOffsets;
        this.blockLengths = blockLengths;
    }

    public static SSTable open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, READ);
        boolean ok = false;
        try {
            long size = channel.size();
            if (size < FOOTER_BYTES) {
                throw new SSTableCorruptionException("file too small to be an SSTable: " + path);
            }
            ByteBuffer footer = ByteBuffer.allocate(FOOTER_BYTES);
            readFully(channel, footer, size - FOOTER_BYTES);
            footer.flip();
            long bloomOffset = footer.getLong();
            long indexOffset = footer.getLong();
            int entryCount = footer.getInt();
            long maxSequence = footer.getLong();
            int magic = footer.getInt();
            int crc = footer.getInt();
            if (magic != MAGIC) {
                throw new SSTableCorruptionException("bad SSTable magic in " + path);
            }
            CRC32 fcrc = new CRC32();
            fcrc.update(footer.array(), 0, FOOTER_BYTES - 4);
            if ((int) fcrc.getValue() != crc) {
                throw new SSTableCorruptionException("footer CRC mismatch in " + path);
            }

            int bloomLen = (int) (indexOffset - bloomOffset);
            ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLen);
            readFully(channel, bloomBuf, bloomOffset);
            BloomFilter bloom = BloomFilter.deserialize(bloomBuf.array());

            int indexLen = (int) ((size - FOOTER_BYTES) - indexOffset);
            ByteBuffer indexBuf = ByteBuffer.allocate(indexLen);
            readFully(channel, indexBuf, indexOffset);
            indexBuf.flip();
            int blockCount = indexBuf.getInt();
            String[] firstKeys = new String[blockCount];
            long[] blockOffsets = new long[blockCount];
            int[] blockLengths = new int[blockCount];
            for (int i = 0; i < blockCount; i++) {
                int keyLen = indexBuf.getInt();
                byte[] keyBytes = new byte[keyLen];
                indexBuf.get(keyBytes);
                firstKeys[i] = new String(keyBytes, UTF_8);
                blockOffsets[i] = indexBuf.getLong();
                blockLengths[i] = indexBuf.getInt();
            }

            SSTable table = new SSTable(channel, bloom, entryCount, maxSequence, firstKeys, blockOffsets, blockLengths);
            ok = true;
            return table;
        } finally {
            if (!ok) {
                channel.close();
            }
        }
    }

    public Optional<Entry> get(String key) {
        if (!bloom.mightContain(key)) {
            return Optional.empty();
        }
        int blockIdx = findBlock(key);
        if (blockIdx < 0) {
            return Optional.empty();
        }
        for (Entry e : readBlock(blockIdx)) {
            int cmp = e.key().compareTo(key);
            if (cmp == 0) {
                return Optional.of(e);
            }
            if (cmp > 0) {
                break; // entries are sorted; we have passed where the key would be
            }
        }
        return Optional.empty();
    }

    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {
            private int blockIdx = 0;
            private Iterator<Entry> current = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                while (!current.hasNext() && blockIdx < blockOffsets.length) {
                    current = readBlock(blockIdx++).iterator();
                }
                return current.hasNext();
            }

            @Override
            public Entry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }
        };
    }

    public int entryCount() {
        return entryCount;
    }

    /** Highest {@link Entry#sequence()} stored in this table (0 if empty). Read from the footer in O(1). */
    public long maxSequence() {
        return maxSequence;
    }

    /** The smallest key in this table, or {@code null} if the table is empty. */
    public String firstKey() {
        return firstKeys.length == 0 ? null : firstKeys[0];
    }

    /** The largest key in this table, or {@code null} if empty. */
    public String lastKey() {
        if (firstKeys.length == 0) {
            return null;
        }
        List<Entry> last = readBlock(firstKeys.length - 1);
        return last.get(last.size() - 1).key();
    }

    int blockCount() {
        return firstKeys.length;
    }

    /** Test instrumentation: whether the underlying file channel is still open (ref-count lifecycle). */
    boolean isChannelOpen() {
        return channel.isOpen();
    }

    /** Test instrumentation: total data-block reads from disk since open (for block-skipping assertions). */
    int blocksRead() {
        return blocksRead.get();
    }

    /** Test instrumentation: total block reads served from the in-memory block cache since open. */
    int blockCacheHits() {
        return blockCacheHits.get();
    }

    /**
     * Ascending entries whose key is in {@code [fromInclusive, toExclusive)}. Seeks via the sparse
     * block index to the first block that may contain {@code fromInclusive} (block-skipping: blocks
     * entirely below {@code from} are never read), and stops before any block whose first key is
     * {@code >= toExclusive} (sorted blocks ⇒ no later block can hold an in-range key). {@code null}
     * bounds are open. Tombstones in range pass through verbatim; newest-wins/tombstone resolution
     * is the {@link MergeIterator}'s job.
     */
    public Iterator<Entry> rangeScan(String fromInclusive, String toExclusive) {
        final int startBlock;
        if (fromInclusive == null) {
            startBlock = 0;
        } else {
            int fb = findBlock(fromInclusive);
            startBlock = fb < 0 ? 0 : fb; // from below the table ⇒ start at block 0
        }
        return new Iterator<Entry>() {
            private int blockIdx = startBlock;
            private Iterator<Entry> current = Collections.emptyIterator();
            private Entry next;
            private boolean done;

            {
                advance();
            }

            private void advance() {
                next = null;
                while (true) {
                    while (!current.hasNext()) {
                        if (done || blockIdx >= blockOffsets.length) {
                            return; // no more blocks
                        }
                        // Upper-bound block skip: once a block starts at/after `to`, stop.
                        if (toExclusive != null && firstKeys[blockIdx].compareTo(toExclusive) >= 0) {
                            done = true;
                            return;
                        }
                        current = readBlock(blockIdx++).iterator();
                    }
                    Entry e = current.next();
                    if (fromInclusive != null && e.key().compareTo(fromInclusive) < 0) {
                        continue; // below the lower bound (edge of the start block)
                    }
                    if (toExclusive != null && e.key().compareTo(toExclusive) >= 0) {
                        done = true; // reached the upper bound; ascending ⇒ done
                        return;
                    }
                    next = e;
                    return;
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Entry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Entry result = next;
                advance();
                return result;
            }
        };
    }

    /** Index of the block that may contain {@code key}: the last block whose firstKey <= key. */
    private int findBlock(String key) {
        int lo = 0;
        int hi = firstKeys.length - 1;
        int result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (firstKeys[mid].compareTo(key) <= 0) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    private List<Entry> readBlock(int blockIdx) {
        List<Entry> cached = blockCache.get(blockIdx);
        if (cached != null) {
            blockCacheHits.incrementAndGet();
            return cached;
        }
        List<Entry> decoded = readBlockFromDisk(blockIdx);
        // Bounded: evict an arbitrary existing entry before inserting when at capacity, so a full
        // scan cannot pin every block in memory. Concurrent readers may each decode a missed block
        // once; only one copy is retained (putIfAbsent) — no corruption, no hot-path lock.
        if (blockCache.size() >= BLOCK_CACHE_CAPACITY && !blockCache.containsKey(blockIdx)) {
            Iterator<Integer> it = blockCache.keySet().iterator();
            if (it.hasNext()) {
                it.next();
                it.remove();
            }
        }
        blockCache.putIfAbsent(blockIdx, decoded);
        return decoded;
    }

    private List<Entry> readBlockFromDisk(int blockIdx) {
        blocksRead.incrementAndGet();
        try {
            int length = blockLengths[blockIdx];
            ByteBuffer buf = ByteBuffer.allocate(length);
            readFully(channel, buf, blockOffsets[blockIdx]);
            buf.flip();
            byte[] payload = new byte[length - 4];
            buf.get(payload);
            int storedCrc = buf.getInt();
            CRC32 crc = new CRC32();
            crc.update(payload);
            if ((int) crc.getValue() != storedCrc) {
                throw new SSTableCorruptionException(
                        "data block CRC mismatch at offset " + blockOffsets[blockIdx]);
            }
            return Collections.unmodifiableList(decodeEntries(payload));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<Entry> decodeEntries(byte[] payload) {
        List<Entry> entries = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(payload);
        while (buf.hasRemaining()) {
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            String key = new String(keyBytes, UTF_8);
            int valLen = buf.getInt();
            if (valLen < 0) { // tombstone: no value bytes
                long seq = buf.getLong();
                entries.add(Entry.tombstone(key, seq));
            } else {
                byte[] val = new byte[valLen];
                buf.get(val);
                long seq = buf.getLong();
                entries.add(Entry.put(key, val, seq));
            }
        }
        return entries;
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long pos) throws IOException {
        long p = pos;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, p);
            if (n < 0) {
                throw new EOFException("unexpected EOF reading SSTable at " + p);
            }
            p += n;
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
