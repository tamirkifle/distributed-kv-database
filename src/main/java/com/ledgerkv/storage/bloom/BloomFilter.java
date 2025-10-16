package com.ledgerkv.storage.bloom;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

/**
 * A hand-written Bloom filter: a bit array with {@code k} hash functions derived by double-hashing
 * a single 64-bit hash (fmix64-mixed FNV-1a). No false negatives; the false-positive rate stays
 * near the configured bound. Serializable for inclusion in an SSTable. No external dependencies.
 */
public final class BloomFilter {

    private final int numBits;
    private final int numHashes;
    private final long[] words;

    private BloomFilter(int numBits, int numHashes, long[] words) {
        this.numBits = numBits;
        this.numHashes = numHashes;
        this.words = words;
    }

    /** Sizes the filter from an expected entry count and a target false-positive rate. */
    public static BloomFilter create(int expectedEntries, double falsePositiveRate) {
        int n = Math.max(1, expectedEntries);
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            throw new IllegalArgumentException("falsePositiveRate must be in (0,1): " + falsePositiveRate);
        }
        double ln2 = Math.log(2);
        int m = (int) Math.ceil(-n * Math.log(falsePositiveRate) / (ln2 * ln2));
        if (m < 1) {
            m = 1;
        }
        int k = Math.max(1, (int) Math.round((double) m / n * ln2));
        int numWords = (m + 63) / 64;
        return new BloomFilter(m, k, new long[numWords]);
    }

    public void add(String key) {
        long hash = hash64(key);
        int h1 = (int) (hash >>> 32);
        int h2 = (int) hash;
        if (h2 == 0) {
            h2 = 1; // keep the step non-zero so the k probes differ
        }
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h1, h2, i);
            words[bit >>> 6] |= (1L << (bit & 63));
        }
    }

    public boolean mightContain(String key) {
        long hash = hash64(key);
        int h1 = (int) (hash >>> 32);
        int h2 = (int) hash;
        if (h2 == 0) {
            h2 = 1;
        }
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h1, h2, i);
            if ((words[bit >>> 6] & (1L << (bit & 63))) == 0) {
                return false;
            }
        }
        return true;
    }

    private int bitIndex(int h1, int h2, int i) {
        long combined = (h1 + (long) i * h2) & 0x7fffffffffffffffL; // force non-negative
        return (int) (combined % numBits);
    }

    public int numBits() {
        return numBits;
    }

    public int numHashes() {
        return numHashes;
    }

    /** Layout: [numBits:int][numHashes:int][numWords:int][words: numWords * long]. */
    public byte[] serialize() {
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 4 + words.length * 8);
        buf.putInt(numBits);
        buf.putInt(numHashes);
        buf.putInt(words.length);
        for (long w : words) {
            buf.putLong(w);
        }
        return buf.array();
    }

    public static BloomFilter deserialize(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int numBits = buf.getInt();
        int numHashes = buf.getInt();
        int numWords = buf.getInt();
        long[] words = new long[numWords];
        for (int i = 0; i < numWords; i++) {
            words[i] = buf.getLong();
        }
        return new BloomFilter(numBits, numHashes, words);
    }

    private static long hash64(String key) {
        byte[] bytes = key.getBytes(UTF_8);
        long h = 0xcbf29ce484222325L; // FNV-1a 64-bit offset basis
        for (byte b : bytes) {
            h ^= (b & 0xff);
            h *= 0x100000001b3L; // FNV-1a 64-bit prime
        }
        return fmix64(h); // avalanche so the high/low 32 bits are independent enough to double-hash
    }

    private static long fmix64(long h) {
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }
}
