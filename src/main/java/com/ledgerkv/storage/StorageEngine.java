package com.ledgerkv.storage;

import com.ledgerkv.storage.lsm.Entry;
import java.io.Closeable;
import java.util.Optional;

/**
 * An embedded, persistent key-value storage engine. Keys are {@code String}, ordered
 * lexicographically; values are opaque {@code byte[]}. Versioning and conflict resolution live
 * above the engine. The read/write methods declare no checked exception: I/O failures surface as
 * {@link java.io.UncheckedIOException} (or the more specific
 * {@link com.ledgerkv.storage.lsm.SSTableCorruptionException}).
 */
public interface StorageEngine extends Closeable {

    /** Stores {@code value} under {@code key}, overwriting any previous value. */
    void put(String key, byte[] value);

    /** The current value for {@code key}, or empty if absent or deleted. */
    Optional<byte[]> get(String key);

    /** Removes {@code key} by writing a tombstone. */
    void delete(String key);

    /**
     * An ordered iterator over the live entries with {@code fromInclusive <= key < toExclusive}.
     * A {@code null} bound is open-ended. Tombstoned keys are omitted. The returned iterator pins
     * the SSTables it reads; callers that do not drain it fully MUST {@link CloseableIterator#close()}
     * it (try-with-resources) so those tables can be reclaimed.
     */
    CloseableIterator<Entry> scan(String fromInclusive, String toExclusive);
}
