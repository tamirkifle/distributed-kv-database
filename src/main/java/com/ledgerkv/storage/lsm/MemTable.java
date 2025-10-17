package com.ledgerkv.storage.lsm;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory, sorted, thread-safe write buffer backed by a {@link ConcurrentSkipListMap}.
 * Tracks an approximate byte size; once sealed it becomes immutable and ready to flush to an SSTable.
 */
public final class MemTable {

    private final ConcurrentSkipListMap<String, Entry> map = new ConcurrentSkipListMap<>();
    private final AtomicLong approximateBytes = new AtomicLong();
    private volatile boolean sealed;

    public void put(String key, byte[] value, long sequence) {
        apply(Entry.put(key, value, sequence));
    }

    public void delete(String key, long sequence) {
        apply(Entry.tombstone(key, sequence));
    }

    private void apply(Entry entry) {
        if (sealed) {
            throw new IllegalStateException("MemTable is sealed and immutable");
        }
        Entry previous = map.put(entry.key(), entry);
        approximateBytes.addAndGet(sizeOf(entry) - (previous == null ? 0L : sizeOf(previous)));
    }

    /** The current entry for {@code key} (which may be a tombstone), or {@code null} if absent. */
    public Entry get(String key) {
        return map.get(key);
    }

    public long approximateSizeBytes() {
        return approximateBytes.get();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    /** Marks the MemTable immutable. Subsequent {@code put}/{@code delete} calls throw. */
    public void seal() {
        sealed = true;
    }

    public boolean isSealed() {
        return sealed;
    }

    /** Entries in ascending key order — the order an SSTable flush consumes them. */
    public Collection<Entry> entries() {
        return map.values();
    }

    private static long sizeOf(Entry entry) {
        long valueBytes = entry.isTombstone() ? 0L : entry.value().length;
        // Rough heap estimate: key chars (~2 bytes each) + value bytes + per-entry object overhead.
        return (entry.key().length() * 2L) + valueBytes + 48L;
    }
}
