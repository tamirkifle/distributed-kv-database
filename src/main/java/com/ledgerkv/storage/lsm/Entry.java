package com.ledgerkv.storage.lsm;

import java.util.Arrays;
import java.util.Objects;

/** An immutable LSM record: a key, an optional value (null marks a tombstone), and a sequence number. */
public final class Entry {

    private final String key;
    private final byte[] value; // null marks a tombstone (a delete)
    private final long sequence;

    private Entry(String key, byte[] value, long sequence) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
        this.sequence = sequence;
    }

    public static Entry put(String key, byte[] value, long sequence) {
        return new Entry(key, Objects.requireNonNull(value, "value"), sequence);
    }

    public static Entry tombstone(String key, long sequence) {
        return new Entry(key, null, sequence);
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public long sequence() {
        return sequence;
    }

    public boolean isTombstone() {
        return value == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Entry)) {
            return false;
        }
        Entry other = (Entry) o;
        return sequence == other.sequence
                && key.equals(other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return (Objects.hash(key, sequence) * 31) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Entry{" + key + ", seq=" + sequence + (isTombstone() ? ", TOMBSTONE}" : "}");
    }
}
