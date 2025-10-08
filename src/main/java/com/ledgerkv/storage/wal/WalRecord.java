package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class WalRecord {

    private final RecordType type;
    private final String key;
    private final byte[] value; // null for DELETE

    private WalRecord(RecordType type, String key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static WalRecord put(String key, byte[] value) {
        return new WalRecord(RecordType.PUT, key, Objects.requireNonNull(value, "value"));
    }

    public static WalRecord delete(String key) {
        return new WalRecord(RecordType.DELETE, key, null);
    }

    public RecordType type() {
        return type;
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public byte[] encode() {
        byte[] keyBytes = key.getBytes(UTF_8);
        byte[] valBytes = type == RecordType.PUT ? value : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + valBytes.length);
        buf.put(type.code());
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valBytes.length);
        buf.put(valBytes);
        return buf.array();
    }

    public static WalRecord decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        RecordType type = RecordType.fromCode(buf.get());
        byte[] key = new byte[buf.getInt()];
        buf.get(key);
        byte[] val = new byte[buf.getInt()];
        buf.get(val);
        if (type == RecordType.DELETE) {
            return delete(new String(key, UTF_8));
        }
        return put(new String(key, UTF_8), val);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WalRecord)) {
            return false;
        }
        WalRecord other = (WalRecord) o;
        return type == other.type
                && key.equals(other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key) * 31 + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "WalRecord{" + type + ", key=" + key + "}";
    }
}
