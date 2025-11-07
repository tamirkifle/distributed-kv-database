package com.ledgerkv.bench;

/** One workload operation. {@code value} is null for reads/scans; {@code scanLength} is &gt;0 only for scans. */
public final class Operation {

    private final OpType type;
    private final String key;
    private final byte[] value;
    private final int scanLength;

    private Operation(OpType type, String key, byte[] value, int scanLength) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.scanLength = scanLength;
    }

    public static Operation read(String key) {
        return new Operation(OpType.READ, key, null, 0);
    }

    public static Operation update(String key, byte[] value) {
        return new Operation(OpType.UPDATE, key, value, 0);
    }

    public static Operation insert(String key, byte[] value) {
        return new Operation(OpType.INSERT, key, value, 0);
    }

    public static Operation scan(String startKey, int scanLength) {
        return new Operation(OpType.SCAN, startKey, null, scanLength);
    }

    public static Operation readModifyWrite(String key, byte[] value) {
        return new Operation(OpType.READ_MODIFY_WRITE, key, value, 0);
    }

    public OpType type() {
        return type;
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public int scanLength() {
        return scanLength;
    }
}
