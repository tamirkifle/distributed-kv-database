package com.ledgerkv.storage.wal;

enum RecordType {
    PUT((byte) 1),
    DELETE((byte) 2);

    private final byte code;

    RecordType(byte code) {
        this.code = code;
    }

    byte code() {
        return code;
    }

    static RecordType fromCode(byte code) {
        for (RecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown WAL record type: " + code);
    }
}
