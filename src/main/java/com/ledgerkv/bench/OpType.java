package com.ledgerkv.bench;

/** The kinds of operations a YCSB-shaped workload issues. */
public enum OpType {
    READ,
    UPDATE,
    INSERT,
    SCAN,
    READ_MODIFY_WRITE
}
