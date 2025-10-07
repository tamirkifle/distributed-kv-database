package com.ledgerkv.failure;

public enum FailureCause {
    UNAVAILABLE_NODE,
    DROPPED_MESSAGE,
    PARTITION_BLOCKED,
    UNKNOWN
}
