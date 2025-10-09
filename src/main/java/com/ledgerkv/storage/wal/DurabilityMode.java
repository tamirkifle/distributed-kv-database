package com.ledgerkv.storage.wal;

public enum DurabilityMode {
    /** Each batch of appends is fsync'd before the caller returns. */
    SYNC,
    /** Appends return after the buffered write; fsync happens on close/sync(). */
    ASYNC
}
