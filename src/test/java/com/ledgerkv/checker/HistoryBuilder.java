package com.ledgerkv.checker;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.failure.FailureContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only fluent builder for {@link OperationHistory}. Hides the {@link OperationRecord}
 * constructor boilerplate (a non-null {@link QuorumConfig} + {@link FailureContext} + start/end
 * {@link Instant}s) so checker tests read as histories. Ticks are milliseconds from a fixed epoch;
 * an op whose end-tick is strictly less than another op's start-tick is real-time-before it.
 */
final class HistoryBuilder {

    private static final Instant EPOCH = Instant.parse("2026-06-29T00:00:00Z");
    private static final QuorumConfig CONFIG = new QuorumConfig(3, 2, 2);
    private static final FailureContext NO_FAILURE = FailureContext.empty();
    private static final int COORDINATOR = 0;

    private final List<OperationRecord> operations = new ArrayList<>();

    HistoryBuilder write(String key, String value, long startTick, long endTick) {
        operations.add(record(OperationType.WRITE, key, value, startTick, endTick, OperationResult.SUCCESS));
        return this;
    }

    HistoryBuilder read(String key, String value, long startTick, long endTick) {
        operations.add(record(OperationType.READ, key, value, startTick, endTick, OperationResult.SUCCESS));
        return this;
    }

    HistoryBuilder failedRead(String key, long startTick, long endTick) {
        operations.add(record(OperationType.READ, key, null, startTick, endTick, OperationResult.FAILURE));
        return this;
    }

    HistoryBuilder failedWrite(String key, String value, long startTick, long endTick) {
        operations.add(record(OperationType.WRITE, key, value, startTick, endTick, OperationResult.FAILURE));
        return this;
    }

    /** A write whose client timed out: it may have taken effect, then or later, or never. */
    HistoryBuilder unknownWrite(String key, String value, long startTick, long endTick) {
        operations.add(record(OperationType.WRITE, key, value, startTick, endTick, OperationResult.UNKNOWN));
        return this;
    }

    /** A read whose client timed out holding no value. */
    HistoryBuilder unknownRead(String key, long startTick, long endTick) {
        operations.add(record(OperationType.READ, key, null, startTick, endTick, OperationResult.UNKNOWN));
        return this;
    }

    OperationHistory build() {
        return new OperationHistory(operations);
    }

    private static OperationRecord record(OperationType type,
                                          String key,
                                          String value,
                                          long startTick,
                                          long endTick,
                                          OperationResult result) {
        return new OperationRecord(
            type,
            key,
            value,
            EPOCH.plusMillis(startTick),
            EPOCH.plusMillis(endTick),
            result,
            COORDINATOR,
            CONFIG,
            NO_FAILURE
        );
    }
}
