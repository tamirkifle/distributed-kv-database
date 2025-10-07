package com.ledgerkv;

import com.ledgerkv.checker.ConsistencyCheckResult;
import com.ledgerkv.checker.ConsistencyHistoryChecker;
import com.ledgerkv.checker.ConsistencyViolation;
import com.ledgerkv.checker.ConsistencyViolationType;
import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.failure.FailureContext;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistencyHistoryCheckerTest {

    private final ConsistencyHistoryChecker checker = new ConsistencyHistoryChecker();
    private final QuorumConfig weakConfig = new QuorumConfig(3, 1, 1);

    @Test
    void flagsStaleReadsAfterKnownCompletedWrite() {
        OperationHistory history = new OperationHistory(List.of(
            write("profile:42", "v1", "2026-05-15T12:00:00Z", "2026-05-15T12:00:00.010Z"),
            write("profile:42", "v2", "2026-05-15T12:00:01Z", "2026-05-15T12:00:01.010Z"),
            read("profile:42", "v1", "2026-05-15T12:00:02Z", "2026-05-15T12:00:02.010Z")
        ));

        ConsistencyCheckResult result = checker.check(history);

        assertFalse(result.isValid());
        ConsistencyViolation violation = onlyViolationOfType(result, ConsistencyViolationType.STALE_READ);
        assertEquals("profile:42", violation.getKey());
        assertEquals("v2", violation.getExpectedValue());
        assertEquals("v1", violation.getObservedValue());
    }

    @Test
    void flagsReadYourWritesViolationsInSingleClientSession() {
        OperationHistory history = new OperationHistory(List.of(
            write("session:alpha", "token-v1", "2026-05-15T12:00:00Z", "2026-05-15T12:00:00.010Z"),
            read("session:alpha", null, "2026-05-15T12:00:01Z", "2026-05-15T12:00:01.010Z")
        ));

        ConsistencyCheckResult result = checker.check(history);

        assertFalse(result.isValid());
        ConsistencyViolation violation = onlyViolationOfType(result, ConsistencyViolationType.READ_YOUR_WRITES);
        assertEquals("session:alpha", violation.getKey());
        assertEquals("token-v1", violation.getExpectedValue());
        assertNull(violation.getObservedValue());
    }

    @Test
    void flagsMonotonicReadViolationsInSingleClientSession() {
        OperationHistory history = new OperationHistory(List.of(
            write("counter", "v1", "2026-05-15T12:00:00Z", "2026-05-15T12:00:00.010Z"),
            write("counter", "v2", "2026-05-15T12:00:01Z", "2026-05-15T12:00:01.010Z"),
            read("counter", "v2", "2026-05-15T12:00:02Z", "2026-05-15T12:00:02.010Z"),
            read("counter", "v1", "2026-05-15T12:00:03Z", "2026-05-15T12:00:03.010Z")
        ));

        ConsistencyCheckResult result = checker.check(history);

        assertFalse(result.isValid());
        ConsistencyViolation violation = onlyViolationOfType(result, ConsistencyViolationType.MONOTONIC_READ);
        assertEquals("counter", violation.getKey());
        assertEquals("v2", violation.getExpectedValue());
        assertEquals("v1", violation.getObservedValue());
    }

    @Test
    void ignoresFailedOperationsAndConcurrentWrites() {
        OperationHistory history = new OperationHistory(List.of(
            write("k1", "v1", "2026-05-15T12:00:00Z", "2026-05-15T12:00:00.010Z"),
            failedWrite("k1", "v2", "2026-05-15T12:00:01Z", "2026-05-15T12:00:01.010Z"),
            read("k1", "v1", "2026-05-15T12:00:02Z", "2026-05-15T12:00:02.010Z"),
            write("k1", "v3", "2026-05-15T12:00:03Z", "2026-05-15T12:00:04Z"),
            read("k1", "v1", "2026-05-15T12:00:03.500Z", "2026-05-15T12:00:03.510Z")
        ));

        ConsistencyCheckResult result = checker.check(history);

        assertTrue(result.isValid());
        assertTrue(result.getViolations().isEmpty());
    }

    private ConsistencyViolation onlyViolationOfType(ConsistencyCheckResult result, ConsistencyViolationType type) {
        List<ConsistencyViolation> matches = result.getViolationsByType(type);
        assertEquals(1, matches.size(), result.getViolations().toString());
        return matches.get(0);
    }

    private OperationRecord write(String key, String value, String start, String end) {
        return record(OperationType.WRITE, OperationResult.SUCCESS, key, value, start, end);
    }

    private OperationRecord failedWrite(String key, String value, String start, String end) {
        return record(OperationType.WRITE, OperationResult.FAILURE, key, value, start, end);
    }

    private OperationRecord read(String key, String value, String start, String end) {
        return record(OperationType.READ, OperationResult.SUCCESS, key, value, start, end);
    }

    private OperationRecord record(OperationType type,
                                   OperationResult result,
                                   String key,
                                   String value,
                                   String start,
                                   String end) {
        return new OperationRecord(
            type,
            key,
            value,
            Instant.parse(start),
            Instant.parse(end),
            result,
            0,
            weakConfig,
            FailureContext.empty()
        );
    }
}
