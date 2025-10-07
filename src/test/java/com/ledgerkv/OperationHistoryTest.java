package com.ledgerkv;

import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationHistoryRecorder;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.failure.FailureCause;
import com.ledgerkv.failure.FailureContext;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationHistoryTest {

    @Test
    void recorderCapturesWriteOperationDetailsFromQuorumResponse() {
        OperationHistoryRecorder recorder = new OperationHistoryRecorder();
        QuorumConfig config = new QuorumConfig(5, 3, 2);
        FailureContext failureContext = FailureContext.builder()
            .responded("node-0")
            .responded("node-1")
            .responded("node-2")
            .failed("node-3", FailureCause.UNAVAILABLE_NODE)
            .build();
        QuorumResponse response = new QuorumResponse(
            true,
            new VersionedValue("score=0.91", 7),
            List.of(),
            3,
            3,
            12,
            failureContext
        );
        Instant start = Instant.parse("2026-05-15T12:00:00Z");
        Instant end = Instant.parse("2026-05-15T12:00:00.012Z");

        recorder.recordWrite(start, end, 2, config, "trace:candidate", "score=0.91", response);

        OperationRecord record = recorder.snapshot().getOperations().get(0);
        assertEquals(OperationType.WRITE, record.getType());
        assertEquals("trace:candidate", record.getKey());
        assertEquals("score=0.91", record.getValue());
        assertEquals(start, record.getStartTime());
        assertEquals(end, record.getEndTime());
        assertEquals(OperationResult.SUCCESS, record.getResult());
        assertEquals(2, record.getCoordinator());
        assertSame(config, record.getConsistencyConfig());
        assertSame(failureContext, record.getFailureContext());
    }

    @Test
    void recorderCapturesReadResultValueAndFailedQuorumContext() {
        OperationHistoryRecorder recorder = new OperationHistoryRecorder();
        QuorumConfig config = new QuorumConfig(3, 2, 2);
        FailureContext failureContext = FailureContext.builder()
            .responded("node-0")
            .failed("node-1", FailureCause.UNAVAILABLE_NODE)
            .failed("node-2", FailureCause.UNAVAILABLE_NODE)
            .build();
        QuorumResponse response = new QuorumResponse(false, null, List.of(), 1, 2, 9, failureContext);

        recorder.recordRead(
            Instant.parse("2026-05-15T12:01:00Z"),
            Instant.parse("2026-05-15T12:01:00.009Z"),
            1,
            config,
            "trace:baseline",
            response
        );

        OperationRecord record = recorder.snapshot().getOperations().get(0);
        assertEquals(OperationType.READ, record.getType());
        assertEquals("trace:baseline", record.getKey());
        assertNull(record.getValue());
        assertEquals(OperationResult.FAILURE, record.getResult());
        assertEquals(1, record.getCoordinator());
        assertSame(config, record.getConsistencyConfig());
        assertSame(failureContext, record.getFailureContext());
    }

    @Test
    void historySnapshotIsOrderedImmutableAndIndependentFromFutureRecords() {
        OperationHistoryRecorder recorder = new OperationHistoryRecorder();
        QuorumConfig config = new QuorumConfig(3, 1, 1);
        QuorumResponse success = new QuorumResponse(true, new VersionedValue("v1", 1), List.of(), 1, 1, 1);

        recorder.recordWrite(
            Instant.parse("2026-05-15T12:00:00Z"),
            Instant.parse("2026-05-15T12:00:00.001Z"),
            0,
            config,
            "k1",
            "v1",
            success
        );
        OperationHistory snapshot = recorder.snapshot();
        recorder.recordRead(
            Instant.parse("2026-05-15T12:00:01Z"),
            Instant.parse("2026-05-15T12:00:01.001Z"),
            0,
            config,
            "k1",
            success
        );

        assertEquals(1, snapshot.getOperations().size());
        assertEquals("k1", snapshot.getOperations().get(0).getKey());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.getOperations().add(snapshot.getOperations().get(0)));
        assertEquals(2, recorder.snapshot().getOperations().size());
    }

    @Test
    void recordRejectsEndTimeBeforeStartTime() {
        OperationHistoryRecorder recorder = new OperationHistoryRecorder();
        QuorumResponse response = new QuorumResponse(true, new VersionedValue("v1", 1), List.of(), 1, 1, 1);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> recorder.recordRead(
            Instant.parse("2026-05-15T12:00:01Z"),
            Instant.parse("2026-05-15T12:00:00Z"),
            0,
            new QuorumConfig(3, 1, 1),
            "k1",
            response
        ));

        assertTrue(error.getMessage().contains("end time"));
    }
}
