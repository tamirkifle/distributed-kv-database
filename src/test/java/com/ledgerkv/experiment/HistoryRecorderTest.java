package com.ledgerkv.experiment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.checker.LinearizabilityChecker;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.transport.NotLeaderException;
import com.ledgerkv.transport.UnknownOutcomeException;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

/**
 * Whether a failed operation is recorded as definitely-not-happened or as unknown.
 *
 * <p>This is the hinge of every correctness experiment. Record a timed-out write as a failure and
 * the checker will later find a read of a value nothing wrote, and report a violation that never
 * occurred; the run looks like it found a bug in the database when it found one in the harness.
 */
class HistoryRecorderTest {

    @Test
    void aClientDeadlineThatExpiredMidFlightIsUnknown() {
        assertEquals(OperationResult.UNKNOWN,
                HistoryRecorder.classify(new UnknownOutcomeException("c", 1, "deadline", null)));
    }

    @Test
    void aLostAnswerIsUnknownBecauseTheWriteMayHaveCommitted() {
        for (Status status : new Status[] {
                Status.DEADLINE_EXCEEDED, Status.UNAVAILABLE, Status.CANCELLED}) {
            assertEquals(OperationResult.UNKNOWN,
                    HistoryRecorder.classify(status.asRuntimeException()),
                    status.getCode() + " leaves the outcome unknown");
        }
    }

    @Test
    void aRefusalBeforeProposingIsADefiniteFailure() {
        assertEquals(OperationResult.FAILURE,
                HistoryRecorder.classify(new NotLeaderException("n1", "host:9090")),
                "a node that redirected never put the command in any log");
        assertEquals(OperationResult.FAILURE,
                HistoryRecorder.classify(Status.INVALID_ARGUMENT.asRuntimeException()),
                "a rejected request never reached the state machine");
    }

    @Test
    void anUnrecognizedFailureIsUnknownRatherThanFailed() {
        // Guessing "unknown" only widens the orders the checker accepts, so it can hide a
        // violation. Guessing "failure" can manufacture one. Prefer the direction that cannot lie.
        assertEquals(OperationResult.UNKNOWN,
                HistoryRecorder.classify(new RuntimeException("something new")));
        assertEquals(OperationResult.UNKNOWN,
                HistoryRecorder.classify(Status.INTERNAL.asRuntimeException()));
    }

    @Test
    void recordsSuccessfulReadsAndWritesWithTheirValues() {
        HistoryRecorder recorder = new HistoryRecorder();
        recorder.recordWrite("k", "v1", () -> { });
        assertEquals("v1", recorder.recordRead("k", () -> "v1"));

        assertEquals(2, recorder.size());
        assertEquals(2, recorder.counts().success);
        assertTrue(new LinearizabilityChecker().check(recorder.history()).isLinearizable());
    }

    @Test
    void aTimedOutWriteIsRecordedSoTheCheckerCanStillExplainALaterRead() {
        HistoryRecorder recorder = new HistoryRecorder();
        recorder.recordWrite("k", "v1", () -> {
            throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        });
        recorder.recordRead("k", () -> "v1"); // it had committed after all

        assertEquals(1, recorder.counts().unknown);
        assertTrue(new LinearizabilityChecker().check(recorder.history()).isLinearizable(),
                "the unknown write explains the read; calling it a failure would not");
    }

    @Test
    void aFailedReadObservedNothingAndConstrainsNothing() {
        HistoryRecorder recorder = new HistoryRecorder();
        recorder.recordWrite("k", "v1", () -> { });
        assertNull(recorder.recordRead("k", () -> {
            throw Status.UNAVAILABLE.asRuntimeException();
        }));

        assertEquals(1, recorder.counts().failure);
        assertTrue(new LinearizabilityChecker().check(recorder.history()).isLinearizable());
    }

    @Test
    void historyForOneKeyExcludesTheRest() {
        HistoryRecorder recorder = new HistoryRecorder();
        recorder.recordWrite("a", "1", () -> { });
        recorder.recordWrite("b", "2", () -> { });

        assertEquals(1, recorder.historyFor("a").getOperations().size());
        assertEquals("a", recorder.historyFor("a").getOperations().get(0).getKey());
    }
}
