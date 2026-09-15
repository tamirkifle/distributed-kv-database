package com.ledgerkv.experiment;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.failure.FailureContext;
import com.ledgerkv.transport.NotLeaderException;
import com.ledgerkv.transport.UnknownOutcomeException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Records what a client actually observed, so a history can be checked afterwards.
 *
 * <p>The whole value of this class is the classification in {@link #classify}. A history is only
 * as good as its treatment of the operations that did not return cleanly, and the tempting
 * simplification — anything that threw is a failure — produces histories that accuse a correct
 * system of losing writes. A write that times out is still in a leader's log.
 *
 * <p>Timestamps bracket the call: {@code startTime} is taken before the request goes out and
 * {@code endTime} when control returns. For an {@link OperationResult#UNKNOWN} write the checker
 * ignores the end time anyway, since the command may apply long after the client stopped waiting.
 */
public final class HistoryRecorder {

    /** Present only because {@link OperationRecord} demands one; Raft has no N/R/W to report. */
    private static final QuorumConfig RAFT_PLACEHOLDER = new QuorumConfig(3, 2, 2);

    private final List<OperationRecord> operations = new ArrayList<>();
    private final int coordinator;

    public HistoryRecorder() {
        this(0);
    }

    public HistoryRecorder(int coordinator) {
        this.coordinator = coordinator;
    }

    /**
     * Runs a read and records what it observed. A read that fails observed nothing, so it places
     * no constraint either way and is recorded as a definite failure.
     */
    public String recordRead(String key, Callable<String> read) {
        Instant start = Instant.now();
        try {
            String observed = read.call();
            append(OperationType.READ, key, observed, start, OperationResult.SUCCESS);
            return observed;
        } catch (Exception e) {
            append(OperationType.READ, key, null, start, OperationResult.FAILURE);
            return null;
        }
    }

    /** Runs a write and records it, classifying anything it threw. */
    public boolean recordWrite(String key, String value, Runnable write) {
        Instant start = Instant.now();
        try {
            write.run();
            append(OperationType.WRITE, key, value, start, OperationResult.SUCCESS);
            return true;
        } catch (RuntimeException e) {
            append(OperationType.WRITE, key, value, start, classify(e));
            return false;
        }
    }

    /**
     * What a thrown exception says about whether the write took effect.
     *
     * <ul>
     *   <li>{@link UnknownOutcomeException} — the client's own deadline expired mid-flight.
     *   <li>{@code DEADLINE_EXCEEDED}, {@code UNAVAILABLE}, {@code CANCELLED} — the answer was
     *       lost, not the write. A leader may have committed the entry and died before replying.
     *   <li>{@link NotLeaderException} — the node refused before proposing, so nothing entered any
     *       log. Definite.
     *   <li>{@code INVALID_ARGUMENT} — the request was rejected on its merits. Definite.
     * </ul>
     *
     * <p>Anything unrecognized is UNKNOWN. That is the conservative direction: an unknown op only
     * widens the set of orders the checker will accept, so a misclassification here can hide a
     * violation but cannot invent one. Calling it a failure risks the opposite.
     */
    static OperationResult classify(RuntimeException e) {
        if (e instanceof UnknownOutcomeException) {
            return OperationResult.UNKNOWN;
        }
        if (e instanceof NotLeaderException) {
            return OperationResult.FAILURE;
        }
        if (e instanceof StatusRuntimeException) {
            Status.Code code = ((StatusRuntimeException) e).getStatus().getCode();
            if (code == Status.Code.INVALID_ARGUMENT || code == Status.Code.UNIMPLEMENTED) {
                return OperationResult.FAILURE;
            }
            return OperationResult.UNKNOWN;
        }
        return OperationResult.UNKNOWN;
    }

    private synchronized void append(OperationType type, String key, String value,
            Instant start, OperationResult result) {
        operations.add(new OperationRecord(type, key, value, start, Instant.now(), result,
                coordinator, RAFT_PLACEHOLDER, FailureContext.empty()));
    }

    public synchronized OperationHistory history() {
        return new OperationHistory(new ArrayList<>(operations));
    }

    public synchronized int size() {
        return operations.size();
    }

    /**
     * The slowest operation recorded, in milliseconds. Evidence that an injected fault actually
     * bit: if every operation in a leader-loss history finished as fast as a healthy one, the
     * fault landed outside the history and the scenario measured nothing.
     */
    public synchronized long slowestMillis() {
        long slowest = 0;
        for (OperationRecord op : operations) {
            slowest = Math.max(slowest,
                    java.time.Duration.between(op.getStartTime(), op.getEndTime()).toMillis());
        }
        return slowest;
    }

    /** Counts of each outcome, for the run report. */
    public synchronized Counts counts() {
        int success = 0;
        int failure = 0;
        int unknown = 0;
        for (OperationRecord op : operations) {
            switch (op.getResult()) {
                case SUCCESS:
                    success++;
                    break;
                case FAILURE:
                    failure++;
                    break;
                default:
                    unknown++;
            }
        }
        return new Counts(success, failure, unknown);
    }

    /** Immutable tally of observed outcomes. */
    public static final class Counts {
        public final int success;
        public final int failure;
        public final int unknown;

        Counts(int success, int failure, int unknown) {
            this.success = success;
            this.failure = failure;
            this.unknown = unknown;
        }

        public int total() {
            return success + failure + unknown;
        }

        @Override
        public String toString() {
            return "ok=" + success + " failed=" + failure + " unknown=" + unknown;
        }
    }

    /** A history recorded against {@code key} only, for per-key bounded checking. */
    public synchronized OperationHistory historyFor(String key) {
        Objects.requireNonNull(key, "key");
        List<OperationRecord> subset = new ArrayList<>();
        for (OperationRecord op : operations) {
            if (op.getKey().equals(key)) {
                subset.add(op);
            }
        }
        return new OperationHistory(subset);
    }
}
