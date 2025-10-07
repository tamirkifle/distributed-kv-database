package com.ledgerkv.checker;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.failure.FailureContext;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable record of one client-visible operation in a correctness history.
 */
public final class OperationRecord {
    private final OperationType type;
    private final String key;
    private final String value;
    private final Instant startTime;
    private final Instant endTime;
    private final OperationResult result;
    private final int coordinator;
    private final QuorumConfig consistencyConfig;
    private final FailureContext failureContext;

    public OperationRecord(OperationType type,
                           String key,
                           String value,
                           Instant startTime,
                           Instant endTime,
                           OperationResult result,
                           int coordinator,
                           QuorumConfig consistencyConfig,
                           FailureContext failureContext) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.key = requireNonBlank(key, "key");
        this.value = value;
        this.startTime = Objects.requireNonNull(startTime, "start time must not be null");
        this.endTime = Objects.requireNonNull(endTime, "end time must not be null");
        if (endTime.isBefore(startTime)) {
            throw new IllegalArgumentException("end time must not be before start time");
        }
        this.result = Objects.requireNonNull(result, "result must not be null");
        if (coordinator < 0) {
            throw new IllegalArgumentException("coordinator must be non-negative");
        }
        this.coordinator = coordinator;
        this.consistencyConfig = Objects.requireNonNull(consistencyConfig, "consistency config must not be null");
        this.failureContext = Objects.requireNonNull(failureContext, "failure context must not be null");
    }

    public OperationType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public OperationResult getResult() {
        return result;
    }

    public int getCoordinator() {
        return coordinator;
    }

    public QuorumConfig getConsistencyConfig() {
        return consistencyConfig;
    }

    public FailureContext getFailureContext() {
        return failureContext;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return "OperationRecord{"
            + "type=" + type
            + ", key='" + key + '\''
            + ", value='" + value + '\''
            + ", startTime=" + startTime
            + ", endTime=" + endTime
            + ", result=" + result
            + ", coordinator=" + coordinator
            + ", consistencyConfig=" + consistencyConfig
            + ", failureContext=" + failureContext
            + '}';
    }
}
