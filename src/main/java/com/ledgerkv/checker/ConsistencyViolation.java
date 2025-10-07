package com.ledgerkv.checker;

import java.util.Objects;

/**
 * Immutable description of one consistency violation found in an operation history.
 */
public final class ConsistencyViolation {
    private final ConsistencyViolationType type;
    private final String key;
    private final String expectedValue;
    private final String observedValue;
    private final OperationRecord operation;
    private final String message;

    public ConsistencyViolation(ConsistencyViolationType type,
                                String key,
                                String expectedValue,
                                String observedValue,
                                OperationRecord operation,
                                String message) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.key = requireNonBlank(key, "key");
        this.expectedValue = expectedValue;
        this.observedValue = observedValue;
        this.operation = Objects.requireNonNull(operation, "operation must not be null");
        this.message = requireNonBlank(message, "message");
    }

    public ConsistencyViolationType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getExpectedValue() {
        return expectedValue;
    }

    public String getObservedValue() {
        return observedValue;
    }

    public OperationRecord getOperation() {
        return operation;
    }

    public String getMessage() {
        return message;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return "ConsistencyViolation{"
            + "type=" + type
            + ", key='" + key + '\''
            + ", expectedValue='" + expectedValue + '\''
            + ", observedValue='" + observedValue + '\''
            + ", message='" + message + '\''
            + '}';
    }
}
