package com.ledgerkv.checker;

import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot of recorded operations in execution order.
 */
public final class OperationHistory {
    private final List<OperationRecord> operations;

    public OperationHistory(List<OperationRecord> operations) {
        Objects.requireNonNull(operations, "operations must not be null");
        for (OperationRecord operation : operations) {
            if (operation == null) {
                throw new IllegalArgumentException("operations cannot contain null records");
            }
        }
        this.operations = List.copyOf(operations);
    }

    public static OperationHistory empty() {
        return new OperationHistory(List.of());
    }

    public List<OperationRecord> getOperations() {
        return operations;
    }
}
