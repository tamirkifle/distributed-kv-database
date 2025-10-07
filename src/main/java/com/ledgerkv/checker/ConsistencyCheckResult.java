package com.ledgerkv.checker;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable result of checking an operation history.
 */
public final class ConsistencyCheckResult {
    private final List<ConsistencyViolation> violations;

    public ConsistencyCheckResult(List<ConsistencyViolation> violations) {
        Objects.requireNonNull(violations, "violations must not be null");
        for (ConsistencyViolation violation : violations) {
            if (violation == null) {
                throw new IllegalArgumentException("violations cannot contain null values");
            }
        }
        this.violations = List.copyOf(violations);
    }

    public boolean isValid() {
        return violations.isEmpty();
    }

    public List<ConsistencyViolation> getViolations() {
        return violations;
    }

    public List<ConsistencyViolation> getViolationsByType(ConsistencyViolationType type) {
        Objects.requireNonNull(type, "type must not be null");
        return violations.stream()
            .filter(violation -> violation.getType() == type)
            .collect(Collectors.toUnmodifiableList());
    }
}
