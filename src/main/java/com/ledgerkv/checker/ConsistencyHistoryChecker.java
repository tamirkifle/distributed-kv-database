package com.ledgerkv.checker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Checks single-client operation histories for stale reads and session guarantees.
 */
public final class ConsistencyHistoryChecker {

    public ConsistencyCheckResult check(OperationHistory history) {
        Objects.requireNonNull(history, "history must not be null");
        List<OperationRecord> operations = history.getOperations();
        List<ConsistencyViolation> violations = new ArrayList<>();
        Map<String, Map<String, Integer>> valueRanksByKey = buildValueRanks(operations);
        Map<String, Integer> highestReadRankByKey = new HashMap<>();

        for (int i = 0; i < operations.size(); i++) {
            OperationRecord operation = operations.get(i);
            if (!isSuccessfulRead(operation)) {
                continue;
            }

            OperationRecord latestCompletedWrite = latestCompletedWriteBefore(operations, operation, operations.size());
            if (latestCompletedWrite != null && !Objects.equals(latestCompletedWrite.getValue(), operation.getValue())) {
                violations.add(violation(
                    ConsistencyViolationType.STALE_READ,
                    operation,
                    latestCompletedWrite.getValue(),
                    "read did not observe the latest completed write"
                ));
            }

            OperationRecord latestSessionWrite = latestCompletedWriteBefore(operations, operation, i);
            if (latestSessionWrite != null && !Objects.equals(latestSessionWrite.getValue(), operation.getValue())) {
                violations.add(violation(
                    ConsistencyViolationType.READ_YOUR_WRITES,
                    operation,
                    latestSessionWrite.getValue(),
                    "read did not observe the client's latest completed write"
                ));
            }

            Integer observedRank = rankFor(valueRanksByKey, operation.getKey(), operation.getValue());
            Integer highestReadRank = highestReadRankByKey.get(operation.getKey());
            if (observedRank != null) {
                if (highestReadRank != null && observedRank < highestReadRank) {
                    String expectedValue = valueForRank(valueRanksByKey, operation.getKey(), highestReadRank);
                    violations.add(violation(
                        ConsistencyViolationType.MONOTONIC_READ,
                        operation,
                        expectedValue,
                        "read observed an older value than an earlier read in the same session"
                    ));
                } else if (highestReadRank == null || observedRank > highestReadRank) {
                    highestReadRankByKey.put(operation.getKey(), observedRank);
                }
            }
        }

        return new ConsistencyCheckResult(violations);
    }

    private Map<String, Map<String, Integer>> buildValueRanks(List<OperationRecord> operations) {
        Map<String, Map<String, Integer>> ranksByKey = new HashMap<>();
        for (OperationRecord operation : operations) {
            if (!isSuccessfulWrite(operation)) {
                continue;
            }
            Map<String, Integer> ranks = ranksByKey.computeIfAbsent(operation.getKey(), ignored -> new HashMap<>());
            ranks.putIfAbsent(operation.getValue(), ranks.size());
        }
        return ranksByKey;
    }

    private OperationRecord latestCompletedWriteBefore(List<OperationRecord> operations,
                                                       OperationRecord read,
                                                       int exclusiveEndIndex) {
        OperationRecord latest = null;
        for (int i = 0; i < exclusiveEndIndex; i++) {
            OperationRecord candidate = operations.get(i);
            if (!isSuccessfulWrite(candidate) || !candidate.getKey().equals(read.getKey())) {
                continue;
            }
            if (candidate.getEndTime().isAfter(read.getStartTime())) {
                continue;
            }
            if (latest == null || candidate.getEndTime().isAfter(latest.getEndTime())) {
                latest = candidate;
            }
        }
        return latest;
    }

    private Integer rankFor(Map<String, Map<String, Integer>> valueRanksByKey, String key, String value) {
        Map<String, Integer> ranks = valueRanksByKey.get(key);
        if (ranks == null) {
            return null;
        }
        return ranks.get(value);
    }

    private String valueForRank(Map<String, Map<String, Integer>> valueRanksByKey, String key, int rank) {
        Map<String, Integer> ranks = valueRanksByKey.get(key);
        if (ranks == null) {
            return null;
        }
        for (Map.Entry<String, Integer> entry : ranks.entrySet()) {
            if (entry.getValue() == rank) {
                return entry.getKey();
            }
        }
        return null;
    }

    private boolean isSuccessfulRead(OperationRecord operation) {
        return operation.getType() == OperationType.READ && operation.getResult() == OperationResult.SUCCESS;
    }

    private boolean isSuccessfulWrite(OperationRecord operation) {
        return operation.getType() == OperationType.WRITE && operation.getResult() == OperationResult.SUCCESS;
    }

    private ConsistencyViolation violation(ConsistencyViolationType type,
                                           OperationRecord operation,
                                           String expectedValue,
                                           String message) {
        return new ConsistencyViolation(
            type,
            operation.getKey(),
            expectedValue,
            operation.getValue(),
            operation,
            message
        );
    }
}
