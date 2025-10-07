package com.ledgerkv.checker;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Mutable recorder that produces immutable correctness history snapshots.
 */
public final class OperationHistoryRecorder {
    private final Object lock = new Object();
    private final List<OperationRecord> operations = new ArrayList<>();

    public void recordRead(Instant startTime,
                           Instant endTime,
                           int coordinator,
                           QuorumConfig consistencyConfig,
                           String key,
                           QuorumResponse response) {
        Objects.requireNonNull(response, "response must not be null");
        VersionedValue responseValue = response.getValue();
        String value = response.isSuccessful() && responseValue != null ? responseValue.getValue() : null;
        record(OperationType.READ, startTime, endTime, coordinator, consistencyConfig, key, value, response);
    }

    public void recordWrite(Instant startTime,
                            Instant endTime,
                            int coordinator,
                            QuorumConfig consistencyConfig,
                            String key,
                            String value,
                            QuorumResponse response) {
        Objects.requireNonNull(value, "value must not be null");
        record(OperationType.WRITE, startTime, endTime, coordinator, consistencyConfig, key, value, response);
    }

    public OperationHistory snapshot() {
        synchronized (lock) {
            return new OperationHistory(operations);
        }
    }

    private void record(OperationType type,
                        Instant startTime,
                        Instant endTime,
                        int coordinator,
                        QuorumConfig consistencyConfig,
                        String key,
                        String value,
                        QuorumResponse response) {
        Objects.requireNonNull(response, "response must not be null");
        OperationRecord record = new OperationRecord(
            type,
            key,
            value,
            startTime,
            endTime,
            response.isSuccessful() ? OperationResult.SUCCESS : OperationResult.FAILURE,
            coordinator,
            consistencyConfig,
            response.getFailureContext()
        );
        synchronized (lock) {
            operations.add(record);
        }
    }
}
