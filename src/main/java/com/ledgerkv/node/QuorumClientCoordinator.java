package com.ledgerkv.node;

import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.metrics.OperationMetrics;
import com.ledgerkv.metrics.OperationMetricsCollector;
import com.ledgerkv.transport.ClientCoordinator;
import com.ledgerkv.transport.StoredValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Adapts a {@link LeaderlessKVCluster} (the leaderless quorum coordinator) to the transport-layer
 * {@link ClientCoordinator} seam. The cluster's value currency is UTF-8 {@code String} (a 2c
 * deferral), so request/response bytes are bridged through {@link StandardCharsets#UTF_8}.
 */
public final class QuorumClientCoordinator implements ClientCoordinator {

    private final LeaderlessKVCluster cluster;
    private final int coordinatorIndex;
    private final OperationMetricsCollector metricsCollector = new OperationMetricsCollector();

    public QuorumClientCoordinator(LeaderlessKVCluster cluster, int coordinatorIndex) {
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.coordinatorIndex = coordinatorIndex;
    }

    /** Live snapshot of this coordinator's quorum metrics (drives the Prometheus exporter). */
    public OperationMetrics operationMetrics() {
        return metricsCollector.snapshot();
    }

    @Override
    public List<StoredValue> get(String key) {
        long hedgesBefore = cluster.hedgedRequestCount();
        QuorumResponse response = cluster.read(coordinatorIndex, key);
        metricsCollector.recordRead(response);
        metricsCollector.recordHedges(cluster.hedgedRequestCount() - hedgesBefore);
        if (!response.isSuccessful()) {
            throw new IllegalStateException("read quorum not met for key " + key);
        }
        List<StoredValue> values = new ArrayList<>();
        if (response.hasConflicts()) {
            // A delete concurrent with a write is a genuine conflict, so tombstones stay in the
            // sibling list here — the caller has to see that a delete is one of the candidates.
            // The key exists with concurrent values. Returning them is what lets the caller tell a
            // conflict from an absence; reporting the null winner as empty made a live key look
            // missing.
            for (VersionedValue sibling : response.getConflictingValues()) {
                values.add(toStored(sibling));
            }
            return values;
        }
        VersionedValue value = response.getValue();
        if (value != null && !value.isDeleted()) {
            // A winning tombstone means the key is deleted: internally it is a version like any
            // other, but to a client it is simply absent.
            values.add(toStored(value));
        }
        return values;
    }

    @Override
    public boolean delete(String key) {
        boolean existed = !get(key).isEmpty();
        long hedgesBefore = cluster.hedgedRequestCount();
        QuorumResponse response = cluster.delete(coordinatorIndex, key);
        metricsCollector.recordWrite(response);
        metricsCollector.recordHedges(cluster.hedgedRequestCount() - hedgesBefore);
        if (!response.isSuccessful()) {
            throw new IllegalStateException("write quorum not met for delete of key " + key);
        }
        return existed;
    }

    @Override
    public StoredValue put(String key, byte[] value) {
        long hedgesBefore = cluster.hedgedRequestCount();
        String asString = new String(value, StandardCharsets.UTF_8);
        QuorumResponse response = cluster.write(coordinatorIndex, key, asString);
        metricsCollector.recordWrite(response);
        metricsCollector.recordHedges(cluster.hedgedRequestCount() - hedgesBefore);
        if (!response.isSuccessful()) {
            throw new IllegalStateException("write quorum not met for key " + key);
        }
        return toStored(response.getValue());
    }

    private static StoredValue toStored(VersionedValue value) {
        return new StoredValue(
                value.getValue().getBytes(StandardCharsets.UTF_8),
                value.getVersion(),
                value.isDeleted(),
                value.getVersionMetadata());
    }
}
