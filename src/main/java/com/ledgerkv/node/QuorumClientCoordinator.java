package com.ledgerkv.node;

import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.transport.ClientCoordinator;
import com.ledgerkv.transport.StoredValue;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * Adapts a {@link LeaderlessKVCluster} (the leaderless quorum coordinator) to the transport-layer
 * {@link ClientCoordinator} seam. The cluster's value currency is UTF-8 {@code String} (a 2c
 * deferral), so request/response bytes are bridged through {@link StandardCharsets#UTF_8}.
 */
public final class QuorumClientCoordinator implements ClientCoordinator {

    private final LeaderlessKVCluster cluster;
    private final int coordinatorIndex;

    public QuorumClientCoordinator(LeaderlessKVCluster cluster, int coordinatorIndex) {
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.coordinatorIndex = coordinatorIndex;
    }

    @Override
    public Optional<StoredValue> get(String key) {
        QuorumResponse response = cluster.read(coordinatorIndex, key);
        if (!response.isSuccessful()) {
            throw new IllegalStateException("read quorum not met for key " + key);
        }
        VersionedValue value = response.getValue();
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(toStored(value));
    }

    @Override
    public StoredValue put(String key, byte[] value) {
        String asString = new String(value, StandardCharsets.UTF_8);
        QuorumResponse response = cluster.write(coordinatorIndex, key, asString);
        if (!response.isSuccessful()) {
            throw new IllegalStateException("write quorum not met for key " + key);
        }
        return toStored(response.getValue());
    }

    private static StoredValue toStored(VersionedValue value) {
        return new StoredValue(
                value.getValue().getBytes(StandardCharsets.UTF_8),
                value.getVersion(),
                false,
                value.getVersionMetadata());
    }
}
