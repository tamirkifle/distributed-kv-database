package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import java.util.Objects;
import java.util.Optional;

/**
 * Test double: a partition-injecting decorator over any {@link ReplicaClient}. When unavailable,
 * every call throws — modeling a network partition or dead node over both the in-memory and gRPC
 * transports (the design's "a partition is a ReplicaClient that refuses calls").
 */
final class PartitionableReplicaClient implements ReplicaClient {

    private final ReplicaClient delegate;
    private boolean available = true;

    PartitionableReplicaClient(ReplicaClient delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    void setAvailable(boolean available) {
        this.available = available;
    }

    @Override
    public String nodeId() {
        return delegate.nodeId();
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        requireAvailable();
        return delegate.get(key);
    }

    @Override
    public void put(String key, VersionedValue value) {
        requireAvailable();
        delegate.put(key, value);
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        requireAvailable();
        delegate.deliverHint(key, value);
    }

    private void requireAvailable() {
        if (!available) {
            throw new IllegalStateException("replica unavailable");
        }
    }
}
