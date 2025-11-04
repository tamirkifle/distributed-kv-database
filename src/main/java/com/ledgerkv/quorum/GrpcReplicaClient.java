package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.transport.NodeClient;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link ReplicaClient} that reaches a remote node over gRPC via {@link NodeClient}'s internal
 * replica RPCs ({@code ReplicaGet}/{@code ReplicaPut}/{@code DeliverHint}). The versioned value is
 * carried on the wire with its full vector clock (see {@code VersionedValueProtos}).
 *
 * <p>Does not own the {@link NodeClient}/server lifecycle: the caller connects and closes them.
 */
public final class GrpcReplicaClient implements ReplicaClient {

    private final String nodeId;
    private final NodeClient client;

    public GrpcReplicaClient(String nodeId, NodeClient client) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        return client.replicaGet(key).map(ReplicaValues::fromStored);
    }

    @Override
    public void put(String key, VersionedValue value) {
        client.replicaPut(key, ReplicaValues.toStored(value));
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        client.deliverHint(nodeId, key, ReplicaValues.toStored(value));
    }
}
