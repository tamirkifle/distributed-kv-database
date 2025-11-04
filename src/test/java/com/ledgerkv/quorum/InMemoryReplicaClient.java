package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Test double: an in-memory {@link ReplicaClient} backing the quorum-logic unit tests. Stores the
 * supplied {@link VersionedValue} verbatim, exactly as the coordinator pushes it. (The LSM-backed
 * and gRPC transports are covered by {@code LocalReplicaClientTest}, {@code GrpcReplicaClientTest},
 * and the N-node integration test.)
 */
public final class InMemoryReplicaClient implements ReplicaClient {

    private final String nodeId;
    private final Map<String, VersionedValue> data = new LinkedHashMap<>();

    public InMemoryReplicaClient(String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    /** A fresh in-memory client per membership node, keyed by node id in membership order. */
    public static Map<String, ReplicaClient> clusterFor(ClusterMembership membership) {
        Map<String, ReplicaClient> clients = new LinkedHashMap<>();
        for (ClusterNode node : membership.getNodes()) {
            clients.put(node.getId(), new InMemoryReplicaClient(node.getId()));
        }
        return clients;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public void put(String key, VersionedValue value) {
        data.put(key, value);
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        data.put(key, value);
    }
}
