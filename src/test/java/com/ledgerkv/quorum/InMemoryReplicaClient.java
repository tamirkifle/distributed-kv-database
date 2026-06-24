package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionConflictResolver;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Test double: an in-memory {@link ReplicaClient} backing the quorum-logic unit tests. Performs the
 * same causal merge a real replica does — a causally older or duplicate arrival is dropped and
 * concurrent values are kept as siblings — so the unit tests exercise the production model rather
 * than a more forgiving one. (The LSM-backed
 * and gRPC transports are covered by {@code LocalReplicaClientTest}, {@code GrpcReplicaClientTest},
 * and the N-node integration test.)
 */
public final class InMemoryReplicaClient implements ReplicaClient {

    private final String nodeId;
    private final Map<String, List<VersionedValue>> data = new LinkedHashMap<>();

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
    public synchronized List<VersionedValue> get(String key) {
        return List.copyOf(data.getOrDefault(key, List.of()));
    }

    @Override
    public synchronized void put(String key, VersionedValue value) {
        List<VersionedValue> candidates = new ArrayList<>(data.getOrDefault(key, List.of()));
        candidates.add(value);
        data.put(key, VersionConflictResolver.causalFrontier(candidates));
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        put(key, value);
    }
}
