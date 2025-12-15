package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.InMemoryReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;
import com.ledgerkv.metrics.OperationMetrics;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class QuorumClientCoordinatorMetricsTest {

    private LeaderlessKVCluster cluster(int nodes, int n, int w, int r) {
        ClusterMembership membership = ClusterMembership.create("ledgerkv", nodes, n);
        Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        for (int i = 0; i < nodes; i++) {
            String nodeId = "ledgerkv-node-" + i;
            replicas.put(nodeId, new InMemoryReplicaClient(nodeId));
        }
        return LeaderlessKVCluster.create(membership, new QuorumConfig(n, w, r), replicas);
    }

    @Test
    void recordsReadsAndWrites() {
        QuorumClientCoordinator coordinator =
            new QuorumClientCoordinator(cluster(3, 3, 2, 2), 0);

        coordinator.put("k", "v".getBytes(StandardCharsets.UTF_8));
        coordinator.get("k");

        OperationMetrics metrics = coordinator.operationMetrics();
        assertEquals(2, metrics.getOperationCount());
        assertEquals(1, metrics.getReadCount());
        assertEquals(1, metrics.getWriteCount());
        assertEquals(2, metrics.getSuccessCount());
    }

    @Test
    void recordsQuorumFailureBeforeThrowing() {
        // W=3 but only 1 reachable replica forces a write quorum failure.
        ClusterMembership membership = ClusterMembership.create("ledgerkv", 3, 3);
        Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        replicas.put("ledgerkv-node-0", new InMemoryReplicaClient("ledgerkv-node-0"));
        replicas.put("ledgerkv-node-1", failing("ledgerkv-node-1"));
        replicas.put("ledgerkv-node-2", failing("ledgerkv-node-2"));
        LeaderlessKVCluster cluster =
            LeaderlessKVCluster.create(membership, new QuorumConfig(3, 3, 2), replicas);
        QuorumClientCoordinator coordinator = new QuorumClientCoordinator(cluster, 0);

        assertThrows(IllegalStateException.class,
            () -> coordinator.put("k", "v".getBytes(StandardCharsets.UTF_8)));

        OperationMetrics metrics = coordinator.operationMetrics();
        assertEquals(1, metrics.getWriteCount());
        assertEquals(1, metrics.getFailureCount());
        assertEquals(1, metrics.getQuorumFailureCount());
    }

    @Test
    void recordsHedgedRequestCountFromCluster() {
        ClusterMembership membership = ClusterMembership.create("ledgerkv", 4, 3);
        Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) {
            String nodeId = "ledgerkv-node-" + i;
            replicas.put(nodeId, new InMemoryReplicaClient(nodeId));
        }
        // Block one primary for the chosen key so a hedge to the 4th node is needed for W=3.
        String key = "k";
        java.util.List<com.ledgerkv.quorum.ClusterNode> prefs =
            membership.getPreferenceList(key, 4);
        String slowId = prefs.get(2).getId();
        com.ledgerkv.quorum.LatchControlledReplicaClient slow =
            new com.ledgerkv.quorum.LatchControlledReplicaClient(replicas.get(slowId));
        replicas.put(slowId, slow);

        java.util.concurrent.ExecutorService pool =
            java.util.concurrent.Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 3, 2), replicas, pool,
            java.time.Duration.ofSeconds(2), java.time.Duration.ofMillis(20));
        QuorumClientCoordinator coordinator = new QuorumClientCoordinator(cluster, 0);

        coordinator.put(key, "v".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, coordinator.operationMetrics().getHedgedRequestCount());
        slow.release();
        pool.shutdownNow();
    }

    private static ReplicaClient failing(String nodeId) {
        return new ReplicaClient() {
            @Override
            public String nodeId() {
                return nodeId;
            }

            @Override
            public Optional<VersionedValue> get(String key) {
                throw new RuntimeException("down");
            }

            @Override
            public void put(String key, VersionedValue value) {
                throw new RuntimeException("down");
            }

            @Override
            public void deliverHint(String key, VersionedValue value) {
                throw new RuntimeException("down");
            }
        };
    }
}
