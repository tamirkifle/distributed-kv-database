package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.ConflictResolutionPolicy;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConflictResolutionTest {

    @Test
    void readReturnsConflictingSiblingsForConcurrentReplicaValues() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:run-010";
        String node0 = membership.getNodes().get(0).getId();
        String node1 = membership.getNodes().get(1).getId();

        clients.get(node0).put(key, new VersionedValue("score=0.81", 1, VersionMetadata.initial(node0)));
        clients.get(node1).put(key, new VersionedValue("score=0.86", 1, VersionMetadata.initial(node1)));
        clients.get(membership.getNodes().get(2).getId())
            .put(key, new VersionedValue("score=0.81", 1, VersionMetadata.initial(node0)));
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 3),
            clients
        );

        QuorumResponse response = cluster.read(0, key);

        assertTrue(response.isSuccessful());
        assertTrue(response.hasConflicts());
        assertNull(response.getValue());
        assertEquals(2, response.getConflictingValues().size());
        assertEquals(List.of("score=0.81", "score=0.86"), values(response.getConflictingValues()));
        assertEquals(1, response.getConflictingValues().get(0).getVersionMetadata().getCounter(node0));
        assertEquals(1, response.getConflictingValues().get(1).getVersionMetadata().getCounter(node1));
    }

    @Test
    void readReturnsResolvedValueWhenOneVersionDominatesAllReplicas() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:run-011";
        String node0 = membership.getNodes().get(0).getId();
        String node1 = membership.getNodes().get(1).getId();
        VersionMetadata oldMetadata = VersionMetadata.initial(node0);
        VersionMetadata latestMetadata = oldMetadata.increment(node1);

        clients.get(node0).put(key, new VersionedValue("score=0.81", 1, oldMetadata));
        clients.get(node1).put(key, new VersionedValue("score=0.86", 2, latestMetadata));
        clients.get(membership.getNodes().get(2).getId())
            .put(key, new VersionedValue("score=0.86", 2, latestMetadata));
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 3),
            clients
        );

        QuorumResponse response = cluster.read(1, key);

        assertTrue(response.isSuccessful());
        assertFalse(response.hasConflicts());
        assertEquals("score=0.86", response.getValue().getValue());
        assertEquals(latestMetadata, response.getValue().getVersionMetadata());
    }

    @Test
    void nonConflictingLeaderlessReadsKeepResolvedValueBehavior() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 2),
            InMemoryReplicaClient.clusterFor(membership)
        );

        cluster.write(1, "trace:run-012", "score=0.91");

        QuorumResponse response = cluster.read(2, "trace:run-012");

        assertTrue(response.isSuccessful());
        assertFalse(response.hasConflicts());
        assertEquals("score=0.91", response.getValue().getValue());
        assertTrue(response.getConflictingValues().isEmpty());
    }

    @Test
    void deterministicPolicyChoosesSameWinningSiblingRegardlessOfReplicaOrder() {
        VersionedValue node0Value = new VersionedValue(
            "score=0.81",
            1,
            VersionMetadata.initial("test-cluster-node-0")
        );
        VersionedValue node1Value = new VersionedValue(
            "score=0.86",
            1,
            VersionMetadata.initial("test-cluster-node-1")
        );

        QuorumResponse firstOrder = new QuorumResponse(
            true,
            node0Value,
            List.of(node0Value, node1Value),
            2,
            2,
            0,
            ConflictResolutionPolicy.RESOLVE_DETERMINISTICALLY
        );
        QuorumResponse reversedOrder = new QuorumResponse(
            true,
            node1Value,
            List.of(node1Value, node0Value),
            2,
            2,
            0,
            ConflictResolutionPolicy.RESOLVE_DETERMINISTICALLY
        );

        assertFalse(firstOrder.hasConflicts());
        assertFalse(reversedOrder.hasConflicts());
        assertEquals("score=0.86", firstOrder.getValue().getValue());
        assertEquals(firstOrder.getValue().getValue(), reversedOrder.getValue().getValue());
        assertEquals(firstOrder.getValue().getVersionMetadata(), reversedOrder.getValue().getVersionMetadata());
    }

    private static List<String> values(List<VersionedValue> versionedValues) {
        return versionedValues.stream()
            .map(VersionedValue::getValue)
            .collect(Collectors.toList());
    }
}
