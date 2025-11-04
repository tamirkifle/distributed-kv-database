package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LeaderlessKVClusterTest {

    @Test
    void writeFailsWhenSuccessfulAcknowledgmentsStayBelowW() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String unavailableNodeId = membership.getNodes().get(1).getId();
        PartitionableReplicaClient unavailable =
            new PartitionableReplicaClient(clients.get(unavailableNodeId));
        unavailable.setAvailable(false);
        clients.put(unavailableNodeId, unavailable);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 3, 2),
            clients
        );

        QuorumResponse response = cluster.write(0, "trace:run-005", "score=0.79");

        assertFalse(response.isSuccessful());
        assertNull(response.getValue());
        assertEquals(2, response.getRespondingNodes());
        assertEquals(3, response.getRequiredNodes());
    }

    @Test
    void readFailsWhenSuccessfulResponsesStayBelowR() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        VersionedValue seeded = new VersionedValue("score=0.84", 1, VersionMetadata.legacy(1));
        for (ReplicaClient client : clients.values()) {
            client.put("trace:run-006", seeded);
        }
        String unavailableNodeId = membership.getNodes().get(2).getId();
        PartitionableReplicaClient unavailable =
            new PartitionableReplicaClient(clients.get(unavailableNodeId));
        unavailable.setAvailable(false);
        clients.put(unavailableNodeId, unavailable);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 3),
            clients
        );

        QuorumResponse response = cluster.read(1, "trace:run-006");

        assertFalse(response.isSuccessful());
        assertNull(response.getValue());
        assertEquals(2, response.getRespondingNodes());
        assertEquals(3, response.getRequiredNodes());
        assertEquals(2, response.getAllValues().size());
    }
}
