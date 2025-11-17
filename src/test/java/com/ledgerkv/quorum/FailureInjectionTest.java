package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.failure.FailureCause;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FailureInjectionTest {

    @Test
    void failedWriteQuorumReportsRespondingAndFailedReplicaIds() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String failedNodeId = membership.getNodes().get(1).getId();
        PartitionableReplicaClient failed =
            new PartitionableReplicaClient(clients.get(failedNodeId));
        failed.setAvailable(false);
        clients.put(failedNodeId, failed);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 3, 2),
            clients
        );

        QuorumResponse response = cluster.write(0, "trace:run-007", "score=0.77");

        assertFalse(response.isSuccessful());
        assertEquals(2, response.getFailureContext().getRespondingNodeIds().size());
        assertTrue(response.getFailureContext().getFailedNodeIds().contains(failedNodeId));
        assertEquals(
            FailureCause.UNAVAILABLE_NODE,
            response.getFailureContext().getFailureCauses().get(failedNodeId)
        );
    }

    @Test
    void failedReadQuorumReportsRespondingAndFailedReplicaIds() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        VersionedValue seeded = new VersionedValue("score=0.89", 1, VersionMetadata.legacy(1));
        for (ReplicaClient client : clients.values()) {
            client.put("trace:run-008", seeded);
        }
        String failedNodeId = membership.getNodes().get(2).getId();
        PartitionableReplicaClient failed =
            new PartitionableReplicaClient(clients.get(failedNodeId));
        failed.setAvailable(false);
        clients.put(failedNodeId, failed);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 3),
            clients
        );

        QuorumResponse response = cluster.read(1, "trace:run-008");

        assertFalse(response.isSuccessful());
        assertEquals(2, response.getFailureContext().getRespondingNodeIds().size());
        assertTrue(response.getFailureContext().getFailedNodeIds().contains(failedNodeId));
        assertEquals(
            FailureCause.UNAVAILABLE_NODE,
            response.getFailureContext().getFailureCauses().get(failedNodeId)
        );
    }
}
