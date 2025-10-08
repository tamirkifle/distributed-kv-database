package com.ledgerkv;

import com.ledgerkv.quorum.ClusterNode;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LeaderlessKVClusterTest {

    @Test
    void anyNodeCanCoordinateWritesToTheSameReplicaSet() {
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            "test-cluster",
            new QuorumConfig(3, 2, 2)
        );

        QuorumResponse firstWrite = cluster.write(0, "trace:run-001", "baseline-score=0.82");
        QuorumResponse secondWrite = cluster.write(2, "trace:run-001", "baseline-score=0.91");

        assertTrue(firstWrite.isSuccessful());
        assertEquals(3, firstWrite.getRespondingNodes());
        assertEquals(2, firstWrite.getRequiredNodes());
        assertTrue(secondWrite.isSuccessful());

        for (ClusterNode replica : cluster.selectReplicas("trace:run-001")) {
            VersionedValue storedValue = cluster.getReplicaValue(replica.getId(), "trace:run-001").orElseThrow();
            assertEquals("baseline-score=0.91", storedValue.getValue());
        }
    }

    @Test
    void anyNodeCanCoordinateReadsFromTheSameReplicaSet() {
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            "test-cluster",
            new QuorumConfig(3, 2, 2)
        );

        cluster.write(1, "trace:run-002", "candidate-score=0.87");

        QuorumResponse readFromNode0 = cluster.read(0, "trace:run-002");
        QuorumResponse readFromNode2 = cluster.read(2, "trace:run-002");

        assertTrue(readFromNode0.isSuccessful());
        assertTrue(readFromNode2.isSuccessful());
        assertEquals("candidate-score=0.87", readFromNode0.getValue().getValue());
        assertEquals("candidate-score=0.87", readFromNode2.getValue().getValue());
        assertEquals(readFromNode0.getValue(), readFromNode2.getValue());
    }

    @Test
    void replicaSelectionIsIndependentOfCoordinatorChoice() {
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            "test-cluster",
            new QuorumConfig(5, 3, 3)
        );

        List<ClusterNode> coordinator0Replicas = cluster.selectReplicas("trace:run-003");
        cluster.write(0, "trace:run-003", "score=0.73");
        List<ClusterNode> coordinator4Replicas = cluster.selectReplicas("trace:run-003");

        assertEquals(coordinator0Replicas, coordinator4Replicas);
        assertEquals(5, coordinator0Replicas.size());
    }

    @Test
    void rejectsInvalidCoordinatorIndex() {
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            "test-cluster",
            new QuorumConfig(3, 2, 2)
        );

        assertThrows(IllegalArgumentException.class,
            () -> cluster.write(-1, "trace:run-004", "score=0.65"));
        assertThrows(IllegalArgumentException.class,
            () -> cluster.read(3, "trace:run-004"));
    }
}
