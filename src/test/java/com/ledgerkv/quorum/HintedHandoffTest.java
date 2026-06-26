package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HintedHandoffTest {

    @Test
    void successfulPartialWriteRecordsHintForUnavailableReplica() throws Exception {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String unavailableNodeId = membership.getNodes().get(1).getId();
        PartitionableReplicaClient unavailable =
            new PartitionableReplicaClient(clients.get(unavailableNodeId));
        unavailable.setAvailable(false);
        clients.put(unavailableNodeId, unavailable);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 2),
            clients
        );

        QuorumResponse response = cluster.write(0, "trace:run-009", "score=0.91");

        assertTrue(response.isSuccessful());
        // The write returns at W; the hint for the unavailable replica is filed by the background
        // accounting task, so wait for that rather than racing it.
        assertTrue(cluster.awaitReplication(java.time.Duration.ofSeconds(5)));
        List<HintedHandoff> pendingHints = cluster.getPendingHints();
        assertEquals(1, pendingHints.size());
        HintedHandoff hint = pendingHints.get(0);
        assertEquals(membership.getNodes().get(0).getId(), hint.getCoordinatorNodeId());
        assertEquals(unavailableNodeId, hint.getTargetNodeId());
        assertEquals("trace:run-009", hint.getKey());
        assertEquals("score=0.91", hint.getValue());
        assertEquals(response.getValue().getVersionMetadata(), hint.getVersionMetadata());
    }

    @Test
    void replayKeepsFailedHintsPendingAndClearsAppliedHints() throws Exception {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String unavailableNodeId = membership.getNodes().get(1).getId();
        PartitionableReplicaClient recovering =
            new PartitionableReplicaClient(clients.get(unavailableNodeId));
        recovering.setAvailable(false);
        clients.put(unavailableNodeId, recovering);
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            new QuorumConfig(3, 2, 2),
            clients
        );
        QuorumResponse writeResponse = cluster.write(0, "trace:run-010", "score=0.93");
        assertTrue(cluster.awaitReplication(java.time.Duration.ofSeconds(5)));

        HintedHandoffReplayResult failedReplay = cluster.replayPendingHints();
        recovering.setAvailable(true);
        HintedHandoffReplayResult successfulReplay = cluster.replayPendingHints();

        assertEquals(1, failedReplay.getAttemptedCount());
        assertEquals(0, failedReplay.getAppliedCount());
        assertEquals(1, failedReplay.getRemainingCount());
        assertEquals(1, successfulReplay.getAttemptedCount());
        assertEquals(1, successfulReplay.getAppliedCount());
        assertEquals(0, successfulReplay.getRemainingCount());
        assertTrue(cluster.getPendingHints().isEmpty());

        VersionedValue recoveredValue = cluster.getReplicaValue(unavailableNodeId, "trace:run-010")
            .orElseThrow(() -> new AssertionError("expected hinted value on recovered replica"));
        assertEquals("score=0.93", recoveredValue.getValue());
        assertEquals(writeResponse.getValue().getVersionMetadata(), recoveredValue.getVersionMetadata());
    }
}
