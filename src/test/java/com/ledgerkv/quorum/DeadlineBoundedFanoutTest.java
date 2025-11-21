package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class DeadlineBoundedFanoutTest {

    private static ClusterMembership membership() {
        return ClusterMembership.create("test-cluster", 3, 3);
    }

    @Test
    void writeReturnsOnWAcksWithoutWaitingForSlowReplicaAndRecordsHint() throws Exception {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        // Make the LAST replica in the preference list slow (latch-blocked, never released).
        String key = "trace:run-101";
        java.util.List<ClusterNode> prefs = membership.getPreferenceList(key, 3);
        String slowId = prefs.get(2).getId();
        LatchControlledReplicaClient slow = new LatchControlledReplicaClient(clients.get(slowId));
        clients.put(slowId, slow);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), clients, pool, Duration.ofMillis(200));

        // W=2 fast replicas should satisfy the write without releasing the slow one. The write
        // returns at the (small) deadline with two acks; the slow replica becomes a pending hint.
        QuorumResponse response = cluster.write(0, key, "v1");

        assertTrue(response.isSuccessful());
        assertEquals(2, response.getRespondingNodes());
        // The slow replica did not ack within budget -> becomes a pending hint.
        assertTrue(cluster.getPendingHints().stream()
            .anyMatch(h -> h.getTargetNodeId().equals(slowId)),
            "slow replica should be recorded as a pending hint");

        slow.release();
        pool.shutdownNow();
    }

    @Test
    void readReturnsNewestVersionRegardlessOfArrivalOrder() throws Exception {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:run-102";
        // Seed two replicas with v1, one with a NEWER v2; all fast.
        java.util.List<ClusterNode> prefs = membership.getPreferenceList(key, 3);
        VersionedValue v1 = new VersionedValue("old", 1, VersionMetadata.legacy(1));
        VersionedValue v2 = new VersionedValue("new", 2, VersionMetadata.legacy(2));
        clients.get(prefs.get(0).getId()).put(key, v1);
        clients.get(prefs.get(1).getId()).put(key, v1);
        clients.get(prefs.get(2).getId()).put(key, v2);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 3), clients, pool, Duration.ofSeconds(2));

        QuorumResponse response = cluster.read(0, key);

        assertTrue(response.isSuccessful());
        assertNotNull(response.getValue());
        assertEquals("new", response.getValue().getValue());
        pool.shutdownNow();
    }

    @Test
    void readDoesNotBlockOnSlowReplicaWhenRFastResponders() throws Exception {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:run-103";
        VersionedValue seeded = new VersionedValue("score=0.9", 1, VersionMetadata.legacy(1));
        for (ReplicaClient c : clients.values()) {
            c.put(key, seeded);
        }
        java.util.List<ClusterNode> prefs = membership.getPreferenceList(key, 3);
        String slowId = prefs.get(2).getId();
        LatchControlledReplicaClient slow = new LatchControlledReplicaClient(clients.get(slowId));
        clients.put(slowId, slow);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), clients, pool, Duration.ofSeconds(2));

        QuorumResponse response = cluster.read(0, key);

        assertTrue(response.isSuccessful());
        assertEquals("score=0.9", response.getValue().getValue());
        slow.release();
        pool.shutdownNow();
    }

    @Test
    void writeFailsWhenFewerThanWCanRespondWithinDeadline() throws Exception {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:run-104";
        // Block TWO of three replicas; with W=2 only one can ack -> failure, independent of timing.
        java.util.List<ClusterNode> prefs = membership.getPreferenceList(key, 3);
        LatchControlledReplicaClient slow1 = new LatchControlledReplicaClient(clients.get(prefs.get(1).getId()));
        LatchControlledReplicaClient slow2 = new LatchControlledReplicaClient(clients.get(prefs.get(2).getId()));
        clients.put(prefs.get(1).getId(), slow1);
        clients.put(prefs.get(2).getId(), slow2);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), clients, pool, Duration.ofMillis(50));

        QuorumResponse response = cluster.write(0, key, "v1");

        assertFalse(response.isSuccessful());
        assertEquals(1, response.getRespondingNodes());
        assertEquals(2, response.getRequiredNodes());
        slow1.release();
        slow2.release();
        pool.shutdownNow();
    }

    @Test
    void closeShutsDownOwnedPoolButNotCallerSuppliedExecutor() {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> ownedClients =
            new LinkedHashMap<>(InMemoryReplicaClient.clusterFor(membership));
        // Owned pool (default 3-arg factory) -> close() shuts it down.
        LeaderlessKVCluster owned = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), ownedClients);
        owned.close();
        // Idempotent / safe to call twice.
        owned.close();

        // Caller-supplied pool -> close() must NOT shut it down.
        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster injected = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2),
            new LinkedHashMap<>(InMemoryReplicaClient.clusterFor(membership)),
            pool, Duration.ofSeconds(1));
        injected.close();
        assertFalse(pool.isShutdown(), "close() must not shut down a caller-supplied executor");
        pool.shutdownNow();
    }
}
