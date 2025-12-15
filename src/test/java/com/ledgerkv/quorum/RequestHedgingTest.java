package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class RequestHedgingTest {

    private static ClusterMembership membership() {
        // 4 nodes, N=3 -> a distinct 4th replica exists as a hedge candidate.
        return ClusterMembership.create("test-cluster", 4, 3);
    }

    @Test
    void writeHedgesToNextReplicaWhenAPrimaryIsSlow() {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:hedge-write";
        List<ClusterNode> prefs = membership.getPreferenceList(key, 4);
        // Make ONE primary (the 3rd of N=3) slow, so only 2 primaries can ack; W=3 needs the hedge.
        String slowPrimaryId = prefs.get(2).getId();
        LatchControlledReplicaClient slow =
            new LatchControlledReplicaClient(clients.get(slowPrimaryId));
        clients.put(slowPrimaryId, slow);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 3, 2), clients, pool,
            Duration.ofSeconds(2), Duration.ofMillis(20));

        QuorumResponse response = cluster.write(0, key, "v1");

        assertTrue(response.isSuccessful(), "hedge to the 4th replica should complete the W=3 quorum");
        assertEquals(3, response.getRespondingNodes(), "exactly W distinct acks, no double count");
        assertEquals(1, cluster.hedgedRequestCount(), "exactly one hedge fired");

        slow.release();
        pool.shutdownNow();
    }

    @Test
    void readHedgesToNextReplicaWhenAPrimaryIsSlow() {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:hedge-read";
        VersionedValue seeded = new VersionedValue("score=0.9", 1, VersionMetadata.legacy(1));
        for (ReplicaClient c : clients.values()) {
            c.put(key, seeded);
        }
        List<ClusterNode> prefs = membership.getPreferenceList(key, 4);
        // Block two of the three primaries; R=2 then needs the hedge to the 4th replica.
        LatchControlledReplicaClient slow1 =
            new LatchControlledReplicaClient(clients.get(prefs.get(1).getId()));
        LatchControlledReplicaClient slow2 =
            new LatchControlledReplicaClient(clients.get(prefs.get(2).getId()));
        clients.put(prefs.get(1).getId(), slow1);
        clients.put(prefs.get(2).getId(), slow2);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), clients, pool,
            Duration.ofSeconds(2), Duration.ofMillis(20));

        QuorumResponse response = cluster.read(0, key);

        assertTrue(response.isSuccessful());
        assertEquals("score=0.9", response.getValue().getValue());
        assertEquals(1, cluster.hedgedRequestCount());

        slow1.release();
        slow2.release();
        pool.shutdownNow();
    }

    @Test
    void noHedgeFiresWhenAllPrimariesAreFast() {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:no-hedge";

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 2, 2), clients, pool,
            Duration.ofSeconds(2), Duration.ofMillis(20));

        QuorumResponse write = cluster.write(0, key, "v1");
        QuorumResponse read = cluster.read(0, key);

        assertTrue(write.isSuccessful());
        assertTrue(read.isSuccessful());
        assertEquals(0, cluster.hedgedRequestCount(), "fast primaries -> no hedge");

        pool.shutdownNow();
    }

    @Test
    void writeStillFailsWhenNeitherPrimariesNorHedgeCanMeetQuorum() {
        ClusterMembership membership = membership();
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String key = "trace:hedge-fail";
        List<ClusterNode> prefs = membership.getPreferenceList(key, 4);
        // Block two primaries AND the hedge candidate (4th): only 1 distinct node can ack, W=3 fails.
        LatchControlledReplicaClient s1 = new LatchControlledReplicaClient(clients.get(prefs.get(1).getId()));
        LatchControlledReplicaClient s2 = new LatchControlledReplicaClient(clients.get(prefs.get(2).getId()));
        LatchControlledReplicaClient s3 = new LatchControlledReplicaClient(clients.get(prefs.get(3).getId()));
        clients.put(prefs.get(1).getId(), s1);
        clients.put(prefs.get(2).getId(), s2);
        clients.put(prefs.get(3).getId(), s3);

        ExecutorService pool = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership, new QuorumConfig(3, 3, 2), clients, pool,
            Duration.ofMillis(80), Duration.ofMillis(20));

        QuorumResponse response = cluster.write(0, key, "v1");

        assertFalse(response.isSuccessful());
        assertEquals(1, response.getRespondingNodes());

        s1.release();
        s2.release();
        s3.release();
        pool.shutdownNow();
    }
}
