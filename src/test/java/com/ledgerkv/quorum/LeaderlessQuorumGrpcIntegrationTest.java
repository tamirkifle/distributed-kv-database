package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spins up a 3-node cluster of real gRPC {@link NodeServer}s on loopback — each persisting to its
 * own {@link com.ledgerkv.storage.lsm.LsmEngine} — and runs the leaderless quorum
 * ({@link LeaderlessKVCluster}) over {@link GrpcReplicaClient}s. Exercises write/read quorum,
 * read-repair, hinted-handoff, and partition scenarios end to end over the wire. Partitions are
 * modeled with {@link PartitionableReplicaClient} decorators.
 */
class LeaderlessQuorumGrpcIntegrationTest {

    @TempDir
    Path baseDir;

    private ClusterMembership membership;
    private final List<NodeServer> servers = new ArrayList<>();
    private final List<NodeClient> clients = new ArrayList<>();
    private final Map<String, ReplicaClient> replicaClients = new LinkedHashMap<>();
    private final Map<String, PartitionableReplicaClient> partitions = new LinkedHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        membership = ClusterMembership.create("itest", 3, 3);
        for (int i = 0; i < membership.getNodes().size(); i++) {
            String nodeId = membership.getNodes().get(i).getId();
            NodeServer server = NodeServer.builder(0).dataDir(baseDir.resolve("node-" + i)).build();
            server.start();
            servers.add(server);
            NodeClient client = NodeClient.connect("localhost", server.port());
            clients.add(client);
            PartitionableReplicaClient replica =
                new PartitionableReplicaClient(new GrpcReplicaClient(nodeId, client));
            partitions.put(nodeId, replica);
            replicaClients.put(nodeId, replica);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (NodeClient client : clients) {
            client.close();
        }
        for (NodeServer server : servers) {
            server.close();
        }
    }

    private LeaderlessKVCluster clusterWith(QuorumConfig config) {
        return LeaderlessKVCluster.create(membership, config, replicaClients);
    }

    private String nodeId(int index) {
        return membership.getNodes().get(index).getId();
    }

    @Test
    void quorumWriteAndReadRoundTripOverGrpc() {
        LeaderlessKVCluster cluster = clusterWith(new QuorumConfig(3, 2, 2));

        QuorumResponse write = cluster.write(0, "k", "v");
        assertTrue(write.isSuccessful());
        // Released at W=2; the third replica is written behind the client.
        assertEquals(2, write.getRespondingNodes());

        QuorumResponse read = cluster.read(1, "k");
        assertTrue(read.isSuccessful());
        assertEquals("v", read.getValue().getValue());
    }

    @Test
    void readRepairHealsStaleReplicaOverGrpc() {
        LeaderlessKVCluster cluster = clusterWith(new QuorumConfig(3, 2, 2));
        partitions.get(nodeId(2)).setAvailable(false);

        QuorumResponse write = cluster.write(0, "k", "v");
        assertTrue(write.isSuccessful());
        assertEquals(2, write.getRespondingNodes());

        // Heal node 2; the partitioned write never reached it.
        partitions.get(nodeId(2)).setAvailable(true);
        assertTrue(cluster.getReplicaValue(nodeId(2), "k").isEmpty());

        cluster.repair(0, "k");

        assertEquals("v", cluster.getReplicaValue(nodeId(2), "k").orElseThrow().getValue());
    }

    @Test
    void hintedHandoffDeliversAfterPartitionHealsOverGrpc() {
        LeaderlessKVCluster cluster = clusterWith(new QuorumConfig(3, 2, 2));
        partitions.get(nodeId(2)).setAvailable(false);

        QuorumResponse write = cluster.write(0, "k", "v");
        assertTrue(write.isSuccessful());
        assertEquals(1, cluster.getPendingHints().size());

        HintedHandoffReplayResult whilePartitioned = cluster.replayPendingHints();
        assertEquals(0, whilePartitioned.getAppliedCount());
        assertEquals(1, whilePartitioned.getRemainingCount());

        partitions.get(nodeId(2)).setAvailable(true);
        HintedHandoffReplayResult afterHeal = cluster.replayPendingHints();
        assertEquals(1, afterHeal.getAppliedCount());
        assertEquals(0, afterHeal.getRemainingCount());

        assertEquals("v", cluster.getReplicaValue(nodeId(2), "k").orElseThrow().getValue());
    }

    @Test
    void partitionedReadFailsQuorumOverGrpc() {
        LeaderlessKVCluster cluster = clusterWith(new QuorumConfig(3, 3, 3));

        assertTrue(cluster.write(0, "k", "v").isSuccessful());

        partitions.get(nodeId(1)).setAvailable(false);
        partitions.get(nodeId(2)).setAvailable(false);

        assertFalse(cluster.read(0, "k").isSuccessful());
    }
}
