package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.GrpcReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Wires three real gRPC {@link NodeServer}s, each acting as a quorum coordinator over the shared
 * preference list, and drives their <em>public</em> Get/Put. Proves a value written via one node is
 * readable via another and survives a hard node loss with N=3, W=R=2.
 */
class NodeServerCoordinatorRoutingTest {

    @TempDir
    Path baseDir;

    private ClusterMembership membership;
    private final List<NodeServer> servers = new ArrayList<>();
    private final List<NodeClient> publicClients = new ArrayList<>();
    private final List<NodeClient> replicaClients = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        membership = ClusterMembership.create("itest", 3, 3);
        // Start three nodes.
        for (int i = 0; i < 3; i++) {
            NodeServer server = NodeServer.builder(0).dataDir(baseDir.resolve("node-" + i)).build();
            server.start();
            servers.add(server);
            publicClients.add(NodeClient.connect("localhost", server.port()));
        }
        // Build a shared replica-client map (one GrpcReplicaClient per node), then give each server
        // its own coordinator over that map.
        Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            String nodeId = membership.getNodes().get(i).getId();
            NodeClient client = NodeClient.connect("localhost", servers.get(i).port());
            replicaClients.add(client);
            replicas.put(nodeId, new GrpcReplicaClient(nodeId, client));
        }
        for (int i = 0; i < 3; i++) {
            LeaderlessKVCluster cluster =
                    LeaderlessKVCluster.create(membership, new QuorumConfig(3, 2, 2), replicas);
            servers.get(i).useCoordinator(new QuorumClientCoordinator(cluster, i));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (NodeClient client : publicClients) {
            client.close();
        }
        for (NodeClient client : replicaClients) {
            client.close();
        }
        for (NodeServer server : servers) {
            try {
                server.close();
            } catch (Exception ignored) {
                // a server killed mid-test is already closed
            }
        }
    }

    @Test
    void writeViaOneNodeIsReadableViaAnother() {
        publicClients.get(0).put("k", "v".getBytes(StandardCharsets.UTF_8));
        Optional<byte[]> read = publicClients.get(1).get("k");
        assertTrue(read.isPresent());
        assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), read.get());
    }

    @Test
    void quorumReadSurvivesNodeLoss() throws Exception {
        publicClients.get(0).put("k", "v".getBytes(StandardCharsets.UTF_8));

        // Hard-stop one replica; with N=3, W=R=2 the value is still readable through a survivor.
        servers.get(2).close();

        Optional<byte[]> read = publicClients.get(1).get("k");
        assertTrue(read.isPresent());
        assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), read.get());
    }
}
