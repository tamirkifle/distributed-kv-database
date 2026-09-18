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
 * What a stale interrupt flag does to a gRPC call.
 *
 * <p>Written to explain an intermittent failure of
 * {@code LeaderlessQuorumGrpcIntegrationTest.hintedHandoffDeliversAfterPartitionHealsOverGrpc},
 * which reported zero hints applied after a partition healed, roughly once in five full-suite runs
 * and never in isolation.
 *
 * <p>A gRPC blocking stub checks {@code Thread.interrupted()} before it waits, and turns a set flag
 * into {@code CANCELLED} without sending anything. So any thread carrying a stale interrupt cannot
 * make a gRPC call at all. Surefire runs every test class on one thread, which is what makes this
 * reach across test boundaries: one test leaves the flag set, an unrelated test later fails.
 */
class InterruptFlagLeakTest {

    @TempDir
    Path baseDir;

    private ClusterMembership membership;
    private final List<NodeServer> servers = new ArrayList<>();
    private final List<NodeClient> clients = new ArrayList<>();
    private final Map<String, ReplicaClient> replicaClients = new LinkedHashMap<>();
    private final Map<String, PartitionableReplicaClient> partitions = new LinkedHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        Thread.interrupted(); // start from a known state whatever ran before this
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
        Thread.interrupted(); // never leak the flag to the next test, which is the whole point
        for (NodeClient client : clients) {
            client.close();
        }
        for (NodeServer server : servers) {
            server.close();
        }
    }

    private String nodeId(int index) {
        return membership.getNodes().get(index).getId();
    }

    private LeaderlessKVCluster cluster() {
        return LeaderlessKVCluster.create(membership, new QuorumConfig(3, 2, 2), replicaClients);
    }

    @Test
    void aStaleInterruptFlagMakesHintDeliveryFailWithoutSendingAnything() {
        LeaderlessKVCluster cluster = cluster();
        partitions.get(nodeId(2)).setAvailable(false);
        QuorumResponse write = cluster.write(0, "k", "v");
        assertTrue(write.isSuccessful());
        assertEquals(1, cluster.getPendingHints().size());
        partitions.get(nodeId(2)).setAvailable(true);

        // Exactly what pollWithin leaves behind when a wait is interrupted: the flag restored on a
        // thread that then carries on and makes more calls.
        Thread.currentThread().interrupt();

        HintedHandoffReplayResult replay = cluster.replayPendingHints();

        assertEquals(0, replay.getAppliedCount(),
                "a stale interrupt is enough on its own to reproduce the flake");
        assertEquals(1, replay.getRemainingCount());
        assertTrue(Thread.interrupted(), "gRPC restores the flag after converting it to CANCELLED");
    }

    @Test
    void theSameReplaySucceedsOnceTheFlagIsCleared() {
        LeaderlessKVCluster cluster = cluster();
        partitions.get(nodeId(2)).setAvailable(false);
        assertTrue(cluster.write(0, "k", "v").isSuccessful());
        assertEquals(1, cluster.getPendingHints().size());
        partitions.get(nodeId(2)).setAvailable(true);

        assertFalse(Thread.currentThread().isInterrupted());
        HintedHandoffReplayResult replay = cluster.replayPendingHints();

        assertEquals(1, replay.getAppliedCount(), "the control: nothing else differs");
        assertEquals(0, replay.getRemainingCount());
    }
}
