package com.ledgerkv.node;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.GrpcReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Container entrypoint. Reads {@link NodeConfig} from the environment, starts this node's gRPC
 * {@link NodeServer} + {@link HealthServer}, connects a {@link GrpcReplicaClient} to every peer
 * (including itself, over loopback), and routes the node's public Get/Put through a
 * {@link LeaderlessKVCluster} quorum coordinator. Blocks until the JVM is asked to shut down.
 */
public final class NodeMain {

    private NodeMain() {
    }

    public static void main(String[] args) throws Exception {
        NodeConfig config = NodeConfig.fromEnv(System.getenv());

        NodeServer server = NodeServer.builder(config.grpcPort()).dataDir(config.dataDir()).build();
        server.start();
        HealthServer health = HealthServer.start(config.healthPort());

        ClusterMembership membership = ClusterMembership.create(
                config.clusterId(), config.nodeCount(), config.replicationFactor());

        List<NodeClient> peerClients = new ArrayList<>();
        Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        for (int i = 0; i < config.nodeCount(); i++) {
            String nodeId = config.clusterId() + "-node-" + i;
            String[] hostPort = config.peers().get(i).split(":", 2);
            NodeClient client = NodeClient.connect(hostPort[0], Integer.parseInt(hostPort[1]));
            peerClients.add(client);
            replicas.put(nodeId, new GrpcReplicaClient(nodeId, client));
        }

        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
                membership,
                new QuorumConfig(config.replicationFactor(), config.writeQuorum(), config.readQuorum()),
                replicas);
        server.useCoordinator(new QuorumClientCoordinator(cluster, config.nodeIndex()));

        System.out.println("LedgerKV " + config.nodeId() + " up: gRPC=" + config.grpcPort()
                + " health=" + config.healthPort() + " peers=" + config.peers());

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (NodeClient client : peerClients) {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // best-effort shutdown
                }
            }
            try {
                server.close();
            } catch (Exception ignored) {
                // best-effort shutdown
            }
            health.close();
            shutdown.countDown();
        }));
        shutdown.await();
    }
}
