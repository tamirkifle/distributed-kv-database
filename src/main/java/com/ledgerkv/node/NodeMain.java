package com.ledgerkv.node;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.GrpcReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;
import com.ledgerkv.metrics.LatencySummary;
import com.ledgerkv.metrics.PrometheusExporter;
import com.ledgerkv.metrics.RepairMetrics;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

        ExecutorService quorumExecutor = Executors.newCachedThreadPool();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
                membership,
                new QuorumConfig(config.replicationFactor(), config.writeQuorum(), config.readQuorum()),
                replicas,
                quorumExecutor,
                config.requestDeadline(),
                config.hedgingDelay());
        QuorumClientCoordinator coordinator =
                new QuorumClientCoordinator(cluster, config.nodeIndex());
        server.useCoordinator(coordinator);

        // RepairMetrics is empty: the leaderless cluster path does not surface a live repair
        // counter yet (the leaderless quorum path has no live read-repair counter).
        HealthServer health = HealthServer.start(config.healthPort(), () ->
                PrometheusExporter.render(
                        config.nodeId(),
                        coordinator.operationMetrics(),
                        LatencySummary.from(coordinator.operationMetrics()),
                        RepairMetrics.empty()));

        System.out.println("LedgerKV " + config.nodeId() + " up: gRPC=" + config.grpcPort()
                + " health=" + config.healthPort() + " deadline=" + config.requestDeadline().toMillis()
                + "ms hedge=" + config.hedgingDelay().toMillis()
                + "ms peers=" + config.peers());

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
            try {
                cluster.close();
            } catch (Exception ignored) {
                // best-effort shutdown
            }
            quorumExecutor.shutdownNow();
            health.close();
            shutdown.countDown();
        }));
        shutdown.await();
    }
}
