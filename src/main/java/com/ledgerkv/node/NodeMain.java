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
 * Container entrypoint. Reads {@link NodeConfig} from the environment and starts whichever
 * replication path {@code LEDGERKV_MODE} names, then blocks until the JVM is asked to shut down.
 *
 * <ul>
 *   <li>{@code quorum} (the default) starts this node's {@link NodeServer} over its own
 *       {@link com.ledgerkv.storage.lsm.LsmEngine}, connects a {@link GrpcReplicaClient} to every
 *       peer including itself over loopback, and coordinates through {@link LeaderlessKVCluster}.
 *   <li>{@code raft} starts a {@link RaftRuntime} on the separate Raft port and a storage-free
 *       {@link NodeServer} in front of it. No LSM engine is opened: the Raft state machine is the
 *       store.
 * </ul>
 */
public final class NodeMain {

    private NodeMain() {
    }

    public static void main(String[] args) throws Exception {
        NodeConfig config = NodeConfig.fromEnv(System.getenv());
        if (config.mode() == NodeMode.RAFT) {
            startRaft(config);
        } else {
            startQuorum(config);
        }
    }

    private static void startRaft(NodeConfig config) throws Exception {
        RaftRuntime runtime = RaftRuntime.start(config);
        NodeServer server = NodeServer.builder(config.grpcPort()).coordinatorOnly().build();
        server.start();

        RaftClientCoordinator coordinator = new RaftClientCoordinator(
                runtime.node(), runtime.driver(), runtime.state(),
                config.requestDeadline(), runtime.clientEndpoints());
        server.useCoordinator(coordinator);

        HealthServer health = HealthServer.start(config.healthPort(),
                () -> PrometheusExporter.render(
                        config.nodeId(),
                        coordinator.operationMetrics(),
                        LatencySummary.from(coordinator.operationMetrics()),
                        RepairMetrics.empty(),
                        runtime.status()),
                runtime::ready);

        System.out.println("LedgerKV " + config.nodeId() + " up: mode=raft"
                + " gRPC=" + config.grpcPort() + " raft=" + config.raftPort()
                + " health=" + config.healthPort()
                + " deadline=" + config.requestDeadline().toMillis() + "ms"
                + " peers=" + config.raftPeers());

        awaitShutdown(() -> {
            closeQuietly(server);
            runtime.close();
            health.close();
        });
    }

    private static void startQuorum(NodeConfig config) throws Exception {
        ClusterIdentity.claim(config.dataDir(), config.identity());

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
                new QuorumConfig(config.replicationFactor(), config.writeQuorum(),
                        config.readQuorum(), config.primaryWriteQuorum(), config.primaryReadQuorum()),
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

        System.out.println("LedgerKV " + config.nodeId() + " up: mode=quorum"
                + " gRPC=" + config.grpcPort()
                + " health=" + config.healthPort() + " deadline=" + config.requestDeadline().toMillis()
                + "ms hedge=" + config.hedgingDelay().toMillis()
                + "ms peers=" + config.peers());

        awaitShutdown(() -> {
            for (NodeClient client : peerClients) {
                closeQuietly(client);
            }
            closeQuietly(server);
            closeQuietly(cluster);
            quorumExecutor.shutdownNow();
            health.close();
        });
    }

    private static void awaitShutdown(Runnable teardown) throws InterruptedException {
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                teardown.run();
            } catch (RuntimeException ignored) {
                // best-effort shutdown
            }
            shutdown.countDown();
        }));
        shutdown.await();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort shutdown
        }
    }
}
