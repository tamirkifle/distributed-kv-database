package com.ledgerkv.node;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable per-node configuration parsed from environment variables.
 *
 * <p>The derived {@link #nodeId()} (= {@code clusterId-node-index}) matches the id scheme of
 * {@link com.ledgerkv.quorum.ClusterMembership#create}, so a coordinator's replica-client map keys
 * line up with the membership's node ids.
 */
public final class NodeConfig {

    private final String clusterId;
    private final int nodeIndex;
    private final int nodeCount;
    private final int grpcPort;
    private final int healthPort;
    private final Path dataDir;
    private final int replicationFactor;
    private final int writeQuorum;
    private final int readQuorum;
    private final int requestDeadlineMillis;
    private final List<String> peers;

    private NodeConfig(String clusterId, int nodeIndex, int nodeCount, int grpcPort, int healthPort,
                       Path dataDir, int replicationFactor, int writeQuorum, int readQuorum,
                       int requestDeadlineMillis, List<String> peers) {
        this.clusterId = clusterId;
        this.nodeIndex = nodeIndex;
        this.nodeCount = nodeCount;
        this.grpcPort = grpcPort;
        this.healthPort = healthPort;
        this.dataDir = dataDir;
        this.replicationFactor = replicationFactor;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.requestDeadlineMillis = requestDeadlineMillis;
        this.peers = Collections.unmodifiableList(peers);
    }

    public static NodeConfig fromEnv(Map<String, String> env) {
        List<String> peers = parsePeers(require(env, "LEDGERKV_PEERS"));
        int nodeIndex = requireInt(env, "LEDGERKV_NODE_INDEX");
        String clusterId = env.getOrDefault("LEDGERKV_CLUSTER_ID", "ledgerkv");
        int nodeCount = intOrDefault(env, "LEDGERKV_NODE_COUNT", peers.size());
        int grpcPort = intOrDefault(env, "LEDGERKV_GRPC_PORT", 9090);
        int healthPort = intOrDefault(env, "LEDGERKV_HEALTH_PORT", 8080);
        Path dataDir = Paths.get(env.getOrDefault("LEDGERKV_DATA_DIR", "/data"));
        int replicationFactor = intOrDefault(env, "LEDGERKV_REPLICATION_FACTOR", 3);
        int writeQuorum = intOrDefault(env, "LEDGERKV_WRITE_QUORUM", 2);
        int readQuorum = intOrDefault(env, "LEDGERKV_READ_QUORUM", 2);
        int requestDeadlineMillis = intOrDefault(env, "LEDGERKV_REQUEST_DEADLINE_MS", 5000);

        if (peers.size() != nodeCount) {
            throw new IllegalArgumentException(
                    "peer count " + peers.size() + " does not match node count " + nodeCount);
        }
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException(
                    "node index " + nodeIndex + " is out of range [0, " + nodeCount + ")");
        }
        return new NodeConfig(clusterId, nodeIndex, nodeCount, grpcPort, healthPort, dataDir,
                replicationFactor, writeQuorum, readQuorum, requestDeadlineMillis, peers);
    }

    public String clusterId() { return clusterId; }
    public int nodeIndex() { return nodeIndex; }
    public int nodeCount() { return nodeCount; }
    public int grpcPort() { return grpcPort; }
    public int healthPort() { return healthPort; }
    public Path dataDir() { return dataDir; }
    public int replicationFactor() { return replicationFactor; }
    public int writeQuorum() { return writeQuorum; }
    public int readQuorum() { return readQuorum; }
    public Duration requestDeadline() { return Duration.ofMillis(requestDeadlineMillis); }
    public List<String> peers() { return peers; }

    public String nodeId() { return clusterId + "-node-" + nodeIndex; }

    private static List<String> parsePeers(String raw) {
        List<String> peers = new ArrayList<>();
        for (String entry : raw.split(",")) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                peers.add(trimmed);
            }
        }
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("LEDGERKV_PEERS must list at least one host:port");
        }
        return peers;
    }

    private static String require(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing required env var " + key);
        }
        return value.trim();
    }

    private static int requireInt(Map<String, String> env, String key) {
        return Integer.parseInt(require(env, key));
    }

    private static int intOrDefault(Map<String, String> env, String key, int fallback) {
        String value = env.get(key);
        return (value == null || value.trim().isEmpty()) ? fallback : Integer.parseInt(value.trim());
    }
}
