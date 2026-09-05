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
 *
 * <p>{@code LEDGERKV_MODE} picks the replication path. The quorum settings (N/R/W, hedging) apply
 * only in {@code quorum} mode and the {@code LEDGERKV_RAFT_*} settings only in {@code raft} mode;
 * both are parsed either way so a misspelled value still fails at startup rather than silently
 * doing nothing.
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
    private final int primaryWriteQuorum;
    private final int primaryReadQuorum;
    private final int requestDeadlineMillis;
    private final int hedgingDelayMillis;
    private final List<String> peers;
    private final NodeMode mode;
    private final int raftPort;
    private final List<String> raftPeers;
    private final int raftHeartbeatMillis;
    private final int raftElectionMinTicks;
    private final int raftElectionMaxTicks;
    private final int raftRpcDeadlineMillis;
    private final int raftSnapshotThreshold;
    private final int raftMaxSessions;

    private NodeConfig(Builder b) {
        this.clusterId = b.clusterId;
        this.nodeIndex = b.nodeIndex;
        this.nodeCount = b.nodeCount;
        this.grpcPort = b.grpcPort;
        this.healthPort = b.healthPort;
        this.dataDir = b.dataDir;
        this.replicationFactor = b.replicationFactor;
        this.writeQuorum = b.writeQuorum;
        this.readQuorum = b.readQuorum;
        this.primaryWriteQuorum = b.primaryWriteQuorum;
        this.primaryReadQuorum = b.primaryReadQuorum;
        this.requestDeadlineMillis = b.requestDeadlineMillis;
        this.hedgingDelayMillis = b.hedgingDelayMillis;
        this.peers = Collections.unmodifiableList(b.peers);
        this.mode = b.mode;
        this.raftPort = b.raftPort;
        this.raftPeers = Collections.unmodifiableList(b.raftPeers);
        this.raftHeartbeatMillis = b.raftHeartbeatMillis;
        this.raftElectionMinTicks = b.raftElectionMinTicks;
        this.raftElectionMaxTicks = b.raftElectionMaxTicks;
        this.raftRpcDeadlineMillis = b.raftRpcDeadlineMillis;
        this.raftSnapshotThreshold = b.raftSnapshotThreshold;
        this.raftMaxSessions = b.raftMaxSessions;
    }

    /** Mutable carrier for the parsed env, so the constructor does not take eighteen arguments. */
    private static final class Builder {
        String clusterId;
        int nodeIndex;
        int nodeCount;
        int grpcPort;
        int healthPort;
        Path dataDir;
        int replicationFactor;
        int writeQuorum;
        int readQuorum;
        int primaryWriteQuorum;
        int primaryReadQuorum;
        int requestDeadlineMillis;
        int hedgingDelayMillis;
        List<String> peers;
        NodeMode mode;
        int raftPort;
        List<String> raftPeers;
        int raftHeartbeatMillis;
        int raftElectionMinTicks;
        int raftElectionMaxTicks;
        int raftRpcDeadlineMillis;
        int raftSnapshotThreshold;
        int raftMaxSessions;
    }

    public static NodeConfig fromEnv(Map<String, String> env) {
        Builder b = new Builder();
        b.peers = parsePeers(require(env, "LEDGERKV_PEERS"));
        b.nodeIndex = requireInt(env, "LEDGERKV_NODE_INDEX");
        b.clusterId = env.getOrDefault("LEDGERKV_CLUSTER_ID", "ledgerkv");
        b.nodeCount = intOrDefault(env, "LEDGERKV_NODE_COUNT", b.peers.size());
        b.grpcPort = intOrDefault(env, "LEDGERKV_GRPC_PORT", 9090);
        b.healthPort = intOrDefault(env, "LEDGERKV_HEALTH_PORT", 8080);
        b.dataDir = Paths.get(env.getOrDefault("LEDGERKV_DATA_DIR", "/data"));
        b.replicationFactor = intOrDefault(env, "LEDGERKV_REPLICATION_FACTOR", 3);
        b.writeQuorum = intOrDefault(env, "LEDGERKV_WRITE_QUORUM", 2);
        b.readQuorum = intOrDefault(env, "LEDGERKV_READ_QUORUM", 2);
        // Primary-only counts (Riak's pw/pr). Default 0 keeps the existing sloppy-quorum
        // behaviour; set both so PW+PR>N to demand the overlap W+R>N only appears to give.
        b.primaryWriteQuorum = intOrDefault(env, "LEDGERKV_PRIMARY_WRITE_QUORUM", 0);
        b.primaryReadQuorum = intOrDefault(env, "LEDGERKV_PRIMARY_READ_QUORUM", 0);
        b.requestDeadlineMillis = intOrDefault(env, "LEDGERKV_REQUEST_DEADLINE_MS", 5000);
        b.hedgingDelayMillis = intOrDefault(env, "LEDGERKV_HEDGING_DELAY_MS", 50);

        b.mode = NodeMode.parse(env.getOrDefault("LEDGERKV_MODE", NodeMode.QUORUM.label()));
        b.raftPort = intOrDefault(env, "LEDGERKV_RAFT_PORT", 9095);
        // etcd's defaults and its own tuning advice: heartbeat near the round-trip time, election
        // timeout 5-10x that. The timeout is drawn per election from [min, max) ticks of one
        // heartbeat each, which is where Raft's randomization comes from.
        b.raftHeartbeatMillis = intOrDefault(env, "LEDGERKV_RAFT_HEARTBEAT_MS", 100);
        b.raftElectionMinTicks = intOrDefault(env, "LEDGERKV_RAFT_ELECTION_MIN_TICKS", 10);
        b.raftElectionMaxTicks = intOrDefault(env, "LEDGERKV_RAFT_ELECTION_MAX_TICKS", 20);
        b.raftRpcDeadlineMillis = intOrDefault(env, "LEDGERKV_RAFT_RPC_DEADLINE_MS", 500);
        b.raftSnapshotThreshold = intOrDefault(env, "LEDGERKV_RAFT_SNAPSHOT_THRESHOLD", 10_000);
        b.raftMaxSessions = intOrDefault(env, "LEDGERKV_RAFT_MAX_SESSIONS", 4096);
        b.raftPeers = env.containsKey("LEDGERKV_RAFT_PEERS")
                ? parsePeers(require(env, "LEDGERKV_RAFT_PEERS"))
                : derivedRaftPeers(b.peers, b.raftPort);

        validate(b);
        return new NodeConfig(b);
    }

    /**
     * Raft peer endpoints default to each client peer's host paired with the Raft port, so a
     * deployment only sets {@code LEDGERKV_RAFT_PEERS} when consensus traffic takes a different
     * route than client traffic.
     */
    private static List<String> derivedRaftPeers(List<String> peers, int raftPort) {
        List<String> derived = new ArrayList<>();
        for (String peer : peers) {
            derived.add(peer.substring(0, peer.lastIndexOf(':')) + ":" + raftPort);
        }
        return derived;
    }

    private static void validate(Builder b) {
        if (b.peers.size() != b.nodeCount) {
            throw new IllegalArgumentException(
                    "peer count " + b.peers.size() + " does not match node count " + b.nodeCount);
        }
        if (b.nodeIndex < 0 || b.nodeIndex >= b.nodeCount) {
            throw new IllegalArgumentException(
                    "node index " + b.nodeIndex + " is out of range [0, " + b.nodeCount + ")");
        }
        if (b.mode != NodeMode.RAFT) {
            return;
        }
        if (b.nodeCount != 3 && b.nodeCount != 5) {
            throw new IllegalArgumentException("raft mode runs a fixed group of 3 or 5 members, got "
                    + b.nodeCount + "; an even group buys no extra fault tolerance");
        }
        if (b.raftPeers.size() != b.nodeCount) {
            throw new IllegalArgumentException("raft peer count " + b.raftPeers.size()
                    + " does not match node count " + b.nodeCount);
        }
        if (b.raftPort == b.grpcPort) {
            throw new IllegalArgumentException(
                    "raft peer traffic needs its own port, but both are " + b.raftPort);
        }
        if (b.raftElectionMinTicks < 2 || b.raftElectionMaxTicks <= b.raftElectionMinTicks) {
            throw new IllegalArgumentException("need 2 <= election min ticks < max ticks, got ["
                    + b.raftElectionMinTicks + ", " + b.raftElectionMaxTicks + ")");
        }
        // A peer RPC that can outlive an election cycle lets one unreachable follower hold a
        // replication slot across the very timeout that is supposed to notice it is gone.
        long shortestElection = (long) b.raftHeartbeatMillis * b.raftElectionMinTicks;
        if (b.raftRpcDeadlineMillis >= shortestElection) {
            throw new IllegalArgumentException("raft RPC deadline " + b.raftRpcDeadlineMillis
                    + "ms must be below the shortest election timeout " + shortestElection + "ms");
        }
        if (b.raftSnapshotThreshold < 1) {
            throw new IllegalArgumentException(
                    "raft snapshot threshold must be positive: " + b.raftSnapshotThreshold);
        }
        if (b.raftMaxSessions < 1) {
            throw new IllegalArgumentException(
                    "raft max sessions must be positive: " + b.raftMaxSessions);
        }
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
    public int primaryWriteQuorum() { return primaryWriteQuorum; }
    public int primaryReadQuorum() { return primaryReadQuorum; }
    public Duration requestDeadline() { return Duration.ofMillis(requestDeadlineMillis); }
    public Duration hedgingDelay() { return Duration.ofMillis(hedgingDelayMillis); }
    public List<String> peers() { return peers; }

    public NodeMode mode() { return mode; }
    public int raftPort() { return raftPort; }
    public List<String> raftPeers() { return raftPeers; }
    public Duration raftHeartbeat() { return Duration.ofMillis(raftHeartbeatMillis); }
    public int raftElectionMinTicks() { return raftElectionMinTicks; }
    public int raftElectionMaxTicks() { return raftElectionMaxTicks; }
    public Duration raftRpcDeadline() { return Duration.ofMillis(raftRpcDeadlineMillis); }
    public int raftSnapshotThreshold() { return raftSnapshotThreshold; }
    public int raftMaxSessions() { return raftMaxSessions; }

    public String nodeId() { return clusterId + "-node-" + nodeIndex; }

    /**
     * Where this node's Raft log and snapshots live: a subdirectory of the data dir, so a volume
     * can hold one mode's state without the other's half-written files sitting beside it.
     */
    public Path raftDataDir() { return dataDir.resolve("raft"); }

    /** The identity this node expects its data directory to already carry, if it carries one. */
    public ClusterIdentity identity() {
        return new ClusterIdentity(clusterId, nodeId(), mode);
    }

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
