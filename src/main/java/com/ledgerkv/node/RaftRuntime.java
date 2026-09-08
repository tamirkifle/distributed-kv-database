package com.ledgerkv.node;

import com.ledgerkv.metrics.RaftStatus;
import com.ledgerkv.raft.GrpcRaftPeer;
import com.ledgerkv.raft.RaftClient;
import com.ledgerkv.raft.RaftNode;
import com.ledgerkv.raft.RaftPeer;
import com.ledgerkv.raft.RaftPersistence;
import com.ledgerkv.raft.RaftReplicationDriver;
import com.ledgerkv.raft.RaftServer;
import com.ledgerkv.raft.RaftState;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One node's Raft mode: durable state replayed from disk, a state machine, a {@link RaftNode}, a
 * {@link RaftServer} on the peer port, gRPC peers to the rest of the group, and a
 * {@link RaftReplicationDriver} running the whole thing on its own threads.
 *
 * <h2>Recovery</h2>
 *
 * <p>{@link RaftPersistence#replay} returns the last snapshot plus every log entry that was written
 * after it. Those entries are <em>persisted</em>, which is not the same as committed: a node can
 * crash holding a suffix its leader never committed, and a later leader may replace it. So recovery
 * restores the state machine only to the snapshot, and marks committed and applied only up to the
 * snapshot's last included index. Everything above is left in the log unapplied, to be committed or
 * overwritten once a leader is elected. {@link RaftNode}'s recovery constructor already does this;
 * the point of saying so here is that it is deliberate, not an omission.
 */
public final class RaftRuntime implements AutoCloseable {

    private final RaftNode node;
    private final RaftKvStateMachine state;
    private final RaftReplicationDriver driver;
    private final RaftServer server;
    private final List<RaftClient> clients;
    private final Map<String, String> endpoints;

    private RaftRuntime(RaftNode node, RaftKvStateMachine state, RaftReplicationDriver driver,
            RaftServer server, List<RaftClient> clients, Map<String, String> endpoints) {
        this.node = node;
        this.state = state;
        this.driver = driver;
        this.server = server;
        this.clients = clients;
        this.endpoints = endpoints;
    }

    /**
     * Claims the data directory, replays durable state, and starts consensus. The peer port is
     * separate from the client port so consensus traffic and client traffic cannot starve each
     * other on one server's thread pool.
     */
    public static RaftRuntime start(NodeConfig config) throws IOException {
        ClusterIdentity.claim(config.dataDir(), config.identity());

        RaftState recovered = RaftPersistence.replay(config.raftDataDir());
        RaftPersistence persistence = RaftPersistence.open(config.raftDataDir());

        RaftKvStateMachine state = new RaftKvStateMachine(config.raftMaxSessions());
        List<String> peerIds = memberIds(config);
        String selfId = config.nodeId();
        peerIds.remove(selfId);

        int minTicks = config.raftElectionMinTicks();
        int maxTicks = config.raftElectionMaxTicks();
        RaftNode node = new RaftNode(selfId, peerIds, state,
                () -> ThreadLocalRandom.current().nextInt(minTicks, maxTicks),
                persistence, recovered);
        node.setCompactionThreshold(config.raftSnapshotThreshold());

        RaftServer server = RaftServer.create(config.raftPort(), node).start();

        List<RaftClient> clients = new ArrayList<>();
        List<RaftPeer> peers = new ArrayList<>();
        List<String> raftPeers = config.raftPeers();
        List<String> allIds = memberIds(config);
        for (int i = 0; i < allIds.size(); i++) {
            String id = allIds.get(i);
            if (id.equals(selfId)) {
                continue;
            }
            String[] hostPort = raftPeers.get(i).split(":", 2);
            RaftClient client = RaftClient.connect(
                    hostPort[0], Integer.parseInt(hostPort[1]), config.raftRpcDeadline());
            clients.add(client);
            RaftPeer peer = new GrpcRaftPeer(id, client);
            node.registerPeer(peer);
            peers.add(peer);
        }

        RaftReplicationDriver driver =
                new RaftReplicationDriver(node, peers, config.raftHeartbeat()).start();

        return new RaftRuntime(node, state, driver, server, clients, clientEndpoints(config));
    }

    /** Member ids in peer-list order, so index i names the member at {@code peers().get(i)}. */
    private static List<String> memberIds(NodeConfig config) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < config.nodeCount(); i++) {
            ids.add(config.clusterId() + "-node-" + i);
        }
        return ids;
    }

    /** Node id to <em>client</em> endpoint, which is what a leader hint has to point a client at. */
    private static Map<String, String> clientEndpoints(NodeConfig config) {
        Map<String, String> endpoints = new LinkedHashMap<>();
        List<String> ids = memberIds(config);
        for (int i = 0; i < ids.size(); i++) {
            endpoints.put(ids.get(i), config.peers().get(i));
        }
        return endpoints;
    }

    public RaftNode node() {
        return node;
    }

    public RaftKvStateMachine state() {
        return state;
    }

    public RaftReplicationDriver driver() {
        return driver;
    }

    /** The Raft peer port actually bound (resolves an OS-assigned port 0). */
    public int raftPort() {
        return server.port();
    }

    public Map<String, String> clientEndpoints() {
        return endpoints;
    }

    /** This member's current Raft state, for the {@code /metrics} endpoint. */
    public RaftStatus status() {
        return new RaftStatus(
                node.role().name(),
                node.leaderId(),
                node.currentTerm(),
                node.commitIndex(),
                node.lastApplied(),
                node.lastIncludedIndex());
    }

    /**
     * Whether this node can currently take part in serving requests: it either leads, or it knows
     * who does and can redirect. During an election it can do neither, and says so.
     */
    public boolean ready() {
        return node.leaderId() != null;
    }

    /**
     * Stops in the order that avoids work being handed to something already shut: replication
     * first, then peer channels, then the inbound server, and only then the durable log.
     */
    @Override
    public void close() {
        driver.close();
        for (RaftClient client : clients) {
            try {
                client.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            server.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            node.closePersistence();
        } catch (IOException e) {
            throw new IllegalStateException("could not close the raft log cleanly", e);
        }
    }
}
