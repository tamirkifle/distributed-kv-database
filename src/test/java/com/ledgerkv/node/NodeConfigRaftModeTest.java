package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Mode selection and the Raft-only settings. The validations here are the ones that turn a
 * mis-sized or mis-timed Raft deployment into a refusal to start, rather than a cluster that comes
 * up and then behaves strangely under a fault.
 */
class NodeConfigRaftModeTest {

    private static Map<String, String> raftEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("LEDGERKV_MODE", "raft");
        env.put("LEDGERKV_NODE_INDEX", "1");
        env.put("LEDGERKV_PEERS", "node0:9090,node1:9090,node2:9090");
        return env;
    }

    @Test
    void defaultsToQuorumSoExistingDeploymentsAreUnchanged() {
        Map<String, String> env = new HashMap<>();
        env.put("LEDGERKV_NODE_INDEX", "0");
        env.put("LEDGERKV_PEERS", "node0:9090,node1:9090,node2:9090,node3:9090,node4:9090");
        assertEquals(NodeMode.QUORUM, NodeConfig.fromEnv(env).mode());
    }

    @Test
    void parsesModeCaseInsensitively() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_MODE", "RaFt");
        assertEquals(NodeMode.RAFT, NodeConfig.fromEnv(env).mode());
    }

    @Test
    void rejectsAnUnknownMode() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_MODE", "paxos");
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
        assertTrue(e.getMessage().contains("quorum|raft"), e.getMessage());
    }

    @Test
    void raftPeersDefaultToTheClientHostsOnTheRaftPort() {
        NodeConfig config = NodeConfig.fromEnv(raftEnv());
        assertEquals(9095, config.raftPort());
        assertEquals(List.of("node0:9095", "node1:9095", "node2:9095"), config.raftPeers());
    }

    @Test
    void raftPeersCanBeGivenExplicitly() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_RAFT_PEERS", "a.internal:7000,b.internal:7000,c.internal:7000");
        assertEquals(List.of("a.internal:7000", "b.internal:7000", "c.internal:7000"),
                NodeConfig.fromEnv(env).raftPeers());
    }

    @Test
    void raftStateLivesBesideRatherThanInsideTheLsmDataDir() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_DATA_DIR", "/data");
        assertEquals("/data/raft", NodeConfig.fromEnv(env).raftDataDir().toString());
    }

    @Test
    void raftGroupsMustBeThreeOrFiveMembers() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_PEERS", "node0:9090,node1:9090,node2:9090,node3:9090");
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
        assertTrue(e.getMessage().contains("3 or 5"), e.getMessage());
    }

    @Test
    void evenSizedGroupsAreOnlyRejectedInRaftMode() {
        Map<String, String> env = raftEnv();
        env.remove("LEDGERKV_MODE");
        env.put("LEDGERKV_PEERS", "node0:9090,node1:9090,node2:9090,node3:9090");
        assertEquals(4, NodeConfig.fromEnv(env).nodeCount(), "quorum mode sizes itself by N/R/W");
    }

    @Test
    void raftPeerTrafficNeedsItsOwnPort() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_RAFT_PORT", "9090");
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
    }

    @Test
    void rpcDeadlineMustStayBelowTheShortestElectionTimeout() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_RAFT_HEARTBEAT_MS", "100");
        env.put("LEDGERKV_RAFT_ELECTION_MIN_TICKS", "10");
        env.put("LEDGERKV_RAFT_RPC_DEADLINE_MS", "1000"); // exactly one election timeout
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
        assertTrue(e.getMessage().contains("must be below"), e.getMessage());
    }

    @Test
    void electionWindowMustBeAWidenedRange() {
        Map<String, String> env = raftEnv();
        env.put("LEDGERKV_RAFT_ELECTION_MIN_TICKS", "10");
        env.put("LEDGERKV_RAFT_ELECTION_MAX_TICKS", "10");
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env),
                "a fixed timeout gives every node the same deadline and splits the vote");
    }

    @Test
    void appliesRaftTimingDefaults() {
        NodeConfig config = NodeConfig.fromEnv(raftEnv());
        assertEquals(100, config.raftHeartbeat().toMillis());
        assertEquals(10, config.raftElectionMinTicks());
        assertEquals(20, config.raftElectionMaxTicks());
        assertEquals(500, config.raftRpcDeadline().toMillis());
        assertEquals(10_000, config.raftSnapshotThreshold());
        assertEquals(4096, config.raftMaxSessions());
    }
}
