package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NodeConfigTest {

    private Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("LEDGERKV_NODE_INDEX", "2");
        env.put("LEDGERKV_PEERS", "node0:9090,node1:9090,node2:9090,node3:9090,node4:9090");
        return env;
    }

    @Test
    void parsesPeersIndexAndDerivedNodeId() {
        NodeConfig config = NodeConfig.fromEnv(baseEnv());
        assertEquals("ledgerkv", config.clusterId());
        assertEquals(2, config.nodeIndex());
        assertEquals(5, config.nodeCount());
        assertEquals("ledgerkv-node-2", config.nodeId());
        assertEquals(List.of("node0:9090", "node1:9090", "node2:9090", "node3:9090", "node4:9090"),
                config.peers());
    }

    @Test
    void appliesDefaultsForOptionalSettings() {
        NodeConfig config = NodeConfig.fromEnv(baseEnv());
        assertEquals(9090, config.grpcPort());
        assertEquals(8080, config.healthPort());
        assertEquals("/data", config.dataDir().toString());
        assertEquals(3, config.replicationFactor());
        assertEquals(2, config.writeQuorum());
        assertEquals(2, config.readQuorum());
    }

    @Test
    void honoursExplicitOverrides() {
        Map<String, String> env = baseEnv();
        env.put("LEDGERKV_CLUSTER_ID", "demo");
        env.put("LEDGERKV_GRPC_PORT", "7000");
        env.put("LEDGERKV_HEALTH_PORT", "7001");
        env.put("LEDGERKV_DATA_DIR", "/var/ledgerkv");
        env.put("LEDGERKV_REPLICATION_FACTOR", "3");
        env.put("LEDGERKV_WRITE_QUORUM", "2");
        env.put("LEDGERKV_READ_QUORUM", "2");
        NodeConfig config = NodeConfig.fromEnv(env);
        assertEquals("demo-node-2", config.nodeId());
        assertEquals(7000, config.grpcPort());
        assertEquals(7001, config.healthPort());
        assertEquals("/var/ledgerkv", config.dataDir().toString());
    }

    @Test
    void rejectsMissingNodeIndex() {
        Map<String, String> env = baseEnv();
        env.remove("LEDGERKV_NODE_INDEX");
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
    }

    @Test
    void rejectsNodeIndexOutOfRange() {
        Map<String, String> env = baseEnv();
        env.put("LEDGERKV_NODE_INDEX", "5");
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.fromEnv(env));
    }

    @Test
    void parsesHedgingDelayFromEnv() {
        Map<String, String> env = baseEnv();
        env.put("LEDGERKV_HEDGING_DELAY_MS", "30");
        NodeConfig config = NodeConfig.fromEnv(env);
        assertEquals(java.time.Duration.ofMillis(30), config.hedgingDelay());
    }

    @Test
    void hedgingDelayDefaultsTo50ms() {
        NodeConfig config = NodeConfig.fromEnv(baseEnv());
        assertEquals(java.time.Duration.ofMillis(50), config.hedgingDelay());
    }
}
