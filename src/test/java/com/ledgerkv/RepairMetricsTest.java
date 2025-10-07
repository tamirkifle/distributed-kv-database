package com.ledgerkv;

import com.ledgerkv.metrics.RepairMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepairMetricsTest {

    private QuorumKVStoreWithRepair cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("Repair metrics value object computes deterministic latency summary")
    void repairMetricsComputesLatencySummary() {
        RepairMetrics metrics = new RepairMetrics(3, 7, 15);

        assertEquals(3, metrics.getRepairCount());
        assertEquals(7, metrics.getReplicasRepaired());
        assertEquals(15, metrics.getTotalRepairLatencyMs());
        assertEquals(5, metrics.getAverageRepairLatencyMs());
    }

    @Test
    @DisplayName("Empty repair metrics report zero latency")
    void emptyRepairMetricsReportZeroLatency() {
        RepairMetrics metrics = RepairMetrics.empty();

        assertEquals(0, metrics.getRepairCount());
        assertEquals(0, metrics.getReplicasRepaired());
        assertEquals(0, metrics.getTotalRepairLatencyMs());
        assertEquals(0, metrics.getAverageRepairLatencyMs());
    }

    @Test
    @DisplayName("Quorum store exposes repair metrics snapshot")
    void quorumStoreExposesRepairMetricsSnapshot() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 5);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.ALWAYS);

        cluster.getNodes().get(0).set("key1", "value1");
        cluster.getNodes().get(1).set("key1", "value1");
        cluster.getNodes().get(2).set("key1", "value1");

        QuorumResponse response = cluster.read("key1");

        assertTrue(response.isSuccessful());
        assertTrue(response.hasInconsistency());
        assertTrue(eventuallyRepairsReplicas());

        RepairMetrics metrics = cluster.getRepairMetrics();
        assertEquals(1, metrics.getRepairCount());
        assertTrue(metrics.getReplicasRepaired() > 0);
        assertTrue(metrics.getTotalRepairLatencyMs() >= 0);
        assertTrue(metrics.getAverageRepairLatencyMs() >= 0);
        assertEquals(cluster.getRepairsTriggered(), metrics.getRepairCount());
        assertEquals(cluster.getNodesRepaired(), metrics.getReplicasRepaired());
    }

    private boolean eventuallyRepairsReplicas() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            if (cluster.getNodesRepaired() > 0) {
                return true;
            }
            Thread.sleep(10);
        }
        return cluster.getNodesRepaired() > 0;
    }
}
