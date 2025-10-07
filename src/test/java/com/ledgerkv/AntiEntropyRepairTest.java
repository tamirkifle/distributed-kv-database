package com.ledgerkv;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AntiEntropyRepairTest {

    private QuorumKVStoreWithRepair cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("Anti-entropy repairs cold keys after partition healing")
    void antiEntropyRepairsColdKeysAfterPartitionHealing() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 3, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.NONE);

        cluster.simulatePartition(Set.of(3, 4));
        QuorumResponse writeResponse = cluster.write("cold-key", "value-after-partition");
        assertTrue(writeResponse.isSuccessful());

        cluster.healPartition();
        assertEquals(3, countReplicasWithValue("cold-key", "value-after-partition"),
            "Partitioned replicas should still be missing the cold key before anti-entropy runs");

        cluster.runAntiEntropy();

        assertTrue(eventuallyAllReplicasHaveValue("cold-key", "value-after-partition"),
            "Anti-entropy should repair the cold key without requiring a read");
        assertEquals(2, cluster.getNodesRepaired());
    }

    private boolean eventuallyAllReplicasHaveValue(String key, String expectedValue) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            if (countReplicasWithValue(key, expectedValue) == 5) {
                return true;
            }
            Thread.sleep(10);
        }
        return countReplicasWithValue(key, expectedValue) == 5;
    }

    private int countReplicasWithValue(String key, String expectedValue) {
        int replicas = 0;
        for (VersionedKVStore node : cluster.getNodes()) {
            Optional<VersionedValue> value = node.get(key);
            if (value.isPresent() && expectedValue.equals(value.get().getValue())) {
                replicas++;
            }
        }
        return replicas;
    }
}
