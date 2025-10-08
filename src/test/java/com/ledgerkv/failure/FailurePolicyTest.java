package com.ledgerkv.failure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FailurePolicyTest {

    @Test
    void tracksUnavailableNodesInObservableState() {
        FailurePolicy policy = FailurePolicy.builder(42L)
            .unavailableNode("test-cluster-node-1")
            .unavailableNode("test-cluster-node-2")
            .build();

        assertTrue(policy.isNodeAvailable("test-cluster-node-0"));
        assertFalse(policy.isNodeAvailable("test-cluster-node-1"));
        assertFalse(policy.isNodeAvailable("test-cluster-node-2"));
        assertEquals(
            2,
            policy.snapshot().getUnavailableNodeIds().size()
        );
        assertTrue(policy.snapshot().getUnavailableNodeIds().contains("test-cluster-node-1"));
    }

    @Test
    void exposesFixedLatencyPerDestinationNode() {
        FailurePolicy policy = FailurePolicy.builder(7L)
            .fixedLatency("test-cluster-node-1", 25)
            .fixedLatency("test-cluster-node-2", 80)
            .build();

        assertEquals(0, policy.latencyMsFor("test-cluster-node-0"));
        assertEquals(25, policy.latencyMsFor("test-cluster-node-1"));
        assertEquals(80, policy.latencyMsFor("test-cluster-node-2"));
        assertEquals(25, policy.snapshot().getFixedLatencyByNodeId().get("test-cluster-node-1"));
    }

    @Test
    void makesDeterministicDropDecisionsFromSeedAndMessageIdentity() {
        FailurePolicy first = FailurePolicy.builder(1234L)
            .messageDropRate(0.50)
            .build();
        FailurePolicy second = FailurePolicy.builder(1234L)
            .messageDropRate(0.50)
            .build();

        for (int i = 0; i < 100; i++) {
            String messageId = "op-" + i;
            assertEquals(
                first.shouldDropMessage(messageId, "test-cluster-node-0", "test-cluster-node-1"),
                second.shouldDropMessage(messageId, "test-cluster-node-0", "test-cluster-node-1")
            );
        }
    }

    @Test
    void respectsDropRateExtremes() {
        FailurePolicy neverDrop = FailurePolicy.builder(1L)
            .messageDropRate(0.0)
            .build();
        FailurePolicy alwaysDrop = FailurePolicy.builder(1L)
            .messageDropRate(1.0)
            .build();

        assertFalse(neverDrop.shouldDropMessage("op-1", "node-0", "node-1"));
        assertTrue(alwaysDrop.shouldDropMessage("op-1", "node-0", "node-1"));
        assertEquals(1.0, alwaysDrop.snapshot().getMessageDropRate());
        assertEquals(1L, alwaysDrop.snapshot().getSeed());
    }

    @Test
    void rejectsInvalidFailureConfiguration() {
        assertThrows(IllegalArgumentException.class,
            () -> FailurePolicy.builder(1L).unavailableNode(" "));
        assertThrows(IllegalArgumentException.class,
            () -> FailurePolicy.builder(1L).fixedLatency("node-1", -1));
        assertThrows(IllegalArgumentException.class,
            () -> FailurePolicy.builder(1L).messageDropRate(1.1));
    }
}
