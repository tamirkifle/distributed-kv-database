package com.ledgerkv.failure;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NetworkPartitionTest {

    @Test
    void blocksCommunicationAcrossPartitionGroupsUntilHealed() {
        NetworkPartition partition = NetworkPartition.partitioned(List.of(
            Set.of("eval-cluster-node-0", "eval-cluster-node-1"),
            Set.of("eval-cluster-node-2")
        ));

        assertTrue(partition.canCommunicate("eval-cluster-node-0", "eval-cluster-node-1"));
        assertFalse(partition.canCommunicate("eval-cluster-node-0", "eval-cluster-node-2"));
        assertFalse(partition.canCommunicate("eval-cluster-node-2", "eval-cluster-node-1"));

        partition.heal();

        assertTrue(partition.canCommunicate("eval-cluster-node-0", "eval-cluster-node-2"));
        assertTrue(partition.snapshot().isHealed());
    }

    @Test
    void exposesPartitionGroupsForTestsAndMetrics() {
        NetworkPartition partition = NetworkPartition.partitioned(List.of(
            Set.of("eval-cluster-node-0"),
            Set.of("eval-cluster-node-1", "eval-cluster-node-2")
        ));

        NetworkPartitionSnapshot snapshot = partition.snapshot();

        assertTrue(snapshot.isActive());
        assertEquals(2, snapshot.getPartitionGroups().size());
        assertEquals(0, snapshot.getGroupIndex("eval-cluster-node-0").orElseThrow());
        assertEquals(1, snapshot.getGroupIndex("eval-cluster-node-2").orElseThrow());
        assertTrue(snapshot.getGroupIndex("unknown-node").isEmpty());
    }

    @Test
    void healingDoesNotMutateCapturedPartitionSnapshot() {
        NetworkPartition partition = NetworkPartition.partitioned(List.of(
            Set.of("eval-cluster-node-0"),
            Set.of("eval-cluster-node-1")
        ));
        NetworkPartitionSnapshot beforeHeal = partition.snapshot();

        partition.heal();

        assertTrue(beforeHeal.isActive());
        assertEquals(2, beforeHeal.getPartitionGroups().size());
        assertTrue(partition.snapshot().isHealed());
        assertTrue(partition.canCommunicate("eval-cluster-node-0", "eval-cluster-node-1"));
    }

    @Test
    void rejectsInvalidPartitionGroups() {
        assertThrows(IllegalArgumentException.class,
            () -> NetworkPartition.partitioned(List.of(Set.of("node-0"))));
        assertThrows(IllegalArgumentException.class,
            () -> NetworkPartition.partitioned(List.of(Set.of("node-0"), Set.of("node-0"))));
        assertThrows(IllegalArgumentException.class,
            () -> NetworkPartition.partitioned(List.of(Set.of("node-0"), Set.of(" "))));
    }
}
