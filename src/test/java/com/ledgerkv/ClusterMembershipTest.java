package com.ledgerkv;

import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.ClusterNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterMembershipTest {

    @Test
    void createsClusterWithStableNodeIds() {
        ClusterMembership membership = ClusterMembership.create("eval-cluster", 3, 2);

        List<ClusterNode> nodes = membership.getNodes();

        assertEquals(3, nodes.size());
        assertEquals("eval-cluster-node-0", nodes.get(0).getId());
        assertEquals("eval-cluster-node-1", nodes.get(1).getId());
        assertEquals("eval-cluster-node-2", nodes.get(2).getId());
        assertEquals(List.of("eval-cluster-node-0", "eval-cluster-node-1", "eval-cluster-node-2"),
            membership.getNodeIds());
    }

    @Test
    void selectsDeterministicReplicaSetForKey() {
        ClusterMembership membership = ClusterMembership.create("eval-cluster", 5, 3);

        List<ClusterNode> firstSelection = membership.selectReplicas("trace:run-001");
        List<ClusterNode> secondSelection = membership.selectReplicas("trace:run-001");
        List<ClusterNode> differentKeySelection = membership.selectReplicas("trace:run-002");

        assertEquals(3, firstSelection.size());
        assertEquals(firstSelection, secondSelection, "Replica selection should be stable for a key");
        assertNotEquals(firstSelection, differentKeySelection,
            "Different keys should be able to map to different replica sets");
    }

    @Test
    void wrapsReplicaSelectionAroundNodeRing() {
        ClusterMembership membership = ClusterMembership.create("cluster", 3, 3);

        List<ClusterNode> replicas = membership.selectReplicas("key-near-end-of-ring");

        assertEquals(3, replicas.size());
        assertEquals(3, replicas.stream().map(ClusterNode::getId).distinct().count(),
            "Replica selection should not duplicate nodes while wrapping");
    }

    @Test
    void rejectsInvalidMembershipConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> ClusterMembership.create("", 3, 2));
        assertThrows(IllegalArgumentException.class, () -> ClusterMembership.create("cluster", 0, 1));
        assertThrows(IllegalArgumentException.class, () -> ClusterMembership.create("cluster", 3, 0));
        assertThrows(IllegalArgumentException.class, () -> ClusterMembership.create("cluster", 3, 4));
        assertThrows(IllegalArgumentException.class,
            () -> ClusterMembership.create("cluster", 3, 2).selectReplicas(""));
    }
}
