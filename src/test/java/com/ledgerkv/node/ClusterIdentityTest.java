package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClusterIdentityTest {

    private static ClusterIdentity raftNode(String nodeId) {
        return new ClusterIdentity("ledgerkv", nodeId, NodeMode.RAFT);
    }

    @Test
    void adoptsADirectoryThatCarriesNoMarkerYet(@TempDir Path dir) throws IOException {
        ClusterIdentity claimed = ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0"));

        assertEquals("ledgerkv-node-0", claimed.nodeId());
        assertEquals(NodeMode.RAFT, claimed.mode());
        assertTrue(Files.exists(dir.resolve("cluster-identity.properties")));
    }

    @Test
    void reclaimingWithTheSameIdentityIsFine(@TempDir Path dir) throws IOException {
        ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0"));
        assertEquals(NodeMode.RAFT, ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0")).mode());
    }

    @Test
    void refusesADirectoryLastUsedByTheOtherMode(@TempDir Path dir) throws IOException {
        ClusterIdentity.claim(dir, new ClusterIdentity("ledgerkv", "ledgerkv-node-0", NodeMode.QUORUM));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0")));
        assertTrue(e.getMessage().contains("mode=quorum"), e.getMessage());
    }

    @Test
    void refusesAVolumeBelongingToAnotherNode(@TempDir Path dir) throws IOException {
        ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0"));
        assertThrows(IllegalStateException.class,
                () -> ClusterIdentity.claim(dir, raftNode("ledgerkv-node-3")),
                "a volume reattached to the wrong ordinal would otherwise serve another node's log");
    }

    @Test
    void refusesAVolumeBelongingToAnotherCluster(@TempDir Path dir) throws IOException {
        ClusterIdentity.claim(dir, raftNode("ledgerkv-node-0"));
        assertThrows(IllegalStateException.class, () -> ClusterIdentity.claim(
                dir, new ClusterIdentity("other", "ledgerkv-node-0", NodeMode.RAFT)));
    }
}
