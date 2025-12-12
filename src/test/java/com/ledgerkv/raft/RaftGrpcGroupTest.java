package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RaftGrpcGroupTest {

    @Test
    void electsReplicatesAndSurvivesLeaderLoss() throws Exception {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        Map<String, Integer> timeouts = new HashMap<>();
        timeouts.put("n0", 2);   // lowest -> first leader
        timeouts.put("n1", 8);
        timeouts.put("n2", 12);

        try (RaftCluster cluster = new RaftCluster(ids, timeouts)) {
            // Elect n0.
            String leader = cluster.awaitLeader("n0", 5);
            assertEquals("n0", leader);
            assertTrue(cluster.node("n0").isLeader());

            // Replicate a command over real gRPC; heartbeats carry the commit to followers.
            assertTrue(cluster.node("n0").propose("set k=1".getBytes()) > 0);
            cluster.tickLeader("n0", 3);
            assertEquals(List.of("set k=1"), cluster.appliedAt("n1"));
            assertEquals(List.of("set k=1"), cluster.appliedAt("n2"));

            // Leader loss: crash n0; a survivor (n1, next-lowest timeout) takes over.
            cluster.crash("n0");
            String newLeader = cluster.awaitLeader("n1", 12);
            assertEquals("n1", newLeader);

            // A fresh write commits on the surviving majority {n1, n2}.
            assertTrue(cluster.node("n1").propose("set k=2".getBytes()) > 0);
            cluster.tickLeader("n1", 3);
            assertEquals(List.of("set k=1", "set k=2"), cluster.appliedAt("n2"));
        }
    }
}
