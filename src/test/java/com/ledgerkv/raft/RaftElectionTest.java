package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;

class RaftElectionTest {

    private static StateMachine noop() {
        return command -> command;
    }

    private static IntSupplier fixed(int t) {
        return () -> t;
    }

    /** Wire every node to every other node via InProcessRaftPeer; returns the node map. */
    private Map<String, RaftNode> cluster(List<String> ids, Map<String, Integer> timeouts,
            AtomicBoolean reachable) {
        Map<String, RaftNode> nodes = new HashMap<>();
        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            nodes.put(id, new RaftNode(id, peers, noop(), fixed(timeouts.get(id))));
        }
        for (String id : ids) {
            RaftNode self = nodes.get(id);
            for (String other : ids) {
                if (!other.equals(id)) {
                    RaftNode target = nodes.get(other);
                    self.registerPeer(new InProcessRaftPeer(target, reachable::get));
                }
            }
        }
        return nodes;
    }

    @Test
    void singleNodeElectsItself() {
        RaftNode solo = new RaftNode("solo", Collections.emptyList(), noop(), fixed(3));
        for (int i = 0; i < 3; i++) {
            solo.tick();
        }
        assertTrue(solo.isLeader());
        assertEquals(1, solo.currentTerm());
    }

    @Test
    void earliestTimeoutWinsElectionInThreeNodeGroup() {
        AtomicBoolean reachable = new AtomicBoolean(true);
        Map<String, Integer> timeouts = new HashMap<>();
        timeouts.put("n0", 2); // shortest timeout: n0 starts the election first
        timeouts.put("n1", 9);
        timeouts.put("n2", 9);
        Map<String, RaftNode> nodes = cluster(Arrays.asList("n0", "n1", "n2"), timeouts, reachable);

        for (int i = 0; i < 2; i++) {
            nodes.values().forEach(RaftNode::tick);
        }
        assertTrue(nodes.get("n0").isLeader(), "n0 should win");
        assertEquals(RaftRole.FOLLOWER, nodes.get("n1").role());
        assertEquals(RaftRole.FOLLOWER, nodes.get("n2").role());
    }

    @Test
    void partitionedCandidateCannotWinUntilHealed() {
        AtomicBoolean reachable = new AtomicBoolean(false); // n0 cut off from peers
        Map<String, Integer> timeouts = new HashMap<>();
        timeouts.put("n0", 2);
        timeouts.put("n1", 50);
        timeouts.put("n2", 50);
        Map<String, RaftNode> nodes = cluster(Arrays.asList("n0", "n1", "n2"), timeouts, reachable);

        for (int i = 0; i < 2; i++) {
            nodes.get("n0").tick();
        }
        assertFalse(nodes.get("n0").isLeader(), "partitioned candidate must not become leader");
        assertEquals(RaftRole.CANDIDATE, nodes.get("n0").role());

        // heal and let n0 retry its election
        reachable.set(true);
        for (int i = 0; i < 2; i++) {
            nodes.get("n0").tick();
        }
        assertTrue(nodes.get("n0").isLeader(), "after healing, n0 wins");
    }
}
