package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Who each node thinks the leader is. Nothing in Raft needs a follower to remember the leader id,
 * but leader-aware client routing does: a follower that cannot name the leader can only tell a
 * client to try somewhere else at random.
 */
class RaftLeaderIdentityTest {

    private static Map<String, RaftNode> group(List<String> ids, String earlyTimeout) {
        Map<String, RaftNode> nodes = new LinkedHashMap<>();
        for (String id : ids) {
            List<String> others = new ArrayList<>(ids);
            others.remove(id);
            nodes.put(id, new RaftNode(id, others, new RaftCluster.Recorder(),
                    () -> id.equals(earlyTimeout) ? 2 : 50));
        }
        for (String from : ids) {
            for (String to : ids) {
                if (!from.equals(to)) {
                    nodes.get(from).registerPeer(new InProcessRaftPeer(nodes.get(to), () -> true));
                }
            }
        }
        return nodes;
    }

    @Test
    void followersLearnTheLeaderFromItsHeartbeat() {
        List<String> ids = List.of("n0", "n1", "n2");
        Map<String, RaftNode> nodes = group(ids, "n0");
        for (String id : ids) {
            assertNull(nodes.get(id).leaderId(), id + " knows no leader before any election");
        }

        RaftNode first = nodes.get("n0");
        first.tick();
        first.tick();
        assertTrue(first.isLeader());

        assertEquals("n0", first.leaderId(), "a leader names itself");
        assertEquals("n0", nodes.get("n1").leaderId());
        assertEquals("n0", nodes.get("n2").leaderId());
    }

    @Test
    void steppingDownToAHigherTermForgetsTheLeader() {
        List<String> ids = List.of("n0", "n1", "n2");
        Map<String, RaftNode> nodes = group(ids, "n0");
        RaftNode first = nodes.get("n0");
        first.tick();
        first.tick();
        assertEquals("n0", nodes.get("n1").leaderId());

        // A candidate from a later term supersedes n0 without itself being the leader yet.
        RaftNode follower = nodes.get("n1");
        follower.handleRequestVote(
                RequestVoteRequest.of(first.currentTerm() + 5, "n2", 0, 0));

        assertNull(follower.leaderId(),
                "a later term has no leader until one heartbeats; a stale id would misroute clients");
    }
}
