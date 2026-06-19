package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.raft.kv.RaftKvClient;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The ReadIndex leader barrier (Raft paper §8).
 *
 * <p>The review's counterexample asserted that a read through an isolated former leader returns
 * {@code v2}. It cannot: that node is partitioned from every node holding {@code v2}, so no correct
 * implementation can produce the value. The review's own fix direction says "rejection or
 * redirection when leadership cannot be established", and that is what is asserted here — the read
 * fails loudly instead of silently serving stale data.
 */
class RaftReadIndexTest {

    /**
     * {@code RaftKvClient.get} drained only the node's locally known commit index
     * and read its state machine, without establishing that the node was still leader. An isolated
     * former leader therefore returned a value that a newer leader had already superseded.
     */
    @Test
    void isolatedFormerLeaderMustNotServeAStaleRead() {
        Map<String, RaftNode> nodes = new LinkedHashMap<>();
        Map<String, RaftKvStateMachine> stateMachines = new HashMap<>();
        AtomicBoolean partitioned = new AtomicBoolean();
        List<String> ids = List.of("n0", "n1", "n2");
        for (String id : ids) {
            List<String> others = new ArrayList<>(ids);
            others.remove(id);
            RaftKvStateMachine sm = new RaftKvStateMachine();
            stateMachines.put(id, sm);
            nodes.put(id, new RaftNode(id, others, sm, () -> id.equals("n0") ? 2 : 50));
        }
        for (String from : ids) {
            for (String to : ids) {
                if (!from.equals(to)) {
                    nodes.get(from).registerPeer(new InProcessRaftPeer(nodes.get(to),
                        () -> !partitioned.get() || (!from.equals("n0") && !to.equals("n0"))));
                }
            }
        }

        RaftNode first = nodes.get("n0");
        first.tick();
        first.tick();
        assertTrue(first.isLeader());
        RaftKvClient oldClient =
            new RaftKvClient("old-client", first, stateMachines.get("n0"), first::tick);
        oldClient.put("k", "v1".getBytes(UTF_8));
        first.tick();

        partitioned.set(true);
        RaftNode second = nodes.get("n1");
        for (int i = 0; i < 50; i++) {
            second.tick();
        }
        assertTrue(second.isLeader());
        assertNotEquals(first.currentTerm(), second.currentTerm());
        RaftKvClient newClient =
            new RaftKvClient("new-client", second, stateMachines.get("n1"), second::tick);
        newClient.put("k", "v2".getBytes(UTF_8));
        assertEquals("v2", new String(newClient.get("k"), UTF_8));

        // n0 still believes it is leader, and its state machine still holds v1. The ReadIndex round
        // cannot reach a majority, so the read must be refused rather than answered from stale state.
        assertTrue(first.isLeader(), "the isolated node has not learned of the new term");
        IllegalStateException refused =
            assertThrows(IllegalStateException.class, () -> oldClient.get("k"));
        assertTrue(refused.getMessage().contains("leadership"),
            "expected a leadership-confirmation failure, got: " + refused.getMessage());
    }

    /** A healthy leader confirms leadership against a majority and serves the read. */
    @Test
    void healthyLeaderServesTheReadAfterConfirmingLeadership() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        RaftNode leader = new RaftNode("n0", List.of("n1", "n2"), sm, () -> 1);
        for (String id : List.of("n1", "n2")) {
            List<String> peers = new ArrayList<>(List.of("n0", "n1", "n2"));
            peers.remove(id);
            leader.registerPeer(new InProcessRaftPeer(
                new RaftNode(id, peers, new RaftKvStateMachine(), () -> 100)));
        }
        leader.tick();
        assertTrue(leader.isLeader());

        RaftKvClient client = new RaftKvClient("c", leader, sm, leader::tick);
        client.put("k", "v1".getBytes(UTF_8));

        assertEquals("v1", new String(client.get("k"), UTF_8));
    }

    /**
     * A leader that can still reach one of two peers keeps its majority, so reads stay available
     * with a minority partitioned away.
     */
    @Test
    void leaderWithOneReachablePeerStillServesReads() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        RaftNode leader = new RaftNode("n0", List.of("n1", "n2"), sm, () -> 1);
        AtomicBoolean n2Reachable = new AtomicBoolean(true);
        RaftNode n1 = new RaftNode("n1", List.of("n0", "n2"), new RaftKvStateMachine(), () -> 100);
        RaftNode n2 = new RaftNode("n2", List.of("n0", "n1"), new RaftKvStateMachine(), () -> 100);
        leader.registerPeer(new InProcessRaftPeer(n1));
        leader.registerPeer(new InProcessRaftPeer(n2, n2Reachable::get));
        leader.tick();
        assertTrue(leader.isLeader());
        RaftKvClient client = new RaftKvClient("c", leader, sm, leader::tick);
        client.put("k", "v1".getBytes(UTF_8));

        n2Reachable.set(false);

        assertEquals("v1", new String(client.get("k"), UTF_8));
    }

    /**
     * Raft §8: a leader cannot trust its commit index until it has committed an entry from its own
     * term, so {@code becomeLeader} appends a no-op barrier. That entry is a log record only — it is
     * never handed to the state machine.
     */
    @Test
    void newLeaderCommitsANoOpBarrierThatTheStateMachineNeverSees() {
        RaftReviewRegressionTest.RecordingStateMachine sm =
            new RaftReviewRegressionTest.RecordingStateMachine();
        RaftNode leader = new RaftNode("n0", List.of("n1", "n2"), sm, () -> 1);
        for (String id : List.of("n1", "n2")) {
            List<String> peers = new ArrayList<>(List.of("n0", "n1", "n2"));
            peers.remove(id);
            leader.registerPeer(new InProcessRaftPeer(new RaftNode(
                id, peers, new RaftReviewRegressionTest.RecordingStateMachine(), () -> 100)));
        }

        leader.tick();

        assertTrue(leader.isLeader());
        assertEquals(1, leader.log().lastIndex(), "the no-op occupies index 1");
        assertEquals(1, leader.commitIndex(), "the barrier commits against the majority");
        assertEquals(1, leader.lastApplied());
        assertTrue(sm.applied.isEmpty(), "a no-op barrier must not reach the state machine");

        assertEquals(2, leader.propose("c1".getBytes(UTF_8)));
        assertEquals(List.of("c1"), sm.applied);
    }

    /** A follower refuses a linearizable read outright rather than serving local state. */
    @Test
    void followerRefusesALinearizableRead() {
        RaftKvStateMachine sm = new RaftKvStateMachine();
        RaftNode follower = new RaftNode("n1", List.of("n0", "n2"), sm, () -> 100);
        RaftKvClient client = new RaftKvClient("c", follower, sm, follower::tick);

        assertThrows(IllegalStateException.class, () -> client.get("k"));
    }
}
