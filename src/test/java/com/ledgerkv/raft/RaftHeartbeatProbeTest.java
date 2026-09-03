package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The heartbeat a ReadIndex round confirms leadership with (Ongaro §6.4 step 3: "a new round of
 * heartbeats ... acknowledgments from a majority").
 *
 * <p>The distinction these tests protect is where the probe is anchored. Building it from
 * {@code nextIndex}, the way ordinary replication does, escalates to an InstallSnapshot for any
 * follower that has fallen behind the log base — so a single read would ship the whole state
 * machine. Anchoring on {@code matchIndex} cannot: that prefix is already acknowledged.
 */
class RaftHeartbeatProbeTest {

    private static final class Sm implements StateMachine {
        @Override public byte[] apply(byte[] command) {
            return command;
        }
        @Override public byte[] snapshot() {
            return "state".getBytes(UTF_8);
        }
    }

    /**
     * A leader of a 3-node group that has compacted every entry away while both followers stayed at
     * index 0. The peers answer the election, then go silent, which is what lets the leader's log
     * base outrun their match index.
     */
    private static RaftNode compactedLeader() {
        List<String> peerIds = Arrays.asList("n1", "n2");
        AtomicBoolean reachable = new AtomicBoolean(true);
        RaftNode node = new RaftNode("n0", peerIds, new Sm(), () -> 1);
        for (String peer : peerIds) {
            node.registerPeer(new InProcessRaftPeer(
                    new RaftNode(peer, Collections.singletonList("n0"), new Sm(), () -> 50),
                    reachable::get));
        }
        node.tick();
        assertTrue(node.isLeader());
        reachable.set(false);

        node.setCompactionThreshold(1);
        node.proposeLocal("c1".getBytes(UTF_8));
        node.proposeLocal("c2".getBytes(UTF_8));
        // n2 alone acknowledging is a majority in a group of three, so the entries commit, apply,
        // and compact away. n1 never answers, which leaves its match index at 0 — below the base.
        node.applyAppendEntriesResponse("n2",
                AppendEntriesResponse.success(node.currentTerm(), node.log().lastIndex()));
        node.maybeCompact();
        return node;
    }

    @Test
    void probeStaysAnEmptyAppendEntriesForAFollowerBehindTheLogBase() {
        RaftNode leader = compactedLeader();
        assertTrue(leader.lastIncludedIndex() > 0, "the log base advanced past the peers");

        // Ordinary replication has to ship the snapshot to catch this follower up.
        assertTrue(leader.nextReplicationRequest("n1").isSnapshot(),
                "replication escalates to InstallSnapshot below the base");

        AppendEntriesRequest probe = leader.heartbeatProbe("n1");
        assertTrue(probe.entries().isEmpty(), "a leadership probe carries no entries");
        assertEquals(leader.lastIncludedIndex(), probe.prevLogIndex(),
                "clamped to the log base rather than reaching below it");
        assertEquals(leader.currentTerm(), probe.term());
    }

    @Test
    void probeSitsAtWhatTheFollowerHasAlreadyAcknowledged() {
        List<String> peerIds = Arrays.asList("n1", "n2");
        RaftNode leader = new RaftNode("n0", peerIds, new Sm(), () -> 1);
        for (String peer : peerIds) {
            leader.registerPeer(new InProcessRaftPeer(
                    new RaftNode(peer, Collections.singletonList("n0"), new Sm(), () -> 50)));
        }
        leader.tick();
        assertTrue(leader.isLeader());
        leader.proposeLocal("c1".getBytes(UTF_8));
        leader.proposeLocal("c2".getBytes(UTF_8));

        leader.applyAppendEntriesResponse("n1", AppendEntriesResponse.success(leader.currentTerm(), 2));

        AppendEntriesRequest probe = leader.heartbeatProbe("n1");
        assertEquals(2, probe.prevLogIndex(), "anchored on matchIndex, not the leader's tail");
        assertTrue(probe.entries().isEmpty());
    }

    @Test
    void onlyALeaderProbes() {
        RaftNode follower = new RaftNode(
                "n0", Arrays.asList("n1", "n2"), new Sm(), () -> 50);
        assertFalse(follower.isLeader());
        assertNull(follower.heartbeatProbe("n1"));
        assertEquals(-1, follower.readIndexCandidate(), "a follower has no read index to offer");
    }
}
