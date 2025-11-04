package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;

class RaftReplicationTest {

    private static final class Recorder implements StateMachine {
        final List<String> applied = new ArrayList<>();
        @Override
        public byte[] apply(byte[] command) {
            applied.add(new String(command));
            return command;
        }
    }

    private static IntSupplier fixed(int t) {
        return () -> t;
    }

    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final Map<String, Recorder> recorders = new HashMap<>();

    private void buildCluster(List<String> ids, Map<String, Integer> timeouts) {
        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            Recorder rec = new Recorder();
            recorders.put(id, rec);
            nodes.put(id, new RaftNode(id, peers, rec, fixed(timeouts.get(id))));
        }
        for (String id : ids) {
            for (String other : ids) {
                if (!other.equals(id)) {
                    nodes.get(id).registerPeer(new InProcessRaftPeer(nodes.get(other)));
                }
            }
        }
    }

    private RaftNode electLeader(String leaderId, List<String> ids) {
        Map<String, Integer> timeouts = new HashMap<>();
        for (String id : ids) {
            timeouts.put(id, id.equals(leaderId) ? 2 : 50);
        }
        buildCluster(ids, timeouts);
        for (int i = 0; i < 2; i++) {
            nodes.get(leaderId).tick();
        }
        assertTrue(nodes.get(leaderId).isLeader());
        return nodes.get(leaderId);
    }

    @Test
    void proposeReplicatesAndCommitsToMajority() {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        RaftNode leader = electLeader("n0", ids);

        long index = leader.propose("set x=1".getBytes());
        assertEquals(1, index);
        assertEquals(1, leader.commitIndex());
        assertEquals(Arrays.asList("set x=1"), recorders.get("n0").applied);

        // a heartbeat propagates the commit index to followers, who then apply
        leader.tick();
        assertEquals(Arrays.asList("set x=1"), recorders.get("n1").applied);
        assertEquals(Arrays.asList("set x=1"), recorders.get("n2").applied);
    }

    @Test
    void proposeOnNonLeaderIsRejected() {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        electLeader("n0", ids);
        RaftNode follower = nodes.get("n1");
        assertEquals(0, follower.propose("nope".getBytes()));
        assertEquals(0, follower.log().lastIndex());
    }

    @Test
    void leaderRepairsDivergentFollowerLog() {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        RaftNode leader = electLeader("n0", ids);
        // n1 has a stale, divergent entry from an old term at index 1
        RaftNode follower = nodes.get("n1");
        follower.handleAppendEntries(AppendEntriesRequest.of(leader.currentTerm(), "n0", 0, 0,
                Collections.singletonList(LogEntry.of(99, 1, "garbage".getBytes())), 0));
        assertEquals(99, follower.log().termAt(1));

        // leader proposes real entries and drives heartbeats until convergence
        leader.propose("real1".getBytes());
        leader.propose("real2".getBytes());
        for (int i = 0; i < 5; i++) {
            leader.tick();
        }
        assertEquals(leader.log().lastIndex(), follower.log().lastIndex());
        assertEquals("real1", new String(follower.log().entryAt(1).command()));
        assertEquals("real2", new String(follower.log().entryAt(2).command()));
    }
}
