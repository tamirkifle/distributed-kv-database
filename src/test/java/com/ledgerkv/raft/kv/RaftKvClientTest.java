package com.ledgerkv.raft.kv;

import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.raft.InProcessRaftPeer;
import com.ledgerkv.raft.RaftNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RaftKvClientTest {

    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final Map<String, RaftKvStateMachine> sms = new HashMap<>();

    private void buildCluster(List<String> ids, String leaderId) {
        Map<String, Integer> timeouts = new HashMap<>();
        for (String id : ids) {
            timeouts.put(id, id.equals(leaderId) ? 2 : 50);
        }
        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            RaftKvStateMachine sm = new RaftKvStateMachine();
            sms.put(id, sm);
            nodes.put(id, new RaftNode(id, peers, sm, () -> timeouts.get(id)));
        }
        for (String id : ids) {
            for (String other : ids) {
                if (!other.equals(id)) {
                    nodes.get(id).registerPeer(new InProcessRaftPeer(nodes.get(other)));
                }
            }
        }
        for (int i = 0; i < 2; i++) {
            nodes.get(leaderId).tick();
        }
        assertTrue(nodes.get(leaderId).isLeader());
    }

    private RaftKvClient clientFor(String clientId, String leaderId) {
        RaftNode leader = nodes.get(leaderId);
        Runnable drive = () -> {
            for (int i = 0; i < 3; i++) {
                leader.tick();
            }
        };
        return new RaftKvClient(clientId, leader, sms.get(leaderId), drive);
    }

    @Test
    void putThenGetThroughRaft() {
        buildCluster(Arrays.asList("n0", "n1", "n2"), "n0");
        RaftKvClient c = clientFor("c1", "n0");
        c.put("k", "v".getBytes());
        assertArrayEquals("v".getBytes(), c.get("k"));
    }

    @Test
    void deleteRemovesKeyAndReturnsPrevious() {
        buildCluster(Arrays.asList("n0", "n1", "n2"), "n0");
        RaftKvClient c = clientFor("c1", "n0");
        c.put("k", "v".getBytes());
        assertArrayEquals("v".getBytes(), c.delete("k"));
        assertNull(c.get("k"));
    }

    @Test
    void successiveWritesUseIncreasingSequences() {
        buildCluster(Arrays.asList("n0", "n1", "n2"), "n0");
        RaftKvClient c = clientFor("c1", "n0");
        c.put("a", "1".getBytes());
        c.put("b", "2".getBytes());
        assertEquals(2, sms.get("n0").lastAppliedSequence("c1"));
    }

    @Test
    void retriedWriteDoesNotDoubleApply() {
        buildCluster(Arrays.asList("n0", "n1", "n2"), "n0");
        RaftKvClient c = clientFor("c1", "n0");
        c.putWithSequence(1, "k", "v1".getBytes());
        RaftKvClient other = clientFor("c2", "n0");
        other.putWithSequence(1, "k", "v2".getBytes());
        c.putWithSequence(1, "k", "v1".getBytes()); // retry of c1 seq 1
        assertArrayEquals("v2".getBytes(), c.get("k"));
    }

    @Test
    void proposeOnNonLeaderThrows() {
        buildCluster(Arrays.asList("n0", "n1", "n2"), "n0");
        RaftNode follower = nodes.get("n1");
        RaftKvClient bad = new RaftKvClient("c3", follower, sms.get("n1"), () -> {});
        assertThrows(IllegalStateException.class, () -> bad.put("k", "v".getBytes()));
    }
}
