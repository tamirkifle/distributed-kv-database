package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InstallSnapshotInProcessTest {

    private static final class CountingSm implements StateMachine {
        final List<String> applied = new ArrayList<>();
        @Override public byte[] apply(byte[] command) {
            applied.add(new String(command, UTF_8));
            return command;
        }
        @Override public byte[] snapshot() { return String.join(",", applied).getBytes(UTF_8); }
        @Override public void restore(byte[] data, long idx, long term) {
            applied.clear();
            String s = new String(data, UTF_8);
            if (!s.isEmpty()) { Collections.addAll(applied, s.split(",")); }
        }
    }

    @Test
    void laggingFollowerCaughtUpViaInstallSnapshot() {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        Map<String, RaftNode> nodes = new HashMap<>();
        Map<String, CountingSm> sms = new HashMap<>();
        Map<String, Integer> timeouts = new HashMap<>();
        timeouts.put("n0", 2); timeouts.put("n1", 50); timeouts.put("n2", 50);

        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            CountingSm sm = new CountingSm();
            sms.put(id, sm);
            RaftNode node = new RaftNode(id, peers, sm, () -> timeouts.get(id));
            node.setCompactionThreshold(2);
            nodes.put(id, node);
        }
        // Wire peers, but n2 starts PARTITIONED so it falls behind.
        boolean[] n2reachable = {false};
        for (String id : ids) {
            for (String other : ids) {
                if (other.equals(id)) {
                    continue;
                }
                if (other.equals("n2")) {
                    nodes.get(id).registerPeer(new InProcessRaftPeer(nodes.get("n2"), () -> n2reachable[0]));
                } else {
                    nodes.get(id).registerPeer(new InProcessRaftPeer(nodes.get(other)));
                }
            }
        }

        RaftNode leader = nodes.get("n0");
        leader.tick(); leader.tick();
        assertTrue(leader.isLeader());

        // Commit 5 entries on the {n0,n1} majority while n2 is partitioned.
        for (int i = 1; i <= 5; i++) {
            leader.propose(("c" + i).getBytes(UTF_8));
        }
        for (int i = 0; i < 4; i++) {
            leader.tick(); // replicate+commit to n1
        }
        assertEquals(5, leader.lastApplied());

        // Leader compacts past index 2 -> n2's nextIndex (1) now precedes the base.
        leader.maybeCompact();
        assertTrue(leader.lastIncludedIndex() >= 2);

        // Heal n2 and let the leader replicate: it must send InstallSnapshot (AppendEntries
        // from index 1 is impossible — those entries are gone).
        n2reachable[0] = true;
        for (int i = 0; i < 6; i++) {
            leader.tick();
        }

        assertEquals(leader.lastApplied(), nodes.get("n2").lastApplied(),
                "follower caught up to leader's applied index");
        assertEquals(sms.get("n0").applied, sms.get("n2").applied,
                "follower converged to the leader's applied state");
    }

    @Test
    void staleTermInstallSnapshotIsRejected() {
        CountingSm sm = new CountingSm();
        RaftNode node = new RaftNode("n0", Arrays.asList("n1"), sm, () -> 50);
        // bump the node's term by handling a higher-term vote
        node.handleRequestVote(RequestVoteRequest.of(5, "n1", 0, 0));
        InstallSnapshotResponse resp = node.handleInstallSnapshot(
                InstallSnapshotRequest.of(3, "n1", 10, 2, "x".getBytes(UTF_8)));
        assertEquals(5, resp.term());
        assertEquals(0, node.lastIncludedIndex(), "stale snapshot not installed");
    }
}
