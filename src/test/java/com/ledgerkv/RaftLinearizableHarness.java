package com.ledgerkv;

import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.raft.InProcessRaftPeer;
import com.ledgerkv.raft.RaftNode;
import com.ledgerkv.raft.kv.RaftKvClient;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import com.ledgerkv.failure.FailureContext;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-only harness: a deterministic in-process 3-node Raft group (manual clock, {@link
 * InProcessRaftPeer} with a partition seam) driving a scripted put/get/delete sequence through
 * {@link RaftKvClient}, recording each client-visible op as an {@link OperationRecord} with logical
 * real-time bounds. Because Raft commits on a majority and tolerates one partitioned follower, the
 * resulting {@link OperationHistory} is linearizable by construction — that is the point of the 3e
 * contrast. No threads, no wall clock: fully reproducible.
 */
final class RaftLinearizableHarness {

    private static final Instant EPOCH = Instant.parse("2026-06-29T00:00:00Z");
    // The Raft path's strong config, recorded for context (W+R>N). Not used by the checker.
    private static final QuorumConfig STRONG = new QuorumConfig(3, 2, 2);

    private RaftLinearizableHarness() {
    }

    static OperationHistory recordHistory() {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        String leaderId = "n0";

        Map<String, RaftNode> nodes = new HashMap<>();
        Map<String, RaftKvStateMachine> sms = new HashMap<>();
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
        // Partition seam: when partitioned[0] is true, n2 is unreachable from everyone.
        boolean[] partitioned = {false};
        for (String id : ids) {
            for (String other : ids) {
                if (!other.equals(id)) {
                    boolean targetsN2 = other.equals("n2");
                    nodes.get(id).registerPeer(new InProcessRaftPeer(
                        nodes.get(other),
                        () -> !(targetsN2 && partitioned[0])));
                }
            }
        }
        for (int i = 0; i < 2; i++) {
            nodes.get(leaderId).tick();
        }
        RaftNode leader = nodes.get(leaderId);
        Runnable drive = () -> {
            for (int i = 0; i < 5; i++) {
                leader.tick();
            }
        };
        RaftKvClient client = new RaftKvClient("c1", leader, sms.get(leaderId), drive);

        List<OperationRecord> ops = new ArrayList<>();
        long tick = 0;

        // 1) write x=A (before partition)
        tick = recordWrite(ops, "x", "A", tick, () -> client.put("x", bytes("A")));
        // 2) read x -> A
        tick = recordRead(ops, "x", str(client.get("x")), tick);

        // Partition n2 away. Majority (n0,n1) still commits — linearizable holds.
        partitioned[0] = true;

        // 3) write x=B under partition (still commits on majority)
        tick = recordWrite(ops, "x", "B", tick, () -> client.put("x", bytes("B")));
        // 4) read x -> B (reflects the committed write — linearizable)
        tick = recordRead(ops, "x", str(client.get("x")), tick);
        // 5) write x=C under partition
        tick = recordWrite(ops, "x", "C", tick, () -> client.put("x", bytes("C")));

        // Heal the partition.
        partitioned[0] = false;
        drive.run();

        // 6) read x -> C after heal
        tick = recordRead(ops, "x", str(client.get("x")), tick);

        return new OperationHistory(ops);
    }

    private static long recordWrite(List<OperationRecord> ops, String key, String value,
            long startTick, Runnable action) {
        long endTick = startTick + 2;
        action.run();
        ops.add(record(OperationType.WRITE, key, value, startTick, endTick));
        return endTick + 1;
    }

    private static long recordRead(List<OperationRecord> ops, String key, String observed,
            long startTick) {
        long endTick = startTick + 2;
        ops.add(record(OperationType.READ, key, observed, startTick, endTick));
        return endTick + 1;
    }

    private static OperationRecord record(OperationType type, String key, String value,
            long startTick, long endTick) {
        return new OperationRecord(type, key, value,
            EPOCH.plusMillis(startTick), EPOCH.plusMillis(endTick),
            OperationResult.SUCCESS, 0, STRONG, FailureContext.empty());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }
}
