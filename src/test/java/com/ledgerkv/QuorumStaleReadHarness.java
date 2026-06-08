package com.ledgerkv;

import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.ClusterNode;
import com.ledgerkv.quorum.InMemoryReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;
import com.ledgerkv.failure.FailureContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Test-only harness: a leaderless quorum cluster (N=3, <b>W=1, R=1</b> so W+R &le; N) driven through
 * a <b>seeded, scripted</b> partition schedule that deterministically produces a stale read — a read
 * that returns an earlier value A even though a newer write B completed and was acknowledged before
 * it. With overlapping logical real-time bounds the read is concurrent with B, so no valid
 * linearization exists: this is the honest demonstration that an AP quorum with W+R&le;N is not
 * linearizable. No threads, no wall clock, no chance: the partition routing forces the stale read.
 */
final class QuorumStaleReadHarness {

    private static final Instant EPOCH = Instant.parse("2026-06-29T00:00:00Z");
    private static final QuorumConfig WEAK = new QuorumConfig(3, 1, 1); // W+R = 2 <= N = 3
    private static final String KEY = "x";

    private QuorumStaleReadHarness() {
    }

    static OperationHistory recordHistory() {
        ClusterMembership membership = ClusterMembership.create("harness-quorum", 3, 3);
        Set<String> unavailable = new LinkedHashSet<>();
        Map<String, ReplicaClient> clients = new LinkedHashMap<>();
        for (ClusterNode node : membership.getNodes()) {
            clients.put(node.getId(), new ControlledFailureReplicaClient(node.getId(), unavailable));
        }
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(membership, WEAK, clients);

        List<ClusterNode> pref = membership.getPreferenceList(KEY, membership.getReplicationFactor());
        String p0 = pref.get(0).getId();
        String p1 = pref.get(1).getId();
        String p2 = pref.get(2).getId();

        List<OperationRecord> ops = new ArrayList<>();

        // 1) write x=A, all reachable -> A on p0,p1,p2. Ticks [0,2].
        QuorumResponse wA = cluster.write(0, KEY, "A");
        ops.add(record(OperationType.WRITE, "A", 0, 2, wA.isSuccessful()));

        // 2) partition p0, write x=B -> B on p1,p2 (W=1 satisfied). Ticks [10,12].
        unavailable.clear();
        unavailable.add(p0);
        QuorumResponse wB = cluster.write(0, KEY, "B");
        ops.add(record(OperationType.WRITE, "B", 10, 12, wB.isSuccessful()));

        // 3) heal p0, partition p1 and p2, read x -> reaches p0 first (R=1) -> stale A.
        //    The read [20,22] starts strictly AFTER the B write completes (B.end=12 < 20), so the
        //    read happens-after B in real time. A linearization must place B before the read, yet
        //    the read returns A (the value B overwrote) -> no valid linearization exists.
        unavailable.clear();
        unavailable.add(p1);
        unavailable.add(p2);
        QuorumResponse r = cluster.read(0, KEY);
        String observed = r.isSuccessful() && r.getValue() != null ? r.getValue().getValue() : null;
        ops.add(record(OperationType.READ, observed, 20, 22,
            r.isSuccessful() ? OperationResult.SUCCESS : OperationResult.FAILURE));

        return new OperationHistory(ops);
    }

    private static OperationRecord record(OperationType type, String value, long startTick,
            long endTick, boolean successful) {
        return record(type, value, startTick, endTick,
            successful ? OperationResult.SUCCESS : OperationResult.FAILURE);
    }

    private static OperationRecord record(OperationType type, String value, long startTick,
            long endTick, OperationResult result) {
        return new OperationRecord(type, KEY, value,
            EPOCH.plusMillis(startTick), EPOCH.plusMillis(endTick),
            result, 0, WEAK, FailureContext.empty());
    }

    /** A {@link ReplicaClient} that refuses every call while its node id is in the unavailable set. */
    private static final class ControlledFailureReplicaClient implements ReplicaClient {
        private final String nodeId;
        private final Set<String> unavailable;
        private final InMemoryReplicaClient delegate;

        ControlledFailureReplicaClient(String nodeId, Set<String> unavailable) {
            this.nodeId = nodeId;
            this.unavailable = unavailable;
            this.delegate = new InMemoryReplicaClient(nodeId);
        }

        @Override
        public String nodeId() {
            return nodeId;
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            requireAvailable();
            return delegate.get(key);
        }

        @Override
        public void put(String key, VersionedValue value) {
            requireAvailable();
            delegate.put(key, value);
        }

        @Override
        public void deliverHint(String key, VersionedValue value) {
            requireAvailable();
            delegate.deliverHint(key, value);
        }

        private void requireAvailable() {
            if (unavailable.contains(nodeId)) {
                throw new IllegalStateException("replica unavailable: " + nodeId);
            }
        }
    }
}
