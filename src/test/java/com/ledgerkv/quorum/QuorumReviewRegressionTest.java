package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.node.QuorumClientCoordinator;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.transport.ConflictingValuesException;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import com.ledgerkv.transport.StoredValue;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regressions for the leaderless-quorum bugs found reviewing commit {@code 4264fd2}, ported
 * from the reviewer's own counterexamples.
 *
 * <p>Three of them share one root cause: a replica that can hold only a single value, and so
 * has to discard one of two concurrent writes. Two more are bugs in the coordinator-side clock
 * map, which is now deleted. The rest are the unreplicated delete, completion at W, and
 * primary-only quorum counts.
 */
class QuorumReviewRegressionTest {

    @TempDir
    Path dir;

    /** A replica decorator that can be made unavailable for reads or writes independently. */
    static final class ControlledReplica implements ReplicaClient {

        final ReplicaClient delegate;
        volatile boolean readAvailable = true;
        volatile boolean writeAvailable = true;
        volatile CountDownLatch holdPut;
        volatile CountDownLatch wrote;

        ControlledReplica(ReplicaClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public String nodeId() {
            return delegate.nodeId();
        }

        @Override
        public List<VersionedValue> get(String key) {
            if (!readAvailable) {
                throw new IllegalStateException("read unavailable");
            }
            return delegate.get(key);
        }

        @Override
        public void put(String key, VersionedValue value) {
            if (!writeAvailable) {
                throw new IllegalStateException("write unavailable");
            }
            await(holdPut);
            delegate.put(key, value);
            if (wrote != null) {
                wrote.countDown();
            }
        }

        @Override
        public void deliverHint(String key, VersionedValue value) {
            if (!writeAvailable) {
                throw new IllegalStateException("hint unavailable");
            }
            delegate.deliverHint(key, value);
        }

        static void await(CountDownLatch latch) {
            if (latch == null) {
                return;
            }
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test gate timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** A cluster of real LSM- or gRPC-backed replicas behind controllable decorators. */
    static final class Fixture implements AutoCloseable {

        final ClusterMembership membership;
        final List<LsmEngine> engines = new ArrayList<>();
        final List<NodeServer> servers = new ArrayList<>();
        final List<NodeClient> wireClients = new ArrayList<>();
        final List<ControlledReplica> nodes = new ArrayList<>();
        final Map<String, ReplicaClient> replicas = new LinkedHashMap<>();
        final ExecutorService fanout = Executors.newCachedThreadPool();
        final List<LeaderlessKVCluster> clusters = new ArrayList<>();

        Fixture(Path base, int count, int replication, boolean grpc) throws Exception {
            membership = ClusterMembership.create("review", count, replication);
            for (int i = 0; i < count; i++) {
                String id = membership.getNodes().get(i).getId();
                ReplicaClient delegate;
                if (grpc) {
                    NodeServer server =
                        NodeServer.builder(0).dataDir(base.resolve("node-" + i)).build().start();
                    servers.add(server);
                    NodeClient client = NodeClient.connect("localhost", server.port());
                    wireClients.add(client);
                    delegate = new GrpcReplicaClient(id, client);
                } else {
                    LsmEngine engine = LsmEngine.open(base.resolve("node-" + i));
                    engines.add(engine);
                    delegate = new LocalReplicaClient(id, engine);
                }
                ControlledReplica node = new ControlledReplica(delegate);
                nodes.add(node);
                replicas.put(id, node);
            }
        }

        LeaderlessKVCluster quorum(int w, int r) {
            LeaderlessKVCluster cluster = LeaderlessKVCluster.create(membership,
                new QuorumConfig(membership.getReplicationFactor(), w, r), replicas, fanout,
                Duration.ofSeconds(5), Duration.ZERO);
            clusters.add(cluster);
            return cluster;
        }

        /** Two causally concurrent values, written by different authors, split across replicas. */
        void seedConflicts() {
            VersionedValue a = new VersionedValue("branch-a", 1, VersionMetadata.initial("writer-a"));
            VersionedValue b = new VersionedValue("branch-b", 1, VersionMetadata.initial("writer-b"));
            nodes.get(0).put("k", a);
            nodes.get(1).put("k", b);
            nodes.get(2).put("k", a);
        }

        @Override
        public void close() throws Exception {
            // Drain replication released early at W before tearing anything down. shutdownNow()
            // would interrupt a task inside a channel write, which closes that replica's WAL.
            for (LeaderlessKVCluster cluster : clusters) {
                cluster.awaitReplication(Duration.ofSeconds(5));
            }
            fanout.shutdown();
            fanout.awaitTermination(5, TimeUnit.SECONDS);
            for (NodeClient client : wireClients) {
                client.close();
            }
            for (NodeServer server : servers) {
                server.close();
            }
            for (LsmEngine engine : engines) {
                engine.close();
            }
        }
    }

    /**
     * Replica storage accepted every arrival unconditionally, so replaying a hint
     * carrying v1 against a replica that had since accepted v2 rolled it back to v1.
     */
    @Test
    void oldHintMustNotOverwriteANewerAcknowledgedValue() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            f.nodes.get(2).writeAvailable = false;
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            // The write returns at W; the hint is filed by the background accounting task.
            assertTrue(q.awaitReplication(Duration.ofSeconds(5)));
            assertEquals(1, q.getPendingHints().size());

            f.nodes.get(2).writeAvailable = true;
            assertTrue(q.write(0, "k", "v2").isSuccessful());
            assertTrue(q.awaitReplication(Duration.ofSeconds(5)));
            assertEquals("v2", single(f.nodes.get(2).get("k")).getValue());

            assertEquals(1, q.replayPendingHints().getAppliedCount());

            assertEquals("v2", single(f.nodes.get(2).get("k")).getValue(),
                "a delayed v1 hint overwrote the causally newer v2 on a real LSM replica");
        }
    }

    /** The same guard on the gRPC transport, which had an identical unconditional write path. */
    @Test
    void oldHintMustNotOverwriteANewerValueOverGrpc() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, true)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            f.nodes.get(2).writeAvailable = false;
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            f.nodes.get(2).writeAvailable = true;
            assertTrue(q.write(0, "k", "v2").isSuccessful());
            assertTrue(q.awaitReplication(Duration.ofSeconds(5)));

            assertEquals(1, q.replayPendingHints().getAppliedCount());

            assertEquals("v2", single(f.nodes.get(2).get("k")).getValue());
        }
    }

    /** A replica keeps two genuinely concurrent writes rather than letting the later one win. */
    @Test
    void replicaKeepsConcurrentSiblingsRatherThanOverwriting() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            ControlledReplica replica = f.nodes.get(0);
            replica.put("k", new VersionedValue("branch-a", 1, VersionMetadata.initial("writer-a")));
            replica.put("k", new VersionedValue("branch-b", 1, VersionMetadata.initial("writer-b")));

            List<VersionedValue> held = replica.get("k");

            assertEquals(2, held.size(), "concurrent clocks must both survive at the replica");
        }
    }

    /** Redelivering the identical write is idempotent, not a second sibling. */
    @Test
    void redeliveringTheSameValueIsIdempotent() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            ControlledReplica replica = f.nodes.get(0);
            VersionedValue value =
                new VersionedValue("v", 1, VersionMetadata.initial("writer-a"));
            replica.put("k", value);
            replica.put("k", value);

            assertEquals(1, replica.get("k").size());
        }
    }

    /**
     * Repair chose a winner by scalar version — which cannot order two concurrent
     * vector clocks, since their counter sums are equal — and copied it over the other branch. The
     * conflict did not get resolved, it got deleted.
     */
    @Test
    void repairMustNotDiscardUnresolvedConcurrentSiblings() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            f.seedConflicts();
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertEquals(2, q.read(0, "k").getConflictingValues().size());

            QuorumResponse repaired = q.repair(0, "k");

            assertTrue(repaired.hasConflicts(), "repair must report the unresolved conflict");
            assertEquals(2, q.read(0, "k").getConflictingValues().size(),
                "repair replaced concurrent siblings with a scalar-version winner");
        }
    }

    /** Repair still does its job when the versions are causally ordered: the newest wins. */
    @Test
    void repairStillPropagatesACausallyDominantValue() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            f.nodes.get(2).writeAvailable = false;
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            f.nodes.get(2).writeAvailable = true;

            QuorumResponse repaired = q.repair(0, "k");

            assertTrue(repaired.isSuccessful());
            assertEquals("v1", repaired.getValue().getValue());
            assertEquals("v1", single(f.nodes.get(2).get("k")).getValue(),
                "the lagging replica must be brought up to date");
        }
    }

    /**
     * {@code QuorumResponse} returns a null winner when it holds unresolved
     * siblings, and the public coordinator read that null as absence — so a caller could not tell a
     * conflicted key from a missing one.
     */
    @Test
    void publicGrpcReadMustNotReportConflictsAsMissing() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, true)) {
            f.seedConflicts();
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertTrue(q.read(0, "k").hasConflicts());
            f.servers.get(0).useCoordinator(new QuorumClientCoordinator(q, 0));
            NodeClient client = f.wireClients.get(0);

            List<StoredValue> siblings = client.getSiblings("k");

            assertEquals(2, siblings.size(),
                "the public RPC must carry both concurrent values");
            ConflictingValuesException conflict =
                assertThrows(ConflictingValuesException.class, () -> client.get("k"));
            assertEquals(2, conflict.siblings().size());
        }
    }

    /** A genuinely absent key is still absent — the conflict signal must not swallow that case. */
    @Test
    void publicGrpcReadStillReportsAMissingKeyAsAbsent() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, true)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            f.servers.get(0).useCoordinator(new QuorumClientCoordinator(q, 0));

            Optional<byte[]> value = f.wireClients.get(0).get("never-written");

            assertTrue(value.isEmpty());
            assertTrue(f.wireClients.get(0).getSiblings("never-written").isEmpty());
        }
    }

    /** The review's passing control: the core read path preserves conflicts as documented. */
    @Test
    void coreReadPreservesConflictsAsDocumented() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            f.seedConflicts();

            QuorumResponse read = f.quorum(2, 3).read(0, "k");

            assertTrue(read.isSuccessful());
            assertTrue(read.hasConflicts());
            assertNull(read.getValue());
            assertEquals(2, read.getConflictingValues().size());
        }
    }

    /** The review's other passing control: a strict replica set survives one unavailable node. */
    @Test
    void strictReplicaQuorumSurvivesOneUnavailableNode() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 2);
            f.nodes.get(2).writeAvailable = false;
            f.nodes.get(2).readAvailable = false;

            assertTrue(q.write(0, "k", "v1").isSuccessful());
            QuorumResponse read = q.read(0, "k");

            assertTrue(read.isSuccessful());
            assertEquals("v1", read.getValue().getValue());
        }
    }

    /**
     * The coordinator's per-key clock map started empty on construction, so a
     * restarted coordinator reissued counter 1 while a surviving replica still held counter 2 — and
     * the old value won the comparison against a write that had just been acknowledged.
     */
    @Test
    void restartingCoordinatorMustNotReuseAnOlderClock() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster before = f.quorum(2, 3);
            assertTrue(before.write(0, "k", "v1").isSuccessful());
            assertTrue(before.write(0, "k", "v2").isSuccessful());
            before.close(); // the replica storage survives this coordinator restart

            LeaderlessKVCluster restarted = f.quorum(2, 3);
            f.nodes.get(2).writeAvailable = false;
            QuorumResponse write = restarted.write(0, "k", "v3");
            assertTrue(write.isSuccessful());
            assertEquals(2, write.getRespondingNodes());

            f.nodes.get(2).writeAvailable = true;
            QuorumResponse read = restarted.read(0, "k");

            assertTrue(read.isSuccessful());
            assertEquals("v3", read.getValue().getValue(),
                "the new acknowledged write reused clock 1 and lost to an old clock-2 replica");
        }
    }

    /** Per-key counters still read 1, 2, 3 — deriving from stored state did not inflate them. */
    @Test
    void versionsRemainDenseAcrossACoordinatorRestart() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster before = f.quorum(2, 3);
            assertEquals(1, before.write(0, "k", "v1").getValue().getVersion());
            assertEquals(2, before.write(0, "k", "v2").getValue().getVersion());
            before.close();

            LeaderlessKVCluster restarted = f.quorum(2, 3);

            assertEquals(3, restarted.write(0, "k", "v3").getValue().getVersion());
        }
    }

    /**
     * Repair replaced the coordinator's clock entry with metadata it had read
     * earlier, so a write that committed while repair was in flight had its counter reissued.
     * There is no longer a map for repair to overwrite; the guard here is that the clock only ever
     * advances.
     */
    @Test
    void repairMustNotRewindAConcurrentWritersClock() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertEquals(1, q.write(0, "k", "v1").getValue().getVersion());
            assertEquals(2, q.write(0, "k", "v2").getValue().getVersion());

            // Repair reads the current state and writes it back; it must not affect allocation.
            assertTrue(q.repair(0, "k").isSuccessful());

            assertEquals(3, q.write(0, "k", "v3").getValue().getVersion(),
                "repair installed old read metadata over the clock of a later write");
        }
    }

    /**
     * A write cannot allocate a version it has not established is unused, so it refuses when it
     * cannot reach a read quorum rather than reissuing a counter that a replica already holds.
     */
    @Test
    void writeRefusesWhenTheCausalContextCannotBeRead() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(1, 2);
            f.nodes.get(1).readAvailable = false;
            f.nodes.get(2).readAvailable = false;

            QuorumResponse write = q.write(0, "k", "v1");

            assertTrue(!write.isSuccessful(),
                "without a read quorum the coordinator cannot know which counters are in use");
            assertEquals(1, write.getRespondingNodes());
        }
    }

    /**
     * With a quorum coordinator configured, Put and Get fanned out but Delete
     * bypassed the coordinator and removed the value from one node's local engine — returning
     * existed=true while a Get through the same client still read the value from another replica.
     */
    @Test
    void publicDeleteMustNotAcknowledgeOnlyALocalDeletion() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, true)) {
            LeaderlessKVCluster q = f.quorum(2, 2);
            f.servers.get(0).useCoordinator(new QuorumClientCoordinator(q, 0));
            NodeClient client = f.wireClients.get(0);
            client.put("k", "v1".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            assertTrue(client.delete("k"));

            assertTrue(client.get("k").isEmpty(),
                "Delete reported success but the same client still reads v1");
        }
    }

    /** The tombstone reaches the other replicas, not just the coordinating node. */
    @Test
    void deleteReplicatesATombstoneToEveryReplica() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(3, 3);
            assertTrue(q.write(0, "k", "v1").isSuccessful());

            assertTrue(q.delete(0, "k").isSuccessful());

            for (ControlledReplica replica : f.nodes) {
                VersionedValue held = single(replica.get("k"));
                assertTrue(held.isDeleted(), replica.nodeId() + " must hold the tombstone");
            }
        }
    }

    /**
     * The reason a delete has to carry a version: read repair must not resurrect the value from a
     * replica that missed the delete.
     */
    @Test
    void readRepairMustNotResurrectADeletedKey() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            f.nodes.get(2).writeAvailable = false;
            assertTrue(q.delete(0, "k").isSuccessful());
            f.nodes.get(2).writeAvailable = true;
            // Replica 2 still holds the live v1 while the others hold the tombstone.
            assertTrue(!single(f.nodes.get(2).get("k")).isDeleted());

            assertTrue(q.repair(0, "k").isSuccessful());

            assertTrue(single(f.nodes.get(2).get("k")).isDeleted(),
                "repair copied the stale live value back over a newer delete");
            assertTrue(q.read(0, "k").getValue().isDeleted());
        }
    }

    /** Writing a key again after deleting it brings it back, with a clock above the tombstone. */
    @Test
    void aKeyCanBeRewrittenAfterDeletion() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            assertTrue(q.delete(0, "k").isSuccessful());

            assertTrue(q.write(0, "k", "v2").isSuccessful());

            QuorumResponse read = q.read(0, "k");
            assertTrue(read.isSuccessful());
            assertTrue(!read.getValue().isDeleted());
            assertEquals("v2", read.getValue().getValue());
        }
    }

    /** A delayed hint carrying a tombstone replays as a delete, not as an empty live value. */
    @Test
    void aHintedTombstoneReplaysAsADelete() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 3);
            assertTrue(q.write(0, "k", "v1").isSuccessful());
            f.nodes.get(2).writeAvailable = false;
            assertTrue(q.delete(0, "k").isSuccessful());
            assertEquals(1, q.getPendingHints().size());

            f.nodes.get(2).writeAvailable = true;
            assertEquals(1, q.replayPendingHints().getAppliedCount());

            assertTrue(single(f.nodes.get(2).get("k")).isDeleted());
        }
    }

    /**
     * The write loop kept collecting every submitted response, and reaching W only
     * ended it once the request deadline had <em>also</em> elapsed. With N=3, W=2 and the third
     * replica blocked, two replicas had durably stored the value while the client stayed blocked.
     */
    @Test
    void quorumWriteMustNotWaitForABlockedMinorityAfterWAcks() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            LeaderlessKVCluster q = f.quorum(2, 2);
            CountDownLatch twoStored = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            List<ClusterNode> prefs = f.membership.getPreferenceList("k", 3);
            replica(f, prefs.get(0)).wrote = twoStored;
            replica(f, prefs.get(1)).wrote = twoStored;
            replica(f, prefs.get(2)).holdPut = release;

            ExecutorService driver = Executors.newSingleThreadExecutor();
            java.util.concurrent.Future<QuorumResponse> write =
                driver.submit(() -> q.write(0, "k", "v1"));
            try {
                assertTrue(twoStored.await(5, TimeUnit.SECONDS),
                    "two durable writes must complete first");
                QuorumResponse response = write.get(2, TimeUnit.SECONDS);

                assertTrue(response.isSuccessful());
                assertEquals(2, response.getRespondingNodes());
                assertEquals(1, replica(f, prefs.get(2)).holdPut.getCount(),
                    "the third replica is still blocked inside its put");
            } finally {
                release.countDown();
                write.get(5, TimeUnit.SECONDS);
                driver.shutdownNow();
            }
        }
    }

    /**
     * A backup replica outside the primary N could count toward W, so a write set
     * and a read set could be disjoint while both hit their thresholds — and the config still
     * claimed strong consistency. PW/PR count only primaries, so the overlap can be demanded.
     */
    @Test
    void overlapFlagMustNotPromiseFreshReadsWithHedgedReplicaSets() {
        QuorumConfig sloppy = new QuorumConfig(3, 2, 2);

        assertTrue(sloppy.hasQuorumOverlap(), "the numbers do satisfy W+R>N");
        assertTrue(!sloppy.guaranteesReadYourWrites(),
            "with no primary requirement a fallback ack can make the sets disjoint");

        QuorumConfig strict = new QuorumConfig(3, 2, 2, 2, 2);
        assertTrue(strict.guaranteesReadYourWrites(), "PW+PR>N is the condition that holds");
    }

    /** A write that cannot reach PW primaries fails, even when a hedge supplies the Wth ack. */
    @Test
    void writeFailsWhenPrimaryAcksFallBelowPw() throws Exception {
        try (Fixture f = new Fixture(dir, 4, 3, false)) {
            List<ClusterNode> prefs = f.membership.getPreferenceList("k", 4);
            replica(f, prefs.get(1)).writeAvailable = false;
            replica(f, prefs.get(2)).writeAvailable = false;
            LeaderlessKVCluster q = LeaderlessKVCluster.create(f.membership,
                new QuorumConfig(3, 2, 2, 2, 2), f.replicas, f.fanout,
                Duration.ofMillis(300), Duration.ZERO);
            f.clusters.add(q);

            QuorumResponse write = q.write(0, "k", "v1");

            assertTrue(!write.isSuccessful(),
                "only one primary could ack, so PW=2 is not satisfied even if a hedge answers");
        }
    }

    /** With every primary reachable, the same strict configuration succeeds. */
    @Test
    void writeSucceedsWhenPrimaryAcksMeetPw() throws Exception {
        try (Fixture f = new Fixture(dir, 4, 3, false)) {
            LeaderlessKVCluster q = LeaderlessKVCluster.create(f.membership,
                new QuorumConfig(3, 2, 2, 2, 2), f.replicas, f.fanout,
                Duration.ofSeconds(5), Duration.ZERO);
            f.clusters.add(q);

            QuorumResponse write = q.write(0, "k", "v1");

            assertTrue(write.isSuccessful());
            QuorumResponse read = q.read(0, "k");
            assertTrue(read.isSuccessful());
            assertEquals("v1", read.getValue().getValue());
        }
    }

    /** A read counts responding replicas, not values: siblings must not inflate the quorum. */
    @Test
    void readQuorumCountsRespondersRatherThanSiblings() throws Exception {
        try (Fixture f = new Fixture(dir, 3, 3, false)) {
            // One replica holds two concurrent siblings; the other two are unreachable.
            List<ClusterNode> prefs = f.membership.getPreferenceList("k", 3);
            replica(f, prefs.get(0)).put("k",
                new VersionedValue("branch-a", 1, VersionMetadata.initial("writer-a")));
            replica(f, prefs.get(0)).put("k",
                new VersionedValue("branch-b", 1, VersionMetadata.initial("writer-b")));
            replica(f, prefs.get(1)).readAvailable = false;
            replica(f, prefs.get(2)).readAvailable = false;
            LeaderlessKVCluster q = f.quorum(2, 2);

            QuorumResponse read = q.read(0, "k");

            assertTrue(!read.isSuccessful(),
                "two siblings from one replica are one response, not a read quorum of two");
            assertEquals(1, read.getRespondingNodes());
        }
    }

    private static ControlledReplica replica(Fixture f, ClusterNode node) {
        return (ControlledReplica) f.replicas.get(node.getId());
    }

    private static VersionedValue single(List<VersionedValue> values) {
        assertEquals(1, values.size(), "expected exactly one value, got " + values);
        return values.get(0);
    }
}
