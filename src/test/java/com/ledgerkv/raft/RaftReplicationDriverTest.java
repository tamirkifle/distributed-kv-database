package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.raft.RaftReviewRegressionTest.RecordingStateMachine;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Peer replication used to run synchronously inside {@code synchronized}
 * node methods, and {@code propose} did not return until that loop finished. A five-node group could
 * therefore reach {@code commitIndex=1} and {@code lastApplied=1} while the proposing caller was
 * still blocked on a slow minority peer.
 *
 * <p>The review's own reproduction asserted that the slow peer is invoked <em>on the proposing
 * thread</em> and that the proposal has already completed by then. Once replication is decoupled the
 * proposing thread never calls a peer at all, so the latch it waited on cannot fire from that
 * thread. These tests assert the property the review is actually about: the caller returns once its
 * entry is committed and applied, while a peer is still blocked, and the node stays responsive.
 */
class RaftReplicationDriverTest {

    /** A peer that can be held indefinitely on AppendEntries, and reports whether it is held. */
    private static final class BlockablePeer implements RaftPeer {

        private final RaftNode target;
        private final AtomicBoolean blocking = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        /** Calls currently parked inside {@link #appendEntries}. */
        private final java.util.concurrent.atomic.AtomicInteger inFlight =
                new java.util.concurrent.atomic.AtomicInteger();

        BlockablePeer(RaftNode target) {
            this.target = target;
        }

        @Override
        public String nodeId() {
            return target.nodeId();
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            return target.handleRequestVote(request);
        }

        @Override
        public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
            return target.handleInstallSnapshot(request);
        }

        @Override
        public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
            if (blocking.get()) {
                inFlight.incrementAndGet();
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    inFlight.decrementAndGet();
                }
            }
            return target.handleAppendEntries(request);
        }
    }

    private static RaftNode follower(String id, List<String> allIds) {
        List<String> peers = new ArrayList<>(allIds);
        peers.remove(id);
        return new RaftNode(id, peers, new RecordingStateMachine(), () -> 100_000);
    }

    @Test
    void proposalCompletesOnMajorityWhileAMinorityPeerIsStillBlocked() throws Exception {
        List<String> ids = List.of("n0", "n1", "n2", "n3", "n4");
        RecordingStateMachine leaderSm = new RecordingStateMachine();
        RaftNode leader = new RaftNode("n0", ids.subList(1, 5), leaderSm, () -> 1);

        List<RaftPeer> peers = new ArrayList<>();
        peers.add(new InProcessRaftPeer(follower("n1", ids)));
        peers.add(new InProcessRaftPeer(follower("n2", ids)));
        BlockablePeer slow = new BlockablePeer(follower("n3", ids));
        BlockablePeer alsoSlow = new BlockablePeer(follower("n4", ids));
        peers.add(slow);
        peers.add(alsoSlow);
        for (RaftPeer peer : peers) {
            leader.registerPeer(peer);
        }

        leader.tick();
        assertTrue(leader.isLeader());

        try (RaftReplicationDriver driver =
                new RaftReplicationDriver(leader, peers, Duration.ofMillis(10)).start()) {
            // Wait for the §8 barrier to commit before wedging the minority, so the test is
            // measuring the proposal path rather than the election.
            assertTrue(leader.awaitApplied(1, Duration.ofSeconds(5)));

            slow.blocking.set(true);
            alsoSlow.blocking.set(true);
            assertTrue(slow.entered.await(5, TimeUnit.SECONDS), "the slow peer must be wedged");
            assertTrue(alsoSlow.entered.await(5, TimeUnit.SECONDS));

            try {
                // n1 and n2 plus the leader are a majority of five. The proposal must complete on
                // that majority, with two peers still held inside appendEntries.
                long index = driver.propose("committed".getBytes(UTF_8), Duration.ofSeconds(5));

                assertEquals(2, index);
                assertEquals(2, leader.commitIndex());
                assertEquals(2, leader.lastApplied());
                assertEquals(List.of("committed"), leaderSm.applied);
                assertEquals(1, slow.inFlight.get(),
                    "the slow peer is still parked inside its AppendEntries call");
                assertEquals(1, alsoSlow.inFlight.get(),
                    "the second slow peer is still parked inside its AppendEntries call");
            } finally {
                slow.release.countDown();
                alsoSlow.release.countDown();
            }
        }
    }

    /**
     * The second half of it: peer I/O held the node monitor, so a wedged peer also froze
     * inbound RPC handling. With replication decoupled, the node still answers RequestVote while a
     * peer is blocked.
     */
    @Test
    void nodeStaysResponsiveToInboundRpcsWhileAPeerIsBlocked() throws Exception {
        List<String> ids = List.of("n0", "n1", "n2");
        RaftNode leader = new RaftNode("n0", ids.subList(1, 3),
            new RecordingStateMachine(), () -> 1);
        BlockablePeer slow = new BlockablePeer(follower("n1", ids));
        List<RaftPeer> peers = List.of(slow, new InProcessRaftPeer(follower("n2", ids)));
        for (RaftPeer peer : peers) {
            leader.registerPeer(peer);
        }
        leader.tick();
        assertTrue(leader.isLeader());

        try (RaftReplicationDriver driver =
                new RaftReplicationDriver(leader, peers, Duration.ofMillis(10)).start()) {
            assertTrue(leader.awaitApplied(1, Duration.ofSeconds(5)));
            slow.blocking.set(true);
            assertTrue(slow.entered.await(5, TimeUnit.SECONDS));

            try {
                // A higher-term RequestVote must be served promptly rather than queueing behind
                // the wedged peer's call.
                CountDownLatch answered = new CountDownLatch(1);
                Thread caller = new Thread(() -> {
                    leader.handleRequestVote(
                        RequestVoteRequest.of(leader.currentTerm() + 1, "n2", 5, 5));
                    answered.countDown();
                });
                caller.setDaemon(true);
                caller.start();

                assertTrue(answered.await(5, TimeUnit.SECONDS),
                    "a blocked peer must not stall inbound RPC handling");
            } finally {
                slow.release.countDown();
            }
        }
    }

    /** The driver must still replicate and commit when every peer is healthy. */
    @Test
    void healthyGroupCommitsThroughTheDriver() throws Exception {
        List<String> ids = List.of("n0", "n1", "n2");
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode leader = new RaftNode("n0", ids.subList(1, 3), sm, () -> 1);
        RaftNode n1 = follower("n1", ids);
        RaftNode n2 = follower("n2", ids);
        List<RaftPeer> peers = List.of(new InProcessRaftPeer(n1), new InProcessRaftPeer(n2));
        for (RaftPeer peer : peers) {
            leader.registerPeer(peer);
        }
        leader.tick();
        assertTrue(leader.isLeader());

        try (RaftReplicationDriver driver =
                new RaftReplicationDriver(leader, peers, Duration.ofMillis(10)).start()) {
            assertEquals(2, driver.propose("a".getBytes(UTF_8), Duration.ofSeconds(5)));
            assertEquals(3, driver.propose("b".getBytes(UTF_8), Duration.ofSeconds(5)));

            assertEquals(List.of("a", "b"), sm.applied);
            assertEquals(3, leader.commitIndex());
        }
    }
}
