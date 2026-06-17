package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regressions for the Raft correctness bugs found reviewing commit {@code 4264fd2}. Each test
 * is the reviewer's own counterexample, ported verbatim except where noted in the test's
 * Javadoc: log matching, snapshot install, snapshot transfer, and WAL replay. The stale leader
 * read and the synchronous replication round are covered by {@link RaftReadIndexTest} and
 * {@link RaftReplicationDriverTest}.
 */
class RaftReviewRegressionTest {

    /** A deterministic state machine that records the exact command sequence it applied. */
    static final class RecordingStateMachine implements StateMachine {

        final List<String> applied = new ArrayList<>();

        @Override
        public byte[] apply(byte[] command) {
            applied.add(new String(command, UTF_8));
            return command;
        }

        @Override
        public byte[] snapshot() {
            return String.join(",", applied).getBytes(UTF_8);
        }

        @Override
        public void restore(byte[] data, long lastIncludedIndex, long lastIncludedTerm) {
            applied.clear();
            String text = new String(data, UTF_8);
            if (!text.isEmpty()) {
                Collections.addAll(applied, text.split(","));
            }
        }
    }

    /**
     * A follower reported its whole {@code log.lastIndex()} as matched even though
     * the request only verified an earlier prefix, so the leader advanced {@code nextIndex} past the
     * end of its own log and the next tick threw
     * {@code IndexOutOfBoundsException: no entry at index 3 (base=0, last=1)}.
     */
    @Test
    void longerUncommittedFollowerSuffixMustNotCrashTheLeader() {
        RaftNode a = recoveredNode("a", List.of("b", "c"), 1);
        RaftNode b = recoveredNode("b", List.of("a", "c"), 3);
        RaftNode c = recoveredNode("c", List.of("a", "b"), 1);
        for (RaftNode from : List.of(a, b, c)) {
            for (RaftNode to : List.of(a, b, c)) {
                if (from != to) {
                    from.registerPeer(new InProcessRaftPeer(to));
                }
            }
        }

        // a wins with c; b holds a longer uncommitted suffix and legitimately refuses its vote.
        a.tick();
        assertTrue(a.isLeader());
        assertEquals(1, a.log().lastIndex());
        assertEquals(3, b.log().lastIndex());

        assertDoesNotThrow(a::tick,
            "a follower acknowledgement must not advance nextIndex beyond the leader's own log");
    }

    /** Finding 2, positive side: the ack reflects the prefix this request actually established. */
    @Test
    void acknowledgedMatchIndexIsTheIndexTheRequestEstablished() {
        RaftNode follower = recoveredNode("f", List.of("l", "other"), 3);

        // An empty heartbeat verifying only index 1 must acknowledge 1, not the local tail at 3.
        AppendEntriesResponse heartbeat = follower.handleAppendEntries(
            AppendEntriesRequest.of(2, "l", 1, 1, List.of(), 0));

        assertTrue(heartbeat.success());
        assertEquals(1, heartbeat.matchIndex());
    }

    private static RaftNode recoveredNode(String id, List<String> peers, int entries) {
        List<LogEntry> log = new ArrayList<>();
        for (int i = 1; i <= entries; i++) {
            log.add(LogEntry.of(1, i, ("c" + i).getBytes(UTF_8)));
        }
        return new RaftNode(id, peers, new RecordingStateMachine(),
            () -> id.equals("a") ? 1 : 100, null, new RaftState(1, null, log));
    }

    /**
     * The stale-snapshot check only compared against the compaction base, so a
     * snapshot older than the applied state was accepted — the follower's commit index dropped
     * 2 to 1 and {@code c2} disappeared from the state machine.
     */
    @Test
    void delayedSnapshotMustNotRollBackCommittedState() {
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode follower = new RaftNode("f", List.of("l", "other"), sm, () -> 100);
        follower.handleAppendEntries(AppendEntriesRequest.of(1, "l", 0, 0,
            List.of(LogEntry.of(1, 1, "c1".getBytes(UTF_8)),
                    LogEntry.of(1, 2, "c2".getBytes(UTF_8))), 2));
        assertEquals(List.of("c1", "c2"), sm.applied);

        follower.handleInstallSnapshot(
            InstallSnapshotRequest.of(1, "l", 1, 1, "c1".getBytes(UTF_8)));

        assertEquals(2, follower.commitIndex(), "an older snapshot regressed the committed index");
        assertEquals(List.of("c1", "c2"), sm.applied);
    }

    /**
     * Raft paper section 7: a snapshot that describes a prefix the follower already has must retain
     * the matching suffix rather than discarding the whole log.
     */
    @Test
    void installingASnapshotOverAMatchingPrefixRetainsTheSuffix() {
        RecordingStateMachine sm = new RecordingStateMachine();
        RaftNode follower = new RaftNode("f", List.of("l", "other"), sm, () -> 100);
        // Three entries replicated, none committed yet.
        follower.handleAppendEntries(AppendEntriesRequest.of(1, "l", 0, 0,
            List.of(LogEntry.of(1, 1, "c1".getBytes(UTF_8)),
                    LogEntry.of(1, 2, "c2".getBytes(UTF_8)),
                    LogEntry.of(1, 3, "c3".getBytes(UTF_8))), 0));

        follower.handleInstallSnapshot(
            InstallSnapshotRequest.of(1, "l", 2, 1, "c1,c2".getBytes(UTF_8)));

        assertEquals(2, follower.lastIncludedIndex());
        assertEquals(3, follower.log().lastIndex(), "the matching suffix must survive the install");
        assertEquals(2, follower.commitIndex());
    }

    /**
     * The request took its index/term from the compaction boundary but its bytes
     * from the live state machine, so a snapshot claiming to cover index 2 already contained c3 and
     * the follower applied c3 twice — {@code [c1, c2, c3, c3]}.
     */
    @Test
    void transferredSnapshotMustMatchItsDeclaredLogIndex() {
        RecordingStateMachine leaderSm = new RecordingStateMachine();
        RecordingStateMachine fastSm = new RecordingStateMachine();
        RecordingStateMachine laggingSm = new RecordingStateMachine();
        RaftNode leader = new RaftNode("n0", List.of("n1", "n2"), leaderSm, () -> 1);
        RaftNode fast = new RaftNode("n1", List.of("n0", "n2"), fastSm, () -> 100);
        RaftNode lagging = new RaftNode("n2", List.of("n0", "n1"), laggingSm, () -> 100);
        java.util.concurrent.atomic.AtomicBoolean reachable =
            new java.util.concurrent.atomic.AtomicBoolean(false);
        leader.registerPeer(new InProcessRaftPeer(fast));
        leader.registerPeer(new InProcessRaftPeer(lagging, reachable::get));

        leader.tick();
        assertTrue(leader.isLeader());
        leader.propose("c1".getBytes(UTF_8));
        leader.propose("c2".getBytes(UTF_8));
        leader.setCompactionThreshold(2);
        leader.maybeCompact();
        assertEquals(2, leader.lastIncludedIndex());
        leader.propose("c3".getBytes(UTF_8));
        assertEquals(3, leader.lastApplied());

        reachable.set(true);
        leader.tick(); // ships the snapshot: base 2 must contain only c1 and c2
        assertEquals(2, lagging.lastApplied());
        leader.tick(); // replicates and applies c3 exactly once

        assertEquals(List.of("c1", "c2", "c3"), laggingSm.applied,
            "the snapshot included c3 while claiming to cover only indices through 2");
    }

    /**
     * Replay compared the shortened entry list's size against absolute log
     * indices, so re-recording the same index after a snapshot produced two entries at index 3.
     */
    @Test
    void repeatedTailEntryAfterSnapshotMustReplayOnce(@TempDir Path dir) throws Exception {
        try (RaftPersistence persistence = RaftPersistence.open(dir)) {
            persistence.recordEntry(LogEntry.of(1, 1, "c1".getBytes(UTF_8)));
            persistence.recordEntry(LogEntry.of(1, 2, "c2".getBytes(UTF_8)));
            persistence.recordSnapshot(Snapshot.of(2, 1, "c1,c2".getBytes(UTF_8)));
            persistence.recordEntry(LogEntry.of(1, 3, "c3".getBytes(UTF_8)));
            persistence.recordEntry(LogEntry.of(1, 3, "c3".getBytes(UTF_8)));
        }

        RaftState recovered = RaftPersistence.replay(dir);

        assertEquals(1, recovered.entries().size(),
            "replaying the same absolute index duplicated the entry after compaction");
        assertEquals(3, recovered.entries().get(0).index());
    }

    /**
     * The same absolute-versus-relative confusion let a truncated entry survive,
     * so recovery kept the old index-3 entry alongside its replacement.
     */
    @Test
    void truncatedTailAfterSnapshotMustNotReappear(@TempDir Path dir) throws Exception {
        try (RaftPersistence persistence = RaftPersistence.open(dir)) {
            persistence.recordEntry(LogEntry.of(1, 1, "c1".getBytes(UTF_8)));
            persistence.recordEntry(LogEntry.of(1, 2, "c2".getBytes(UTF_8)));
            persistence.recordSnapshot(Snapshot.of(2, 1, "c1,c2".getBytes(UTF_8)));
            persistence.recordEntry(LogEntry.of(1, 3, "old".getBytes(UTF_8)));
            persistence.recordTruncate(3);
            persistence.recordEntry(LogEntry.of(2, 3, "new".getBytes(UTF_8)));
        }

        RaftState recovered = RaftPersistence.replay(dir);

        assertEquals(1, recovered.entries().size(),
            "a truncated absolute index survived snapshot-tail recovery");
        assertEquals("new", new String(recovered.entries().get(0).command(), UTF_8));
    }

    /** The review's passing control: a majority still commits with two nodes unreachable. */
    @Test
    void majorityCanCommitWithTwoUnavailableNodes() {
        RaftNode leader = new RaftNode(
            "n0", List.of("n1", "n2", "n3", "n4"), new RecordingStateMachine(), () -> 1);
        for (String id : List.of("n1", "n2", "n3", "n4")) {
            List<String> peers = new ArrayList<>(List.of("n0", "n1", "n2", "n3", "n4"));
            peers.remove(id);
            RaftNode follower = new RaftNode(id, peers, new RecordingStateMachine(), () -> 100);
            leader.registerPeer(new InProcessRaftPeer(
                follower, () -> !id.equals("n1") && !id.equals("n2")));
        }

        leader.tick();
        assertTrue(leader.isLeader());

        assertEquals(1, leader.propose("c1".getBytes(UTF_8)));
        assertEquals(1, leader.commitIndex());
        assertEquals(1, leader.lastApplied());
    }
}
