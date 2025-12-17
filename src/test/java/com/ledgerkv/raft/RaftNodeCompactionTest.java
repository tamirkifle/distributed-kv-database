package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftNodeCompactionTest {

    /** Records applied commands and supports snapshot/restore of that applied list. */
    private static final class CountingSm implements StateMachine {
        final List<String> applied = new ArrayList<>();
        @Override public byte[] apply(byte[] command) {
            applied.add(new String(command, UTF_8));
            return command;
        }
        @Override public byte[] snapshot() {
            return String.join(",", applied).getBytes(UTF_8);
        }
        @Override public void restore(byte[] data, long idx, long term) {
            applied.clear();
            String s = new String(data, UTF_8);
            if (!s.isEmpty()) {
                Collections.addAll(applied, s.split(","));
            }
        }
    }

    private static RaftNode singleLeader(CountingSm sm, int threshold) {
        RaftNode node = new RaftNode("n0", Collections.emptyList(), sm, () -> 1);
        node.setCompactionThreshold(threshold);
        node.tick(); // single node self-elects
        assertTrue(node.isLeader());
        return node;
    }

    @Test
    void compactsAppliedPrefixAndShrinksLog() {
        CountingSm sm = new CountingSm();
        RaftNode node = singleLeader(sm, 3);
        for (int i = 1; i <= 5; i++) {
            node.propose(("c" + i).getBytes(UTF_8)); // single-node: each commits+applies immediately
        }
        assertEquals(5, node.log().lastIndex());
        assertEquals(5, node.lastApplied());

        node.maybeCompact();

        assertEquals(5, node.lastIncludedIndex(), "compacted through lastApplied");
        assertEquals(0, node.log().size(), "physical entries dropped");
        assertEquals(5, node.log().lastIndex(), "absolute last index preserved");
        // a new proposal still appends at the next dense absolute index
        assertEquals(6, node.propose("c6".getBytes(UTF_8)));
    }

    @Test
    void doesNotCompactBelowThreshold() {
        CountingSm sm = new CountingSm();
        RaftNode node = singleLeader(sm, 100);
        for (int i = 1; i <= 5; i++) {
            node.propose(("c" + i).getBytes(UTF_8));
        }
        node.maybeCompact();
        assertEquals(0, node.lastIncludedIndex(), "below threshold: no compaction");
        assertEquals(5, node.log().size());
    }

    @Test
    void restoresStateMachineFromRecoveredSnapshotOnConstruct() {
        CountingSm restored = new CountingSm();
        Snapshot snap = Snapshot.of(3, 1, "c1,c2,c3".getBytes(UTF_8));
        RaftState recovered = new RaftState(1, "n0",
                List.of(LogEntry.of(1, 4, "c4".getBytes(UTF_8))), snap);

        RaftNode node = new RaftNode("n0", Collections.emptyList(), restored, () -> 1, null, recovered);

        assertEquals(3, node.lastIncludedIndex());
        assertEquals(4, node.log().lastIndex());
        // state machine restored to the snapshot's 3 applied commands
        assertEquals(List.of("c1", "c2", "c3"), restored.applied);
    }
}
