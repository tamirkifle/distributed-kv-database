package com.ledgerkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.checker.LinearizabilityChecker;
import com.ledgerkv.checker.LinearizabilityResult;
import com.ledgerkv.checker.OperationHistory;
import org.junit.jupiter.api.Test;

/**
 * The headline correctness artifact: the same {@link LinearizabilityChecker}, pointed at both
 * consistency models under deterministic, seeded partition injection, empirically separates them —
 * the Raft (CP) KV path is linearizable; the leaderless quorum (AP) path with W+R<=N is not. Fully
 * deterministic: no threads, no wall-clock timing, reproducible on every CI run.
 */
class CorrectnessHarnessTest {

    private final LinearizabilityChecker checker = new LinearizabilityChecker();

    @Test
    void raftKvPathIsLinearizableUnderPartition() {
        OperationHistory history = RaftLinearizableHarness.recordHistory();
        LinearizabilityResult result = checker.check(history);
        assertTrue(result.isLinearizable(),
            "Raft KV path must be linearizable; witness=" + result.describeWitness());
    }

    @Test
    void quorumPathWithWPlusRLeqNIsNotLinearizableUnderPartition() {
        OperationHistory history = QuorumStaleReadHarness.recordHistory();
        LinearizabilityResult result = checker.check(history);
        assertFalse(result.isLinearizable(),
            "quorum W+R<=N path must yield a linearizability violation under partition");
        // A real, debuggable witness must be present (this is the artifact's payoff).
        assertEquals("x", result.getStuckKey());
        assertFalse(result.getStuckFrontier().isEmpty(),
            "violation must carry a witness frontier: " + result.describeWitness());
    }

    @Test
    void theContrastHolds_raftLinearizableQuorumNot() {
        LinearizabilityResult raft = checker.check(RaftLinearizableHarness.recordHistory());
        LinearizabilityResult quorum = checker.check(QuorumStaleReadHarness.recordHistory());
        assertTrue(raft.isLinearizable() && !quorum.isLinearizable(),
            "the defining contrast: Raft linearizable, quorum (W+R<=N) not. raft=" + raft
                + " quorum=" + quorum);
    }
}
