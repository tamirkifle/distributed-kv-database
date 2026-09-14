package com.ledgerkv.checker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * How the checker treats an operation whose client never learned the outcome.
 *
 * <p>These exist because the alternative reading — a timeout is a failure — is both tempting and
 * wrong, and it fails in the direction that costs the most: it invents violations in histories
 * that are perfectly legal, so a run that should have been clean produces a counterexample nobody
 * can reproduce.
 */
class LinearizabilityUnknownOutcomeTest {

    @Test
    void aTimedOutWriteThatDidCommitIsNotAViolation() {
        // The client gave up at tick 20. The entry committed anyway, and the read at 30 sees it.
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .unknownWrite("k", "v1", 10, 20)
                .read("k", "v1", 30, 40)
                .build());

        assertTrue(result.isLinearizable(), result.describeWitness());
    }

    @Test
    void thatSameHistoryIsAFalseViolationIfTheWriteIsCalledAFailure() {
        // The bug this guards, stated as a test: same history, timeout recorded as a definite
        // failure. The write vanishes, nothing ever wrote v1, and the read cannot be explained.
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .failedWrite("k", "v1", 10, 20)
                .read("k", "v1", 30, 40)
                .build());

        assertFalse(result.isLinearizable(),
                "a definite failure really does make this history impossible");
    }

    @Test
    void aTimedOutWriteThatNeverCommittedIsAlsoNotAViolation() {
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .write("k", "v0", 0, 5)
                .unknownWrite("k", "v1", 10, 20)
                .read("k", "v0", 30, 40)
                .build());

        assertTrue(result.isLinearizable(),
                "leaving the unknown write out is a legal reading of the history");
    }

    @Test
    void anUnknownWriteMayTakeEffectLongAfterItsClientGaveUp() {
        // v1's client gave up at 20, v2 committed at 50, and the read at 70 still sees v1. That is
        // legal only if v1 applied after v2 — which an unknown write may, having no end time.
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .unknownWrite("k", "v1", 10, 20)
                .write("k", "v2", 40, 50)
                .read("k", "v1", 70, 80)
                .build());

        assertTrue(result.isLinearizable(), result.describeWitness());
    }

    @Test
    void anUnknownWriteCannotTakeEffectBeforeItWasInvoked() {
        // The read finishes at 5, before the write is even sent at 10. No ordering saves this.
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .read("k", "v1", 0, 5)
                .unknownWrite("k", "v1", 10, 20)
                .build());

        assertFalse(result.isLinearizable(),
                "an unknown outcome loosens the end of an operation, never its start");
    }

    @Test
    void aRealViolationIsStillCaughtWhenUnknownsArePresent() {
        // v0 is committed and read back, then the register goes backwards to a value nobody wrote.
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .write("k", "v0", 0, 5)
                .unknownWrite("k", "v1", 10, 20)
                .read("k", "ghost", 30, 40)
                .build());

        assertFalse(result.isLinearizable(),
                "optional ops must not become a licence to explain away any read");
    }

    @Test
    void anUnknownReadIsDroppedBecauseItObservedNothing() {
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .write("k", "v0", 0, 5)
                .unknownRead("k", 10, 20)
                .read("k", "v0", 30, 40)
                .build());

        assertTrue(result.isLinearizable(), result.describeWitness());
    }

    @Test
    void theWitnessSaysWhichOperationsWereUncertain() {
        LinearizabilityResult result = new LinearizabilityChecker().check(new HistoryBuilder()
                .unknownWrite("k", "v1", 0, 5)
                .read("k", "v1", 10, 20)
                .read("k", "ghost", 30, 40)
                .build());

        assertFalse(result.isLinearizable());
        assertTrue(result.getLinearizedPrefix().stream().anyMatch(s -> s.contains("unknown")),
                "a counterexample built on a guess must say so: " + result.describeWitness());
    }
}
