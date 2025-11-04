package com.ledgerkv.checker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinearizabilityCheckerTest {

    private final LinearizabilityChecker checker = new LinearizabilityChecker();

    @Test
    void historyBuilderProducesRecordsInOrderWithMonotonicTimes() {
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 10)
            .read("k", "v0", 20, 30)
            .build();

        assertEquals(2, history.getOperations().size());
        OperationRecord write = history.getOperations().get(0);
        OperationRecord read = history.getOperations().get(1);
        assertEquals(OperationType.WRITE, write.getType());
        assertEquals("v0", write.getValue());
        assertEquals(OperationType.READ, read.getType());
        // read starts strictly after write ends => real-time-before holds
        assertSame(true, read.getStartTime().isAfter(write.getEndTime()));
    }

    @Test
    void historyBuilderReadCanObserveNull() {
        OperationHistory history = new HistoryBuilder()
            .read("k", null, 0, 10)
            .build();
        assertNull(history.getOperations().get(0).getValue());
    }

    @Test
    void resultLinearizableHasNoWitness() {
        LinearizabilityResult result = LinearizabilityResult.linearizable();
        org.junit.jupiter.api.Assertions.assertTrue(result.isLinearizable());
        assertNull(result.getStuckKey());
        assertEquals(java.util.List.of(), result.getLinearizedPrefix());
        assertEquals(java.util.List.of(), result.getStuckFrontier());
    }

    @Test
    void resultNotLinearizableCarriesWitness() {
        LinearizabilityResult result = LinearizabilityResult.notLinearizable(
            "k",
            java.util.List.of("WRITE k=v0"),
            java.util.List.of("READ k=v1"));
        org.junit.jupiter.api.Assertions.assertFalse(result.isLinearizable());
        assertEquals("k", result.getStuckKey());
        assertEquals(java.util.List.of("WRITE k=v0"), result.getLinearizedPrefix());
        assertEquals(java.util.List.of("READ k=v1"), result.getStuckFrontier());
        org.junit.jupiter.api.Assertions.assertTrue(result.describeWitness().contains("k"));
    }

    @Test
    void emptyHistoryIsLinearizable() {
        assertTrue(checker.check(OperationHistory.empty()).isLinearizable());
    }

    @Test
    void sequentialWriteThenReadIsLinearizable() {
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 10)
            .read("k", "v0", 20, 30)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void readBeforeAnyWriteObservingNullIsLinearizable() {
        OperationHistory history = new HistoryBuilder()
            .read("k", null, 0, 10)
            .write("k", "v0", 20, 30)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void concurrentWriteAndReadIsLinearizableWhenAnOrderingExists() {
        // write overlaps read; reading v0 is legal if the write linearizes first
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 30)
            .read("k", "v0", 10, 20)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void concurrentWritesWithReadsPickingEitherAreLinearizable() {
        // two concurrent writes; reads observe v1 -> order w0<w1 works
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 100)
            .write("k", "v1", 0, 100)
            .read("k", "v1", 110, 120)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void staleReadAfterCommittedWriteIsNotLinearizable() {
        // w(v0) completes [0,10], then w(v1) completes [20,30] (strictly after),
        // then r() [40,50] returns v0 -> impossible: v1 already committed before the read started
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 10)
            .write("k", "v1", 20, 30)
            .read("k", "v0", 40, 50)
            .build();
        LinearizabilityResult result = checker.check(history);
        assertFalse(result.isLinearizable());
        assertEquals("k", result.getStuckKey());
        assertTrue(result.describeWitness().contains("k"));
    }

    @Test
    void lostUpdateIsNotLinearizable() {
        // two concurrent writes before all reads; reads are sequential and observe
        // v0 then v1 then v0 again -- a LWW register cannot revert v1->v0 with no rewrite
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 10)
            .write("k", "v1", 0, 10)
            .read("k", "v0", 20, 30)
            .read("k", "v1", 40, 50)
            .read("k", "v0", 60, 70)
            .build();
        LinearizabilityResult result = checker.check(history);
        assertFalse(result.isLinearizable());
        assertEquals("k", result.getStuckKey());
    }

    @Test
    void failedReadIsIgnored() {
        // a failed read carries no constraint; the rest is linearizable
        OperationHistory history = new HistoryBuilder()
            .write("k", "v0", 0, 10)
            .failedRead("k", 20, 30)
            .read("k", "v0", 40, 50)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void independentKeysAreCheckedIndependently() {
        // key a is fine; key b has a stale read -> whole history not linearizable, stuck on b
        OperationHistory history = new HistoryBuilder()
            .write("a", "a0", 0, 10)
            .read("a", "a0", 20, 30)
            .write("b", "b0", 0, 10)
            .write("b", "b1", 20, 30)
            .read("b", "b0", 40, 50)
            .build();
        LinearizabilityResult result = checker.check(history);
        assertFalse(result.isLinearizable());
        assertEquals("b", result.getStuckKey());
    }

    @Test
    void mixedKeyLinearizableHistoryIsAccepted() {
        OperationHistory history = new HistoryBuilder()
            .write("a", "a0", 0, 10)
            .write("b", "b0", 0, 10)
            .read("a", "a0", 20, 30)
            .read("b", "b0", 20, 30)
            .build();
        assertTrue(checker.check(history).isLinearizable());
    }

    @Test
    void exceedingHistoryBoundThrows() {
        HistoryBuilder builder = new HistoryBuilder();
        for (int i = 0; i < 5; i++) {
            builder.write("k", "v" + i, i * 10L, i * 10L + 5);
        }
        OperationHistory history = builder.build();
        LinearizabilityChecker bounded = new LinearizabilityChecker(3);
        org.junit.jupiter.api.Assertions.assertThrows(
            LinearizabilityCheckException.class, () -> bounded.check(history));
    }
}
