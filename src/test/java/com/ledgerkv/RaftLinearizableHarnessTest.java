package com.ledgerkv;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationResult;
import org.junit.jupiter.api.Test;

class RaftLinearizableHarnessTest {

    @Test
    void recordsNonEmptyAllSuccessHistory() {
        OperationHistory history = RaftLinearizableHarness.recordHistory();
        assertFalse(history.getOperations().isEmpty(), "harness should record operations");
        for (OperationRecord op : history.getOperations()) {
            assertTrue(op.getResult() == OperationResult.SUCCESS,
                "every Raft op in the deterministic harness commits: " + op);
        }
    }
}
