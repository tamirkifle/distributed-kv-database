package com.ledgerkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationType;
import java.util.List;
import org.junit.jupiter.api.Test;

class QuorumStaleReadHarnessTest {

    @Test
    void recordsAStaleReadOfAnEarlierValueAfterANewerWrite() {
        OperationHistory history = QuorumStaleReadHarness.recordHistory();
        List<OperationRecord> ops = history.getOperations();
        assertTrue(ops.size() >= 3, "expected write A, write B, read");

        // The harness writes A then B then reads; the read observes the stale A.
        OperationRecord lastRead = null;
        String lastWriteValue = null;
        for (OperationRecord op : ops) {
            if (op.getType() == OperationType.WRITE) {
                lastWriteValue = op.getValue();
            } else if (op.getType() == OperationType.READ) {
                lastRead = op;
            }
        }
        assertEquals("B", lastWriteValue, "last write should be B");
        assertEquals("A", lastRead.getValue(), "read observed the stale value A (W+R<=N)");
    }
}
