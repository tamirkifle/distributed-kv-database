package com.ledgerkv.bench;

import com.ledgerkv.storage.StorageEngine;
import com.ledgerkv.storage.lsm.Entry;
import java.util.Iterator;

/** Applies {@link Operation}s to a {@link StorageEngine}. */
public final class WorkloadRunner {

    private WorkloadRunner() {
    }

    /** Executes one op and returns the number of entries it touched (for result consumption). */
    public static int execute(StorageEngine engine, Operation op) {
        switch (op.type()) {
            case READ:
                return engine.get(op.key()).isPresent() ? 1 : 0;
            case UPDATE:
            case INSERT:
                engine.put(op.key(), op.value());
                return 1;
            case READ_MODIFY_WRITE:
                int present = engine.get(op.key()).isPresent() ? 1 : 0;
                engine.put(op.key(), op.value());
                return present + 1;
            case SCAN:
                Iterator<Entry> it = engine.scan(op.key(), null);
                int n = 0;
                while (it.hasNext() && n < op.scanLength()) {
                    it.next();
                    n++;
                }
                return n;
            default:
                throw new IllegalArgumentException("unknown op type: " + op.type());
        }
    }
}
