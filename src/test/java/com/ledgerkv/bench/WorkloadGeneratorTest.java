package com.ledgerkv.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class WorkloadGeneratorTest {

    private static WorkloadConfig cfg(Workload w, Distribution d) {
        return new WorkloadConfig(w, 1000, 5000, 64, 100, d, 42L);
    }

    @Test
    void loadInsertsEveryRecordWithDistinctAscendingKeys() {
        WorkloadGenerator gen = new WorkloadGenerator(cfg(Workload.A, Distribution.ZIPFIAN));
        List<Operation> load = gen.load();
        assertEquals(1000, load.size());
        String prev = null;
        for (Operation op : load) {
            assertEquals(OpType.INSERT, op.type());
            assertEquals(64, op.value().length);
            if (prev != null) {
                assertTrue(prev.compareTo(op.key()) < 0, "load keys must be strictly ascending");
            }
            prev = op.key();
        }
    }

    @Test
    void runProducesOperationCountOps() {
        WorkloadGenerator gen = new WorkloadGenerator(cfg(Workload.A, Distribution.ZIPFIAN));
        assertEquals(5000, gen.run().size());
    }

    @Test
    void workloadCisAllReads() {
        WorkloadGenerator gen = new WorkloadGenerator(cfg(Workload.C, Distribution.UNIFORM));
        for (Operation op : gen.run()) {
            assertEquals(OpType.READ, op.type());
        }
    }

    @Test
    void workloadAisRoughlyHalfReads() {
        WorkloadGenerator gen = new WorkloadGenerator(cfg(Workload.A, Distribution.UNIFORM));
        long reads = gen.run().stream().filter(o -> o.type() == OpType.READ).count();
        double frac = reads / 5000.0;
        assertTrue(frac > 0.45 && frac < 0.55, "read fraction should be ~0.5 but was " + frac);
    }

    @Test
    void workloadEemitsBoundedScans() {
        WorkloadGenerator gen = new WorkloadGenerator(cfg(Workload.E, Distribution.ZIPFIAN));
        long scans = 0;
        for (Operation op : gen.run()) {
            if (op.type() == OpType.SCAN) {
                scans++;
                assertTrue(op.scanLength() >= 1 && op.scanLength() <= 100,
                        "scan length out of bounds: " + op.scanLength());
            }
        }
        assertTrue(scans > 0, "workload E should emit scans");
    }

    @Test
    void runIsReproducibleForSameSeed() {
        List<Operation> a = new WorkloadGenerator(cfg(Workload.F, Distribution.ZIPFIAN)).run();
        List<Operation> b = new WorkloadGenerator(cfg(Workload.F, Distribution.ZIPFIAN)).run();
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).type(), b.get(i).type());
            assertEquals(a.get(i).key(), b.get(i).key());
        }
    }
}
