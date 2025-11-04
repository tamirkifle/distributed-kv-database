package com.ledgerkv.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ZipfianGeneratorTest {

    @Test
    void valuesStayInRange() {
        ZipfianGenerator gen = new ZipfianGenerator(1000, 0.99, 1L);
        for (int i = 0; i < 100_000; i++) {
            int v = gen.nextInt();
            assertTrue(v >= 0 && v < 1000, "out of range: " + v);
        }
    }

    @Test
    void sameSeedIsReproducible() {
        ZipfianGenerator a = new ZipfianGenerator(1000, 0.99, 7L);
        ZipfianGenerator b = new ZipfianGenerator(1000, 0.99, 7L);
        for (int i = 0; i < 1000; i++) {
            assertEquals(a.nextInt(), b.nextInt());
        }
    }

    @Test
    void distributionIsSkewedTowardLowIndices() {
        int items = 1000;
        ZipfianGenerator gen = new ZipfianGenerator(items, 0.99, 3L);
        long[] counts = new long[items];
        for (int i = 0; i < 200_000; i++) {
            counts[gen.nextInt()]++;
        }
        // The hottest key (index 0) must dominate the coldest decile by a wide margin.
        long coldTail = 0;
        for (int i = items - 100; i < items; i++) {
            coldTail += counts[i];
        }
        assertTrue(counts[0] > coldTail,
                "index 0 (" + counts[0] + ") should beat the cold 100-key tail (" + coldTail + ")");
    }

    @Test
    void rejectsNonPositiveItems() {
        assertThrows(IllegalArgumentException.class, () -> new ZipfianGenerator(0, 0.99, 1L));
    }
}
