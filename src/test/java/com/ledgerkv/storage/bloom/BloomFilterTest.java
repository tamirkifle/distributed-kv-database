package com.ledgerkv.storage.bloom;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class BloomFilterTest {

    @Test
    void noFalseNegatives() {
        BloomFilter bf = BloomFilter.create(10_000, 0.01);
        for (int i = 0; i < 10_000; i++) {
            bf.add("key-" + i);
        }
        for (int i = 0; i < 10_000; i++) {
            assertTrue(bf.mightContain("key-" + i), "an added key must always report present: " + i);
        }
    }

    @Test
    void falsePositiveRateStaysNearTheBound() {
        int n = 10_000;
        double p = 0.01;
        BloomFilter bf = BloomFilter.create(n, p);
        for (int i = 0; i < n; i++) {
            bf.add("present-" + i);
        }
        int falsePositives = 0;
        int trials = 100_000;
        for (int i = 0; i < trials; i++) {
            if (bf.mightContain("absent-" + i)) { // disjoint from the "present-" keys
                falsePositives++;
            }
        }
        double observed = (double) falsePositives / trials;
        // Deterministic (fixed key set, no RNG). Generous 3x headroom over the target keeps it robust.
        assertTrue(observed < p * 3, "observed FPP " + observed + " should stay near target " + p);
    }

    @Test
    void sizingMathMatchesTheFormula() {
        // n=1000, p=0.01: m = ceil(-1000*ln(0.01)/(ln2)^2) = 9586; k = round(9.586*ln2) = 7
        BloomFilter bf = BloomFilter.create(1000, 0.01);
        assertEquals(9586, bf.numBits());
        assertEquals(7, bf.numHashes());
    }

    @Test
    void serializeRoundTripPreservesMembership() {
        BloomFilter bf = BloomFilter.create(1000, 0.01);
        for (int i = 0; i < 1000; i++) {
            bf.add("k" + i);
        }
        BloomFilter restored = BloomFilter.deserialize(bf.serialize());
        assertEquals(bf.numBits(), restored.numBits());
        assertEquals(bf.numHashes(), restored.numHashes());
        for (int i = 0; i < 1000; i++) {
            assertTrue(restored.mightContain("k" + i));
        }
    }

    @Test
    void rejectsNonsensicalFalsePositiveRate() {
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.create(100, 0.0));
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.create(100, 1.0));
    }
}
