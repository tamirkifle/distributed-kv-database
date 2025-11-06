package com.ledgerkv.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UniformGeneratorTest {

    @Test
    void valuesStayInRange() {
        UniformGenerator gen = new UniformGenerator(100, 1L);
        for (int i = 0; i < 10_000; i++) {
            int v = gen.nextInt();
            assertTrue(v >= 0 && v < 100, "out of range: " + v);
        }
    }

    @Test
    void sameSeedIsReproducible() {
        UniformGenerator a = new UniformGenerator(1000, 42L);
        UniformGenerator b = new UniformGenerator(1000, 42L);
        for (int i = 0; i < 1000; i++) {
            assertEquals(a.nextInt(), b.nextInt());
        }
    }

    @Test
    void rejectsNonPositiveItems() {
        assertThrows(IllegalArgumentException.class, () -> new UniformGenerator(0, 1L));
    }
}
