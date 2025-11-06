package com.ledgerkv.bench;

import java.util.Random;

/** Uniformly random index generator over {@code [0, items)}, seeded for reproducibility. */
public final class UniformGenerator implements IntGenerator {

    private final int items;
    private final Random rng;

    public UniformGenerator(int items, long seed) {
        if (items <= 0) {
            throw new IllegalArgumentException("items must be positive: " + items);
        }
        this.items = items;
        this.rng = new Random(seed);
    }

    @Override
    public int nextInt() {
        return rng.nextInt(items);
    }
}
