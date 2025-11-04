package com.ledgerkv.bench;

import java.util.Random;

/**
 * Classic YCSB Zipfian index generator (Gray et al., "Quickly Generating Billion-Record Synthetic
 * Databases"). Returns indices in {@code [0, items)} with index 0 the hottest. Seeded for
 * reproducibility. Indices are not scrambled, so hot keys cluster at low indices — fine for
 * exercising the engine; spreading hot keys across the keyspace is out of scope.
 */
public final class ZipfianGenerator implements IntGenerator {

    private final long items;
    private final double theta;
    private final double alpha;
    private final double zetan;
    private final double eta;
    private final Random rng;

    public ZipfianGenerator(long items, double zipfianConstant, long seed) {
        if (items <= 0) {
            throw new IllegalArgumentException("items must be positive: " + items);
        }
        this.items = items;
        this.theta = zipfianConstant;
        double zeta2 = zeta(2, theta);
        this.zetan = zeta(items, theta);
        this.alpha = 1.0 / (1.0 - theta);
        this.eta = (1.0 - Math.pow(2.0 / items, 1.0 - theta)) / (1.0 - zeta2 / zetan);
        this.rng = new Random(seed);
    }

    /** Riemann-zeta partial sum sum_{i=1..n} 1/i^theta. */
    private static double zeta(long n, double theta) {
        double sum = 0.0;
        for (long i = 1; i <= n; i++) {
            sum += 1.0 / Math.pow(i, theta);
        }
        return sum;
    }

    public long nextLong() {
        double u = rng.nextDouble();
        double uz = u * zetan;
        if (uz < 1.0) {
            return 0;
        }
        if (uz < 1.0 + Math.pow(0.5, theta)) {
            return 1;
        }
        return (long) (items * Math.pow(eta * u - eta + 1.0, alpha));
    }

    @Override
    public int nextInt() {
        long v = nextLong();
        if (v >= items) {
            v = items - 1;   // guard against floating-point rounding at the tail
        }
        return (int) v;
    }
}
