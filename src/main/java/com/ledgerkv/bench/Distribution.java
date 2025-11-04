package com.ledgerkv.bench;

/** Key-access distribution for the run phase. */
public enum Distribution {

    UNIFORM {
        @Override
        public IntGenerator create(int items, long seed) {
            return new UniformGenerator(items, seed);
        }
    },

    ZIPFIAN {
        @Override
        public IntGenerator create(int items, long seed) {
            return new ZipfianGenerator(items, 0.99, seed);
        }
    };

    /** A fresh seeded index generator over {@code [0, items)}. */
    public abstract IntGenerator create(int items, long seed);
}
