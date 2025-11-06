package com.ledgerkv.bench;

/** Produces a stream of indices in {@code [0, items)} per some distribution. */
public interface IntGenerator {

    /** The next index in {@code [0, items)}. */
    int nextInt();
}
