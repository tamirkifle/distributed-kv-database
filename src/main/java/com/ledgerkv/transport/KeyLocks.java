package com.ledgerkv.transport;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Serializes read-merge-write cycles per key within one replica.
 *
 * <p>A causal merge is only correct if nothing interleaves between reading the current siblings and
 * writing the merged result — otherwise two concurrent arrivals both read the old set and the
 * second write erases the first. Riak gets this for free because a vnode is a single-threaded
 * process; here it takes an explicit lock.
 *
 * <p>Locks are striped rather than per-key so the table cannot grow without bound. Two unrelated
 * keys may share a stripe and briefly serialize against each other, which costs a little
 * concurrency and never costs correctness.
 */
final class KeyLocks {

    /** Enough stripes that unrelated keys rarely collide, small enough to be free to allocate. */
    private static final int STRIPES = 64;

    private final ReentrantLock[] locks = new ReentrantLock[STRIPES];

    KeyLocks() {
        for (int i = 0; i < STRIPES; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    ReentrantLock forKey(String key) {
        // Spread hashCode's low bits, which cluster badly for short similar keys.
        int hash = key.hashCode();
        hash ^= (hash >>> 16);
        return locks[Math.floorMod(hash, STRIPES)];
    }
}
