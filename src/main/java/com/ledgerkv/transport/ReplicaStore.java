package com.ledgerkv.transport;

import com.ledgerkv.storage.lsm.LsmEngine;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The causal storage layer of a single replica: the read-merge-write cycle that every incoming
 * replica write goes through, over an {@link LsmEngine}.
 *
 * <p>Both replica transports use it — {@code LocalReplicaClient} in-process and {@code NodeServer}
 * over gRPC — so a value cannot be rolled backward on one path and protected on the other. Before
 * this existed, both simply overwrote whatever was there.
 *
 * <p>Does not own the engine lifecycle: the caller opens and closes the {@link LsmEngine}.
 */
public final class ReplicaStore {

    private final LsmEngine engine;
    private final KeyLocks locks = new KeyLocks();

    public ReplicaStore(LsmEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine must not be null");
    }

    /** The values currently held for {@code key}: empty if absent, more than one if conflicted. */
    public List<StoredValue> get(String key) {
        return engine.get(key)
                .map(StoredValueCodec::decodeAll)
                .orElse(Collections.emptyList());
    }

    /**
     * Merges {@code incoming} into whatever this replica already holds and persists the result,
     * with the whole cycle serialized against other writers of the same key.
     *
     * @return the sibling set now stored
     */
    public List<StoredValue> merge(String key, StoredValue incoming) {
        ReentrantLock lock = locks.forKey(key);
        lock.lock();
        try {
            List<StoredValue> merged = Siblings.merge(get(key), incoming);
            engine.put(key, StoredValueCodec.encodeAll(merged));
            return merged;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replaces everything stored for {@code key} with {@code values}, bypassing the causal merge.
     * Only for a caller that has already resolved the conflict and is deliberately collapsing
     * siblings — never for an ordinary replica write.
     */
    public void replace(String key, List<StoredValue> values) {
        ReentrantLock lock = locks.forKey(key);
        lock.lock();
        try {
            engine.put(key, StoredValueCodec.encodeAll(values));
        } finally {
            lock.unlock();
        }
    }
}
