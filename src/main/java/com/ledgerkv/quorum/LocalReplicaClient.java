package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.transport.ReplicaStore;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link ReplicaClient} that reads/writes a same-JVM node's {@link LsmEngine} directly through a
 * {@link ReplicaStore}, so an in-process replica performs the same causal read-merge-write cycle as
 * a remote one. This is the honest local-node storage path — the replica persists to its own WAL +
 * SSTables.
 *
 * <p>Does not own the engine lifecycle: the caller opens and closes the {@link LsmEngine}.
 */
public final class LocalReplicaClient implements ReplicaClient {

    private final String nodeId;
    private final ReplicaStore store;

    public LocalReplicaClient(String nodeId, LsmEngine engine) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.store = new ReplicaStore(Objects.requireNonNull(engine, "engine must not be null"));
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public List<VersionedValue> get(String key) {
        return store.get(key).stream()
                .map(ReplicaValues::fromStored)
                .collect(Collectors.toList());
    }

    @Override
    public void put(String key, VersionedValue value) {
        store.merge(key, ReplicaValues.toStored(value));
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        put(key, value);
    }
}
