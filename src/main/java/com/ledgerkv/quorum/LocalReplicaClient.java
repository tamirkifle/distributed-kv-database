package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.transport.StoredValue;
import com.ledgerkv.transport.StoredValueCodec;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link ReplicaClient} that reads/writes a same-JVM node's {@link LsmEngine} directly, serializing
 * the versioned value through {@link StoredValueCodec}. This is the honest local-node storage path —
 * the replica persists to its own WAL + SSTables.
 *
 * <p>Does not own the engine lifecycle: the caller opens and closes the {@link LsmEngine}.
 */
public final class LocalReplicaClient implements ReplicaClient {

    private final String nodeId;
    private final LsmEngine engine;

    public LocalReplicaClient(String nodeId, LsmEngine engine) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.engine = Objects.requireNonNull(engine, "engine must not be null");
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        return engine.get(key)
                .map(bytes -> ReplicaValues.fromStored(StoredValueCodec.decode(bytes)));
    }

    @Override
    public void put(String key, VersionedValue value) {
        StoredValue stored = ReplicaValues.toStored(value);
        engine.put(key, StoredValueCodec.encode(stored));
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        put(key, value);
    }
}
