package com.ledgerkv.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.transport.StoredValue;

/**
 * Bridges the cluster's {@link VersionedValue} (String value) and the transport/storage
 * {@link StoredValue} (byte value + tombstone). The cluster has no delete path in 2c, so values
 * always cross as non-tombstones; the round-trip is UTF-8 lossless.
 */
final class ReplicaValues {

    private ReplicaValues() {
    }

    static StoredValue toStored(VersionedValue v) {
        return new StoredValue(
                v.getValue().getBytes(UTF_8), v.getVersion(), false, v.getVersionMetadata());
    }

    static VersionedValue fromStored(StoredValue s) {
        return new VersionedValue(new String(s.value(), UTF_8), s.version(), s.metadata());
    }
}
