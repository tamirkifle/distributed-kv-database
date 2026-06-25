package com.ledgerkv.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.transport.StoredValue;

/**
 * Bridges the cluster's {@link VersionedValue} (String value) and the transport/storage
 * {@link StoredValue} (byte value + tombstone). The round-trip is UTF-8 lossless and carries the
 * tombstone marker, so a replicated delete survives storage and the wire like any other version.
 */
final class ReplicaValues {

    private ReplicaValues() {
    }

    static StoredValue toStored(VersionedValue v) {
        return new StoredValue(v.getValue().getBytes(UTF_8), v.getVersion(), v.isDeleted(),
                v.getVersionMetadata());
    }

    static VersionedValue fromStored(StoredValue s) {
        return new VersionedValue(
                new String(s.value(), UTF_8), s.version(), s.metadata(), s.tombstone());
    }
}
