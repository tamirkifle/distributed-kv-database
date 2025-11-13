package com.ledgerkv.transport;

import com.google.protobuf.ByteString;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.transport.proto.VersionMetadataPb;
import com.ledgerkv.transport.proto.VersionedValuePb;

/**
 * Converts a {@link StoredValue} to and from the wire message {@link VersionedValuePb}. The
 * {@link VersionMetadata} vector clock maps onto {@link VersionMetadataPb}'s {@code vector_clock}
 * map, so the full causal clock travels on the wire (the fidelity 2b deferred to 2c).
 */
public final class VersionedValueProtos {

    private VersionedValueProtos() {
    }

    public static VersionedValuePb toProto(StoredValue s) {
        return VersionedValuePb.newBuilder()
                .setValue(ByteString.copyFrom(s.value()))
                .setVersion(s.version())
                .setTombstone(s.tombstone())
                .setMetadata(VersionMetadataPb.newBuilder()
                        .putAllVectorClock(s.metadata().getVectorClock())
                        .build())
                .build();
    }

    public static StoredValue fromProto(VersionedValuePb pb) {
        return new StoredValue(
                pb.getValue().toByteArray(),
                pb.getVersion(),
                pb.getTombstone(),
                VersionMetadata.of(pb.getMetadata().getVectorClockMap()));
    }
}
