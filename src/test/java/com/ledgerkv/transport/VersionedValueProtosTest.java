package com.ledgerkv.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.transport.proto.VersionedValuePb;
import org.junit.jupiter.api.Test;

/**
 * Round-trips a {@link StoredValue} through the wire message {@link VersionedValuePb} and back.
 * Proves the full {@link VersionMetadata} vector clock now survives the wire — the gap 2b deferred.
 */
class VersionedValueProtosTest {

    @Test
    void roundTripsMultiNodeVectorClock() {
        VersionMetadata clock =
                VersionMetadata.initial("nodeA").increment("nodeB").increment("nodeB");
        StoredValue original = new StoredValue("v".getBytes(UTF_8), 9, false, clock);

        VersionedValuePb pb = VersionedValueProtos.toProto(original);
        StoredValue decoded = VersionedValueProtos.fromProto(pb);

        assertArrayEquals("v".getBytes(UTF_8), decoded.value());
        assertEquals(9, decoded.version());
        assertFalse(decoded.tombstone());
        assertEquals(clock.getVectorClock(), decoded.metadata().getVectorClock());
    }

    @Test
    void roundTripsTombstone() {
        StoredValue original = new StoredValue(new byte[0], 3, true, VersionMetadata.legacy(3));

        StoredValue decoded = VersionedValueProtos.fromProto(VersionedValueProtos.toProto(original));

        assertArrayEquals(new byte[0], decoded.value());
        assertEquals(3, decoded.version());
        assertTrue(decoded.tombstone());
        assertEquals(VersionMetadata.legacy(3).getVectorClock(), decoded.metadata().getVectorClock());
    }

    @Test
    void roundTripsBinaryValue() {
        byte[] value = new byte[] {0, 1, 2, (byte) 0xFF, 0};
        StoredValue original = new StoredValue(value, 1, false, VersionMetadata.legacy(1));

        StoredValue decoded = VersionedValueProtos.fromProto(VersionedValueProtos.toProto(original));

        assertArrayEquals(value, decoded.value());
        assertEquals(1, decoded.version());
        assertFalse(decoded.tombstone());
        assertEquals(VersionMetadata.legacy(1).getVectorClock(), decoded.metadata().getVectorClock());
    }
}
