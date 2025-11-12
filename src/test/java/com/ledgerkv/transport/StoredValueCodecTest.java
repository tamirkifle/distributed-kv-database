package com.ledgerkv.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link StoredValueCodec}. The codec is the single point where a versioned
 * value meets the LSM engine's opaque byte storage, so it must faithfully reproduce the value
 * bytes, version, tombstone flag, and the full {@link VersionMetadata} vector clock.
 */
class StoredValueCodecTest {

    @Test
    void roundTripsSimpleValue() {
        StoredValue original =
                new StoredValue("hello".getBytes(UTF_8), 7, false, VersionMetadata.legacy(7));

        StoredValue decoded = StoredValueCodec.decode(StoredValueCodec.encode(original));

        assertArrayEquals("hello".getBytes(UTF_8), decoded.value());
        assertEquals(7, decoded.version());
        assertEquals(false, decoded.tombstone());
        assertEquals(original.metadata().getVectorClock(), decoded.metadata().getVectorClock());
    }

    @Test
    void roundTripsTombstone() {
        StoredValue original =
                new StoredValue(new byte[0], 3, true, VersionMetadata.legacy(3));

        StoredValue decoded = StoredValueCodec.decode(StoredValueCodec.encode(original));

        assertArrayEquals(new byte[0], decoded.value());
        assertEquals(3, decoded.version());
        assertTrue(decoded.tombstone());
        assertEquals(original.metadata().getVectorClock(), decoded.metadata().getVectorClock());
    }

    @Test
    void roundTripsMultiNodeVectorClock() {
        VersionMetadata metadata =
                VersionMetadata.initial("nodeA").increment("nodeB").increment("nodeB");
        StoredValue original = new StoredValue("v".getBytes(UTF_8), 9, false, metadata);

        StoredValue decoded = StoredValueCodec.decode(StoredValueCodec.encode(original));

        assertArrayEquals("v".getBytes(UTF_8), decoded.value());
        assertEquals(9, decoded.version());
        assertEquals(metadata.getVectorClock(), decoded.metadata().getVectorClock());
    }

    @Test
    void roundTripsBinaryValue() {
        byte[] binary = new byte[] {0, 1, 2, (byte) 0xFF, 0};
        StoredValue original = new StoredValue(binary, 1, false, VersionMetadata.legacy(1));

        StoredValue decoded = StoredValueCodec.decode(StoredValueCodec.encode(original));

        assertArrayEquals(binary, decoded.value());
        assertEquals(1, decoded.version());
        assertEquals(original.metadata().getVectorClock(), decoded.metadata().getVectorClock());
    }
}
