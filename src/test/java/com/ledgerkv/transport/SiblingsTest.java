package com.ledgerkv.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.consistency.VersionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The causal merge that a replica applies to every incoming write, and its storage format. */
class SiblingsTest {

    private static StoredValue value(String contents, VersionMetadata clock) {
        return new StoredValue(contents.getBytes(UTF_8), 1, false, clock);
    }

    @Test
    void anIncomingValueThatDominatesReplacesTheOlderOne() {
        VersionMetadata older = VersionMetadata.initial("a");
        VersionMetadata newer = older.increment("a");

        List<StoredValue> merged =
            Siblings.merge(List.of(value("v1", older)), value("v2", newer));

        assertEquals(1, merged.size());
        assertEquals("v2", new String(merged.get(0).value(), UTF_8));
    }

    @Test
    void aCausallyOlderArrivalIsDropped() {
        VersionMetadata older = VersionMetadata.initial("a");
        VersionMetadata newer = older.increment("a");

        // This is the delayed hinted handoff: v1 arriving after v2 has already been accepted.
        List<StoredValue> merged =
            Siblings.merge(List.of(value("v2", newer)), value("v1", older));

        assertEquals(1, merged.size());
        assertEquals("v2", new String(merged.get(0).value(), UTF_8));
    }

    @Test
    void concurrentValuesAreBothRetained() {
        List<StoredValue> merged = Siblings.merge(
            List.of(value("branch-a", VersionMetadata.initial("writer-a"))),
            value("branch-b", VersionMetadata.initial("writer-b")));

        assertEquals(2, merged.size());
    }

    @Test
    void anIdenticalRedeliveryIsIdempotent() {
        StoredValue stored = value("v", VersionMetadata.initial("a"));

        List<StoredValue> merged = Siblings.merge(List.of(stored), stored);

        assertEquals(1, merged.size());
    }

    @Test
    void sameClockButDifferentBytesStaysAConflict() {
        VersionMetadata clock = VersionMetadata.initial("a");

        List<StoredValue> merged =
            Siblings.merge(List.of(value("left", clock)), value("right", clock));

        assertEquals(2, merged.size(),
            "collapsing these would silently pick a winner between two real values");
    }

    @Test
    void aValueDominatingBothSiblingsCollapsesTheConflict() {
        VersionMetadata a = VersionMetadata.initial("writer-a");
        VersionMetadata b = VersionMetadata.initial("writer-b");
        List<StoredValue> conflicted = List.of(value("branch-a", a), value("branch-b", b));
        // A reader that resolved the conflict writes back a clock descending from both branches.
        VersionMetadata reconciled = a.merge(b).increment("writer-a");

        List<StoredValue> merged = Siblings.merge(conflicted, value("resolved", reconciled));

        assertEquals(1, merged.size());
        assertEquals("resolved", new String(merged.get(0).value(), UTF_8));
    }

    @Test
    void storageFormatRoundTripsASiblingSet() {
        List<StoredValue> siblings = List.of(
            value("branch-a", VersionMetadata.initial("writer-a")),
            new StoredValue(new byte[0], 7, true, VersionMetadata.initial("writer-b")));

        List<StoredValue> decoded = StoredValueCodec.decodeAll(StoredValueCodec.encodeAll(siblings));

        assertEquals(2, decoded.size());
        assertEquals("branch-a", new String(decoded.get(0).value(), UTF_8));
        assertTrue(decoded.get(1).tombstone());
        assertEquals(7, decoded.get(1).version());
    }

    @Test
    void readingAConflictedRecordAsAScalarFailsLoudly() {
        byte[] encoded = StoredValueCodec.encodeAll(List.of(
            value("branch-a", VersionMetadata.initial("writer-a")),
            value("branch-b", VersionMetadata.initial("writer-b"))));

        assertThrows(IllegalStateException.class, () -> StoredValueCodec.decode(encoded));
    }

    @Test
    void aVersion1RecordIsRejectedRatherThanGuessedAt() {
        // v1 had no format prefix: it began with the 8-byte version field.
        byte[] legacy = new byte[] {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0};

        IllegalStateException rejected =
            assertThrows(IllegalStateException.class, () -> StoredValueCodec.decodeAll(legacy));

        assertTrue(rejected.getMessage().contains("format version"));
    }
}
