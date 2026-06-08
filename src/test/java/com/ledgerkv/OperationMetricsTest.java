package com.ledgerkv;

import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.metrics.OperationMetrics;
import com.ledgerkv.metrics.OperationMetricsCollector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OperationMetricsTest {

    @Test
    void collectorRecordsReadsWritesFailuresStaleReadsConflictsAndLatencySamples() {
        OperationMetricsCollector collector = new OperationMetricsCollector();
        VersionedValue latest = new VersionedValue(
            "score=0.86",
            2,
            VersionMetadata.initial("node-a").increment("node-b")
        );
        VersionedValue stale = new VersionedValue(
            "score=0.81",
            1,
            VersionMetadata.initial("node-a")
        );
        VersionedValue conflictA = new VersionedValue(
            "score=0.82",
            1,
            VersionMetadata.initial("node-a")
        );
        VersionedValue conflictB = new VersionedValue(
            "score=0.90",
            1,
            VersionMetadata.initial("node-b")
        );

        collector.recordWrite(new QuorumResponse(true, latest, List.of(), 2, 2, 7));
        collector.recordRead(new QuorumResponse(true, latest, List.of(stale, latest), 2, 2, 11));
        collector.recordRead(new QuorumResponse(true, conflictA, List.of(conflictA, conflictB), 2, 2, 13));
        collector.recordWrite(new QuorumResponse(false, null, List.of(), 1, 2, 17));

        OperationMetrics metrics = collector.snapshot();

        assertEquals(4, metrics.getOperationCount());
        assertEquals(2, metrics.getReadCount());
        assertEquals(2, metrics.getWriteCount());
        assertEquals(3, metrics.getSuccessCount());
        assertEquals(1, metrics.getFailureCount());
        assertEquals(1, metrics.getQuorumFailureCount());
        assertEquals(1, metrics.getStaleReadCount());
        assertEquals(1, metrics.getConflictCount());
        assertEquals(List.of(7L, 11L, 13L, 17L), metrics.getLatencySamplesMs());
    }

    @Test
    void snapshotIsImmutableAndIndependentFromFutureCollectorUpdates() {
        OperationMetricsCollector collector = new OperationMetricsCollector();
        collector.recordRead(new QuorumResponse(true, null, List.of(), 3, 3, 5));

        OperationMetrics firstSnapshot = collector.snapshot();
        assertThrows(UnsupportedOperationException.class,
            () -> firstSnapshot.getLatencySamplesMs().add(99L));

        collector.recordWrite(new QuorumResponse(true, new VersionedValue("value", 1), List.of(), 3, 3, 8));
        OperationMetrics secondSnapshot = collector.snapshot();

        assertEquals(1, firstSnapshot.getOperationCount());
        assertEquals(List.of(5L), firstSnapshot.getLatencySamplesMs());
        assertEquals(2, secondSnapshot.getOperationCount());
        assertEquals(List.of(5L, 8L), secondSnapshot.getLatencySamplesMs());
    }

    @Test
    void metricsRejectNegativeCountsAndLatencySamples() {
        assertThrows(IllegalArgumentException.class,
            () -> new OperationMetrics(-1, 0, 0, 0, 0, 0, 0, 0, List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new OperationMetrics(0, 0, 0, 0, 0, 0, 0, 0, List.of(-1L)));
    }
}
