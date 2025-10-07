package com.ledgerkv;

import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VersionMetadataTest {

    @Test
    void sequentialMetadataIsOrderedByVectorClock() {
        VersionMetadata first = VersionMetadata.initial("eval-cluster-node-0");
        VersionMetadata second = first.increment("eval-cluster-node-1");

        assertTrue(first.happensBefore(second));
        assertFalse(second.happensBefore(first));
        assertFalse(first.isConcurrentWith(second));
    }

    @Test
    void independentMetadataIsDetectedAsConcurrent() {
        VersionMetadata node0Write = VersionMetadata.initial("eval-cluster-node-0");
        VersionMetadata node1Write = VersionMetadata.initial("eval-cluster-node-1");

        assertTrue(node0Write.isConcurrentWith(node1Write));
        assertTrue(node1Write.isConcurrentWith(node0Write));
    }

    @Test
    void leaderlessWritesAttachOrderedVersionMetadata() {
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            "eval-cluster",
            new QuorumConfig(3, 2, 2)
        );

        QuorumResponse firstWrite = cluster.write(0, "trace:run-009", "score=0.81");
        QuorumResponse secondWrite = cluster.write(1, "trace:run-009", "score=0.86");

        assertTrue(firstWrite.isSuccessful());
        assertTrue(secondWrite.isSuccessful());
        assertTrue(firstWrite.getValue().getVersionMetadata()
            .happensBefore(secondWrite.getValue().getVersionMetadata()));
        assertEquals(1, firstWrite.getValue().getVersionMetadata().getCounter("eval-cluster-node-0"));
        assertEquals(1, secondWrite.getValue().getVersionMetadata().getCounter("eval-cluster-node-1"));
    }
}
