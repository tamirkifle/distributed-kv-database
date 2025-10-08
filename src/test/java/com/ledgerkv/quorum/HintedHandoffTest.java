package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedKVStore;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class HintedHandoffTest {

    @Test
    void successfulPartialWriteRecordsHintForUnavailableReplica() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        String unavailableNodeId = membership.getNodes().get(1).getId();
        stores.put(unavailableNodeId, new ToggleableStore(false));
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 2, 2),
            stores
        );

        QuorumResponse response = cluster.write(0, "trace:run-009", "score=0.91");

        assertTrue(response.isSuccessful());
        List<HintedHandoff> pendingHints = cluster.getPendingHints();
        assertEquals(1, pendingHints.size());
        HintedHandoff hint = pendingHints.get(0);
        assertEquals(membership.getNodes().get(0).getId(), hint.getCoordinatorNodeId());
        assertEquals(unavailableNodeId, hint.getTargetNodeId());
        assertEquals("trace:run-009", hint.getKey());
        assertEquals("score=0.91", hint.getValue());
        assertEquals(response.getValue().getVersionMetadata(), hint.getVersionMetadata());
    }

    @Test
    void replayKeepsFailedHintsPendingAndClearsAppliedHints() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        String unavailableNodeId = membership.getNodes().get(1).getId();
        ToggleableStore recoveringStore = new ToggleableStore(false);
        stores.put(unavailableNodeId, recoveringStore);
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 2, 2),
            stores
        );
        QuorumResponse writeResponse = cluster.write(0, "trace:run-010", "score=0.93");

        HintedHandoffReplayResult failedReplay = cluster.replayPendingHints();
        recoveringStore.setAvailable(true);
        HintedHandoffReplayResult successfulReplay = cluster.replayPendingHints();

        assertEquals(1, failedReplay.getAttemptedCount());
        assertEquals(0, failedReplay.getAppliedCount());
        assertEquals(1, failedReplay.getRemainingCount());
        assertEquals(1, successfulReplay.getAttemptedCount());
        assertEquals(1, successfulReplay.getAppliedCount());
        assertEquals(0, successfulReplay.getRemainingCount());
        assertTrue(cluster.getPendingHints().isEmpty());

        VersionedValue recoveredValue = cluster.getReplicaValue(unavailableNodeId, "trace:run-010")
            .orElseThrow(() -> new AssertionError("expected hinted value on recovered replica"));
        assertEquals("score=0.93", recoveredValue.getValue());
        assertEquals(writeResponse.getValue().getVersionMetadata(), recoveredValue.getVersionMetadata());
    }

    private static Map<String, VersionedKVStore> storesFor(ClusterMembership membership) {
        Map<String, VersionedKVStore> stores = new LinkedHashMap<>();
        for (ClusterNode node : membership.getNodes()) {
            stores.put(node.getId(), new VersionedKVStore());
        }
        return stores;
    }

    private static final class ToggleableStore extends VersionedKVStore {
        private boolean available;

        private ToggleableStore(boolean available) {
            this.available = available;
        }

        private void setAvailable(boolean available) {
            this.available = available;
        }

        @Override
        public long set(String key, String value) {
            if (!available) {
                throw new IllegalStateException("replica unavailable");
            }
            return super.set(key, value);
        }

        @Override
        public long set(String key, String value, VersionMetadata versionMetadata) {
            if (!available) {
                throw new IllegalStateException("replica unavailable");
            }
            return super.set(key, value, versionMetadata);
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            if (!available) {
                throw new IllegalStateException("replica unavailable");
            }
            return super.get(key);
        }
    }
}
