package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedKVStore;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.failure.FailureCause;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FailureInjectionTest {

    @Test
    void failedWriteQuorumReportsRespondingAndFailedReplicaIds() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        String failedNodeId = membership.getNodes().get(1).getId();
        stores.put(failedNodeId, new UnavailableStore());
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 3, 2),
            stores
        );

        QuorumResponse response = cluster.write(0, "trace:run-007", "score=0.77");

        assertFalse(response.isSuccessful());
        assertEquals(2, response.getFailureContext().getRespondingNodeIds().size());
        assertTrue(response.getFailureContext().getFailedNodeIds().contains(failedNodeId));
        assertEquals(
            FailureCause.UNAVAILABLE_NODE,
            response.getFailureContext().getFailureCauses().get(failedNodeId)
        );
    }

    @Test
    void failedReadQuorumReportsRespondingAndFailedReplicaIds() {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        for (VersionedKVStore store : stores.values()) {
            store.set("trace:run-008", "score=0.89");
        }
        String failedNodeId = membership.getNodes().get(2).getId();
        stores.put(failedNodeId, new UnavailableStore());
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 2, 3),
            stores
        );

        QuorumResponse response = cluster.read(1, "trace:run-008");

        assertFalse(response.isSuccessful());
        assertEquals(2, response.getFailureContext().getRespondingNodeIds().size());
        assertTrue(response.getFailureContext().getFailedNodeIds().contains(failedNodeId));
        assertEquals(
            FailureCause.UNAVAILABLE_NODE,
            response.getFailureContext().getFailureCauses().get(failedNodeId)
        );
    }

    private static Map<String, VersionedKVStore> storesFor(ClusterMembership membership) {
        Map<String, VersionedKVStore> stores = new LinkedHashMap<>();
        for (ClusterNode node : membership.getNodes()) {
            stores.put(node.getId(), new VersionedKVStore());
        }
        return stores;
    }

    private static final class UnavailableStore extends VersionedKVStore {
        @Override
        public long set(String key, String value) {
            throw new IllegalStateException("replica unavailable");
        }

        @Override
        public long set(String key, String value, VersionMetadata versionMetadata) {
            throw new IllegalStateException("replica unavailable");
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            throw new IllegalStateException("replica unavailable");
        }
    }
}
