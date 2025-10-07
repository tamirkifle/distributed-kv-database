package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedKVStore;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LeaderlessKVClusterTest {

    @Test
    void writeFailsWhenSuccessfulAcknowledgmentsStayBelowW() {
        ClusterMembership membership = ClusterMembership.create("eval-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        stores.put(membership.getNodes().get(1).getId(), new UnavailableStore());
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 3, 2),
            stores
        );

        QuorumResponse response = cluster.write(0, "trace:run-005", "score=0.79");

        assertFalse(response.isSuccessful());
        assertNull(response.getValue());
        assertEquals(2, response.getRespondingNodes());
        assertEquals(3, response.getRequiredNodes());
    }

    @Test
    void readFailsWhenSuccessfulResponsesStayBelowR() {
        ClusterMembership membership = ClusterMembership.create("eval-cluster", 3, 3);
        Map<String, VersionedKVStore> stores = storesFor(membership);
        for (VersionedKVStore store : stores.values()) {
            store.set("trace:run-006", "score=0.84");
        }
        stores.put(membership.getNodes().get(2).getId(), new UnavailableStore());
        LeaderlessKVCluster cluster = new LeaderlessKVCluster(
            membership,
            new QuorumConfig(3, 2, 3),
            stores
        );

        QuorumResponse response = cluster.read(1, "trace:run-006");

        assertFalse(response.isSuccessful());
        assertNull(response.getValue());
        assertEquals(2, response.getRespondingNodes());
        assertEquals(3, response.getRequiredNodes());
        assertEquals(2, response.getAllValues().size());
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
