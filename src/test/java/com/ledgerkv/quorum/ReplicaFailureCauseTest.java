package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.failure.FailureCause;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Whether a reported failure cause reflects what actually went wrong.
 *
 * <p>Every replica failure used to be filed as {@code UNAVAILABLE_NODE} no matter what the call
 * threw, which made {@code getFailureCauses()} look informative while telling you nothing. The
 * distinction that matters operationally: a replica that refused the call is down, whereas a call
 * that was cancelled or timed out may say more about the coordinator than about the replica.
 */
class ReplicaFailureCauseTest {

    /** A replica whose every call throws whatever it was given. */
    private static final class ThrowingReplicaClient implements ReplicaClient {
        private final String nodeId;
        private final RuntimeException failure;

        ThrowingReplicaClient(String nodeId, RuntimeException failure) {
            this.nodeId = nodeId;
            this.failure = failure;
        }

        @Override public String nodeId() {
            return nodeId;
        }

        @Override public List<VersionedValue> get(String key) {
            throw failure;
        }

        @Override public void put(String key, VersionedValue value) {
            throw failure;
        }

        @Override public void deliverHint(String key, VersionedValue value) {
            throw failure;
        }
    }

    private static FailureCause causeReportedFor(RuntimeException thrown) {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);
        String brokenId = membership.getNodes().get(1).getId();
        clients.put(brokenId, new ThrowingReplicaClient(brokenId, thrown));

        LeaderlessKVCluster cluster =
                LeaderlessKVCluster.create(membership, new QuorumConfig(3, 3, 2), clients);
        QuorumResponse response = cluster.write(0, "k", "v");

        assertFalse(response.isSuccessful(), "W=3 cannot be met with one replica throwing");
        return response.getFailureContext().getFailureCauses().get(brokenId);
    }

    @Test
    void aReplicaThatRefusedTheCallIsReportedUnavailable() {
        assertEquals(FailureCause.UNAVAILABLE_NODE,
                causeReportedFor(new IllegalStateException("replica unavailable")));
        assertEquals(FailureCause.UNAVAILABLE_NODE,
                causeReportedFor(Status.UNAVAILABLE.asRuntimeException()));
    }

    @Test
    void aCancelledOrTimedOutCallIsReportedAsADroppedMessage() {
        assertEquals(FailureCause.DROPPED_MESSAGE,
                causeReportedFor(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertEquals(FailureCause.DROPPED_MESSAGE,
                causeReportedFor(Status.CANCELLED.asRuntimeException()),
                "a cancelled call says more about this coordinator than about the replica");
    }

    @Test
    void anUnrecognizedServerErrorIsNotDisguisedAsAnUnreachableNode() {
        assertEquals(FailureCause.UNKNOWN,
                causeReportedFor(Status.INTERNAL.asRuntimeException()));
        assertEquals(FailureCause.UNKNOWN, causeReportedFor(new RuntimeException("boom")));
    }
}
