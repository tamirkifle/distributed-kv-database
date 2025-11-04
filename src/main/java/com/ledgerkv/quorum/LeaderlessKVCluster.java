package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.failure.FailureCause;
import com.ledgerkv.failure.FailureContext;
import com.ledgerkv.consistency.VersionMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Leaderless quorum coordinator. Any node can coordinate a read or write against the key's replica
 * set; the N/R/W math, read-repair, hinted-handoff, and conflict resolution all run over the
 * transport-agnostic {@link ReplicaClient} seam — the replicas may be same-JVM {@code LsmEngine}s
 * ({@link LocalReplicaClient}) or remote gRPC nodes ({@link GrpcReplicaClient}).
 *
 * <p>The coordinator owns versioning: each write assigns one {@link VersionMetadata} vector clock
 * (incremented for the coordinator) plus a derived {@code long} version, and pushes the identical
 * {@link VersionedValue} to every replica verbatim.
 */
public final class LeaderlessKVCluster {
    private final ClusterMembership membership;
    private final QuorumConfig config;
    private final Map<String, ReplicaClient> clientsByNodeId;
    private final Map<String, VersionMetadata> versionMetadataByKey;
    private final List<HintedHandoff> pendingHints;

    LeaderlessKVCluster(ClusterMembership membership, QuorumConfig config,
                        Map<String, ReplicaClient> clientsByNodeId) {
        this.membership = Objects.requireNonNull(membership, "membership must not be null");
        this.config = Objects.requireNonNull(config, "quorum config must not be null");
        this.clientsByNodeId = new LinkedHashMap<>();
        this.versionMetadataByKey = new LinkedHashMap<>();
        this.pendingHints = new ArrayList<>();
        for (ClusterNode node : membership.getNodes()) {
            ReplicaClient client = clientsByNodeId.get(node.getId());
            if (client == null) {
                throw new IllegalArgumentException("replica client missing for node " + node.getId());
            }
            this.clientsByNodeId.put(node.getId(), client);
        }
    }

    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, ReplicaClient> clientsByNodeId) {
        return new LeaderlessKVCluster(membership, config, clientsByNodeId);
    }

    public ClusterMembership getMembership() {
        return membership;
    }

    public List<ClusterNode> selectReplicas(String key) {
        return membership.getPreferenceList(key, membership.getReplicationFactor());
    }

    public Optional<VersionedValue> getReplicaValue(String nodeId, String key) {
        ReplicaClient client = clientsByNodeId.get(nodeId);
        if (client == null) {
            return Optional.empty();
        }
        return client.get(key);
    }

    public List<HintedHandoff> getPendingHints() {
        return List.copyOf(pendingHints);
    }

    public QuorumResponse write(int coordinatorIndex, String key, String value) {
        validateCoordinator(coordinatorIndex);
        Objects.requireNonNull(value, "value must not be null");

        long startTime = System.currentTimeMillis();
        int acknowledgments = 0;
        FailureContext.Builder failureContext = FailureContext.builder();
        VersionMetadata nextMetadata = nextVersionMetadata(key, coordinatorIndex);
        VersionedValue versionedValue = new VersionedValue(value, versionFor(nextMetadata), nextMetadata);
        List<String> failedReplicaIds = new ArrayList<>();

        for (ClusterNode replica : selectReplicas(key)) {
            try {
                clientsByNodeId.get(replica.getId()).put(key, versionedValue);
                acknowledgments++;
                failureContext.responded(replica.getId());
            } catch (RuntimeException ignored) {
                // Failed replicas do not count toward the write quorum.
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
                failedReplicaIds.add(replica.getId());
            }
        }

        boolean successful = acknowledgments >= config.getW();
        if (successful) {
            versionMetadataByKey.put(key, nextMetadata);
            recordHints(coordinatorIndex, key, value, nextMetadata, failedReplicaIds);
        }
        VersionedValue responseValue = successful ? versionedValue : null;
        return new QuorumResponse(successful, responseValue, List.of(), acknowledgments,
            config.getW(), System.currentTimeMillis() - startTime, failureContext.build());
    }

    public HintedHandoffReplayResult replayPendingHints() {
        int attemptedCount = pendingHints.size();
        int appliedCount = 0;
        List<HintedHandoff> remainingHints = new ArrayList<>();

        for (HintedHandoff hint : pendingHints) {
            ReplicaClient client = clientsByNodeId.get(hint.getTargetNodeId());
            if (client == null) {
                remainingHints.add(hint);
                continue;
            }

            try {
                VersionMetadata hintMetadata = hint.getVersionMetadata();
                client.deliverHint(hint.getKey(),
                    new VersionedValue(hint.getValue(), versionFor(hintMetadata), hintMetadata));
                appliedCount++;
            } catch (RuntimeException ignored) {
                remainingHints.add(hint);
            }
        }

        pendingHints.clear();
        pendingHints.addAll(remainingHints);
        return new HintedHandoffReplayResult(attemptedCount, appliedCount, pendingHints.size());
    }

    public QuorumResponse read(int coordinatorIndex, String key) {
        validateCoordinator(coordinatorIndex);

        long startTime = System.currentTimeMillis();
        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();

        for (ClusterNode replica : selectReplicas(key)) {
            try {
                values.add(clientsByNodeId.get(replica.getId()).get(key).orElse(null));
                failureContext.responded(replica.getId());
            } catch (RuntimeException ignored) {
                // Failed replicas do not count toward the read quorum.
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
                continue;
            }

            if (values.size() >= config.getR()) {
                break;
            }
        }

        boolean successful = values.size() >= config.getR();
        VersionedValue latest = successful
            ? values.stream()
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(VersionedValue::getVersion))
                .orElse(null)
            : null;

        return new QuorumResponse(successful, latest, values, values.size(),
            config.getR(), System.currentTimeMillis() - startTime, failureContext.build());
    }

    public QuorumResponse repair(int coordinatorIndex, String key) {
        validateCoordinator(coordinatorIndex);

        long startTime = System.currentTimeMillis();
        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();

        for (ClusterNode replica : selectReplicas(key)) {
            try {
                VersionedValue value = clientsByNodeId.get(replica.getId()).get(key).orElse(null);
                values.add(value);
                failureContext.responded(replica.getId());
            } catch (RuntimeException ignored) {
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
            }
        }

        VersionedValue latest = values.stream()
            .filter(Objects::nonNull)
            .max(Comparator.comparingLong(VersionedValue::getVersion))
            .orElse(null);

        if (latest != null) {
            for (ClusterNode replica : selectReplicas(key)) {
                try {
                    Optional<VersionedValue> current = clientsByNodeId.get(replica.getId()).get(key);
                    if (current.map(value -> sameStoredValue(value, latest)).orElse(false)) {
                        continue;
                    }
                    clientsByNodeId.get(replica.getId()).put(key, latest);
                } catch (RuntimeException ignored) {
                    failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
                }
            }
            versionMetadataByKey.put(key, latest.getVersionMetadata());
        }

        boolean successful = !values.isEmpty();
        return new QuorumResponse(successful, latest, values, values.size(),
            config.getN(), System.currentTimeMillis() - startTime, failureContext.build());
    }

    private void validateCoordinator(int coordinatorIndex) {
        if (coordinatorIndex < 0 || coordinatorIndex >= membership.getNodes().size()) {
            throw new IllegalArgumentException("coordinator index must identify a cluster node");
        }
    }

    private VersionMetadata nextVersionMetadata(String key, int coordinatorIndex) {
        String coordinatorNodeId = membership.getNodes().get(coordinatorIndex).getId();
        VersionMetadata current = versionMetadataByKey.get(key);
        if (current == null) {
            return VersionMetadata.initial(coordinatorNodeId);
        }
        return current.increment(coordinatorNodeId);
    }

    /**
     * Derives the {@code long} version from the vector clock (sum of per-node counters): monotonic
     * along a causal chain, equal for concurrent writes. Reads use it only to pick a proposed latest;
     * conflict detection keys off the clock itself.
     */
    private static long versionFor(VersionMetadata metadata) {
        long sum = 0;
        for (long counter : metadata.getVectorClock().values()) {
            sum += counter;
        }
        return sum;
    }

    private void recordHints(int coordinatorIndex, String key, String value,
                             VersionMetadata versionMetadata, List<String> failedReplicaIds) {
        if (failedReplicaIds.isEmpty()) {
            return;
        }

        String coordinatorNodeId = membership.getNodes().get(coordinatorIndex).getId();
        for (String failedReplicaId : failedReplicaIds) {
            pendingHints.add(new HintedHandoff(
                coordinatorNodeId,
                failedReplicaId,
                key,
                value,
                versionMetadata
            ));
        }
    }

    private static boolean sameStoredValue(VersionedValue left, VersionedValue right) {
        return left.getValue().equals(right.getValue())
            && left.getVersionMetadata().equals(right.getVersionMetadata());
    }
}
