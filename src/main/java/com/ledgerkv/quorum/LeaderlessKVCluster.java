package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedKVStore;
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

public final class LeaderlessKVCluster {
    private final ClusterMembership membership;
    private final QuorumConfig config;
    private final Map<String, VersionedKVStore> storesByNodeId;
    private final Map<String, VersionMetadata> versionMetadataByKey;
    private final List<HintedHandoff> pendingHints;

    private LeaderlessKVCluster(ClusterMembership membership, QuorumConfig config) {
        this.membership = membership;
        this.config = config;
        this.storesByNodeId = new LinkedHashMap<>();
        this.versionMetadataByKey = new LinkedHashMap<>();
        this.pendingHints = new ArrayList<>();
        for (ClusterNode node : membership.getNodes()) {
            storesByNodeId.put(node.getId(), new VersionedKVStore());
        }
    }

    LeaderlessKVCluster(ClusterMembership membership, QuorumConfig config,
                        Map<String, VersionedKVStore> storesByNodeId) {
        this.membership = Objects.requireNonNull(membership, "membership must not be null");
        this.config = Objects.requireNonNull(config, "quorum config must not be null");
        this.storesByNodeId = new LinkedHashMap<>();
        this.versionMetadataByKey = new LinkedHashMap<>();
        this.pendingHints = new ArrayList<>();
        for (ClusterNode node : membership.getNodes()) {
            VersionedKVStore store = storesByNodeId.get(node.getId());
            if (store == null) {
                throw new IllegalArgumentException("store missing for node " + node.getId());
            }
            this.storesByNodeId.put(node.getId(), store);
        }
    }

    public static LeaderlessKVCluster create(String clusterId, QuorumConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("quorum config must not be null");
        }

        ClusterMembership membership = ClusterMembership.create(
            clusterId,
            config.getN(),
            config.getN()
        );
        return new LeaderlessKVCluster(membership, config);
    }

    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, VersionedKVStore> storesByNodeId) {
        return new LeaderlessKVCluster(membership, config, storesByNodeId);
    }

    public ClusterMembership getMembership() {
        return membership;
    }

    public List<ClusterNode> selectReplicas(String key) {
        return membership.selectReplicas(key);
    }

    public Optional<VersionedValue> getReplicaValue(String nodeId, String key) {
        VersionedKVStore store = storesByNodeId.get(nodeId);
        if (store == null) {
            return Optional.empty();
        }
        return store.get(key);
    }

    public List<HintedHandoff> getPendingHints() {
        return List.copyOf(pendingHints);
    }

    public QuorumResponse write(int coordinatorIndex, String key, String value) {
        validateCoordinator(coordinatorIndex);
        Objects.requireNonNull(value, "value must not be null");

        long startTime = System.currentTimeMillis();
        int acknowledgments = 0;
        long maxVersion = 0;
        FailureContext.Builder failureContext = FailureContext.builder();
        VersionMetadata nextMetadata = nextVersionMetadata(key, coordinatorIndex);
        List<String> failedReplicaIds = new ArrayList<>();

        for (ClusterNode replica : selectReplicas(key)) {
            try {
                long version = storesByNodeId.get(replica.getId()).set(key, value, nextMetadata);
                acknowledgments++;
                maxVersion = Math.max(maxVersion, version);
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
        VersionedValue responseValue = successful ? new VersionedValue(value, maxVersion, nextMetadata) : null;
        return new QuorumResponse(successful, responseValue, List.of(), acknowledgments,
            config.getW(), System.currentTimeMillis() - startTime, failureContext.build());
    }

    public HintedHandoffReplayResult replayPendingHints() {
        int attemptedCount = pendingHints.size();
        int appliedCount = 0;
        List<HintedHandoff> remainingHints = new ArrayList<>();

        for (HintedHandoff hint : pendingHints) {
            VersionedKVStore store = storesByNodeId.get(hint.getTargetNodeId());
            if (store == null) {
                remainingHints.add(hint);
                continue;
            }

            try {
                store.set(hint.getKey(), hint.getValue(), hint.getVersionMetadata());
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
                values.add(storesByNodeId.get(replica.getId()).get(key).orElse(null));
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
                VersionedValue value = storesByNodeId.get(replica.getId()).get(key).orElse(null);
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
                    Optional<VersionedValue> current = storesByNodeId.get(replica.getId()).get(key);
                    if (current.map(value -> sameStoredValue(value, latest)).orElse(false)) {
                        continue;
                    }
                    storesByNodeId.get(replica.getId()).set(key, latest.getValue(), latest.getVersionMetadata());
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
