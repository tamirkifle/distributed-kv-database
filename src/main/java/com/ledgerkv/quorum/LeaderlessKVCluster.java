package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.failure.FailureCause;
import com.ledgerkv.failure.FailureContext;
import com.ledgerkv.consistency.VersionMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Leaderless quorum coordinator. Any node can coordinate a read or write against the key's replica
 * set; the N/R/W math, read-repair, hinted-handoff, and conflict resolution all run over the
 * transport-agnostic {@link ReplicaClient} seam — the replicas may be same-JVM {@code LsmEngine}s
 * ({@link LocalReplicaClient}) or remote gRPC nodes ({@link GrpcReplicaClient}).
 *
 * <p>The coordinator owns versioning: each write assigns one {@link VersionMetadata} vector clock
 * (incremented for the coordinator) plus a derived {@code long} version, and pushes the identical
 * {@link VersionedValue} to every replica verbatim.
 *
 * <p><b>Fan-out scheduling (Phase 5a):</b> replicas are contacted <em>concurrently</em> on an
 * injected {@link ExecutorService}, and each operation is bounded by a per-request
 * {@link Duration} deadline measured on the monotonic {@link System#nanoTime()} clock. A write
 * returns as soon as {@code W} acks arrive within the deadline; a read as soon as {@code R}
 * responses arrive. A replica that fails <em>or</em> misses the deadline does not count toward
 * quorum and (for writes) becomes a hinted handoff — exactly as in the prior sequential design, so
 * one slow or dead replica no longer head-of-line-blocks its ring-neighbours. Quorum math,
 * versioning, hinted handoff, read repair, and conflict resolution are behaviorally unchanged; only
 * the scheduling differs, and out-of-order ack arrival cannot change which version wins.
 */
public final class LeaderlessKVCluster implements AutoCloseable {

    /** Default per-request deadline when callers use the simple {@code create(...)} factory. */
    private static final Duration DEFAULT_REQUEST_DEADLINE = Duration.ofSeconds(5);

    private final ClusterMembership membership;
    private final QuorumConfig config;
    private final Map<String, ReplicaClient> clientsByNodeId;
    private final Map<String, VersionMetadata> versionMetadataByKey;
    private final List<HintedHandoff> pendingHints;
    private final ExecutorService executor;
    private final Duration requestDeadline;
    private final boolean ownsExecutor;

    LeaderlessKVCluster(ClusterMembership membership, QuorumConfig config,
                        Map<String, ReplicaClient> clientsByNodeId,
                        ExecutorService executor, Duration requestDeadline, boolean ownsExecutor) {
        this.membership = Objects.requireNonNull(membership, "membership must not be null");
        this.config = Objects.requireNonNull(config, "quorum config must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.requestDeadline = Objects.requireNonNull(requestDeadline, "request deadline must not be null");
        if (requestDeadline.isNegative() || requestDeadline.isZero()) {
            throw new IllegalArgumentException("request deadline must be positive");
        }
        this.ownsExecutor = ownsExecutor;
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

    /**
     * Backward-compatible factory: provides a default cached thread pool (owned by the cluster, shut
     * down by {@link #close()}) and a default per-request deadline.
     */
    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, ReplicaClient> clientsByNodeId) {
        return new LeaderlessKVCluster(membership, config, clientsByNodeId,
            Executors.newCachedThreadPool(), DEFAULT_REQUEST_DEADLINE, true);
    }

    /**
     * Factory accepting a caller-supplied {@link ExecutorService} and per-request deadline. The
     * executor is <em>not</em> owned by the cluster — {@link #close()} leaves it running (the caller
     * shuts it down).
     */
    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, ReplicaClient> clientsByNodeId,
                                             ExecutorService executor,
                                             Duration requestDeadline) {
        return new LeaderlessKVCluster(membership, config, clientsByNodeId,
            executor, requestDeadline, false);
    }

    public ClusterMembership getMembership() {
        return membership;
    }

    public Duration requestDeadline() {
        return requestDeadline;
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

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + requestDeadline.toNanos();
        VersionMetadata nextMetadata = nextVersionMetadata(key, coordinatorIndex);
        VersionedValue versionedValue = new VersionedValue(value, versionFor(nextMetadata), nextMetadata);

        List<ClusterNode> replicas = selectReplicas(key);
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> futures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            String nodeId = replica.getId();
            futures.add(completion.submit(() -> {
                try {
                    clientsByNodeId.get(nodeId).put(key, versionedValue);
                    return ReplicaResult.ok(nodeId, null);
                } catch (RuntimeException e) {
                    return ReplicaResult.failed(nodeId);
                }
            }));
        }

        int acknowledgments = 0;
        FailureContext.Builder failureContext = FailureContext.builder();
        List<String> acked = new ArrayList<>();
        int completed = 0;
        try {
            // Collect replica completions until either every replica reports (all-healthy: each
            // reachable replica still durably receives the write) or W acks are in hand AND the
            // deadline has elapsed (a slow/dead replica that has not acked by the deadline does not
            // hold up the write — it becomes a hinted handoff below).
            while (completed < replicas.size()) {
                if (acknowledgments >= config.getW() && System.nanoTime() >= deadlineNanos) {
                    break;
                }
                ReplicaResult result = pollWithin(completion, deadlineNanos);
                if (result == null) {
                    break; // deadline reached
                }
                completed++;
                if (result.ok) {
                    acknowledgments++;
                    acked.add(result.nodeId);
                    failureContext.responded(result.nodeId);
                }
            }
        } finally {
            cancelAll(futures);
        }

        // Any replica that did not positively ack within budget (failed OR too slow) becomes a hint.
        List<String> failedReplicaIds = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            if (!acked.contains(replica.getId())) {
                failedReplicaIds.add(replica.getId());
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
            }
        }

        boolean successful = acknowledgments >= config.getW();
        if (successful) {
            versionMetadataByKey.put(key, nextMetadata);
            recordHints(coordinatorIndex, key, value, nextMetadata, failedReplicaIds);
        }
        VersionedValue responseValue = successful ? versionedValue : null;
        return new QuorumResponse(successful, responseValue, List.of(), acknowledgments,
            config.getW(), elapsedMs(startNanos), failureContext.build());
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

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + requestDeadline.toNanos();

        List<ClusterNode> replicas = selectReplicas(key);
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> futures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            String nodeId = replica.getId();
            futures.add(completion.submit(() -> {
                try {
                    return ReplicaResult.ok(nodeId, clientsByNodeId.get(nodeId).get(key).orElse(null));
                } catch (RuntimeException e) {
                    return ReplicaResult.failed(nodeId);
                }
            }));
        }

        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();
        List<String> responded = new ArrayList<>();
        int completedCount = 0;
        try {
            // Block only until R reads (or the deadline); a slow replica does not hold up the read.
            while (completedCount < replicas.size() && values.size() < config.getR()) {
                ReplicaResult result = pollWithin(completion, deadlineNanos);
                if (result == null) {
                    break;
                }
                completedCount++;
                if (result.ok) {
                    values.add(result.value);
                    responded.add(result.nodeId);
                    failureContext.responded(result.nodeId);
                }
            }
        } finally {
            cancelAll(futures);
        }

        // Replicas we never heard a successful response from (threw, or too slow) are failures.
        for (ClusterNode replica : replicas) {
            if (!responded.contains(replica.getId())) {
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
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
            config.getR(), elapsedMs(startNanos), failureContext.build());
    }

    public QuorumResponse repair(int coordinatorIndex, String key) {
        validateCoordinator(coordinatorIndex);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + requestDeadline.toNanos();

        List<ClusterNode> replicas = selectReplicas(key);
        // Phase 1: read from everyone we can reach within the deadline (repair wants the freshest
        // across all reachable replicas, not just the first R).
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> readFutures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            String nodeId = replica.getId();
            readFutures.add(completion.submit(() -> {
                try {
                    return ReplicaResult.ok(nodeId, clientsByNodeId.get(nodeId).get(key).orElse(null));
                } catch (RuntimeException e) {
                    return ReplicaResult.failed(nodeId);
                }
            }));
        }

        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();
        Map<String, VersionedValue> currentByNode = new LinkedHashMap<>();
        int completedCount = 0;
        try {
            while (completedCount < replicas.size()) {
                ReplicaResult result = pollWithin(completion, deadlineNanos);
                if (result == null) {
                    break;
                }
                completedCount++;
                if (result.ok) {
                    values.add(result.value);
                    currentByNode.put(result.nodeId, result.value);
                    failureContext.responded(result.nodeId);
                } else {
                    failureContext.failed(result.nodeId, FailureCause.UNAVAILABLE_NODE);
                }
            }
        } finally {
            cancelAll(readFutures);
        }

        VersionedValue latest = values.stream()
            .filter(Objects::nonNull)
            .max(Comparator.comparingLong(VersionedValue::getVersion))
            .orElse(null);

        if (latest != null) {
            // Phase 2: push the freshest value to replicas whose current value differs, concurrently
            // within the same deadline budget. Repair writes do not gate the response (matches prior
            // behavior); failures only mark the failure context.
            long repairDeadlineNanos = System.nanoTime() + requestDeadline.toNanos();
            List<Future<ReplicaResult>> repairFutures = new ArrayList<>();
            for (ClusterNode replica : replicas) {
                String nodeId = replica.getId();
                VersionedValue current = currentByNode.get(nodeId);
                if (currentByNode.containsKey(nodeId) && current != null && sameStoredValue(current, latest)) {
                    continue; // already up to date
                }
                VersionedValue toWrite = latest;
                repairFutures.add(executor.submit(() -> {
                    clientsByNodeId.get(nodeId).put(key, toWrite);
                    return ReplicaResult.ok(nodeId, null);
                }));
            }
            awaitRepairWrites(repairFutures, repairDeadlineNanos);
            versionMetadataByKey.put(key, latest.getVersionMetadata());
        }

        boolean successful = !values.isEmpty();
        return new QuorumResponse(successful, latest, values, values.size(),
            config.getN(), elapsedMs(startNanos), failureContext.build());
    }

    /**
     * Polls the completion service for the next result, blocking up to the remaining deadline budget;
     * returns null if the deadline passes with no completion. Replica tasks catch their own failures
     * and return a {@link ReplicaResult#failed(String)}, so {@code get()} never throws here.
     */
    private static ReplicaResult pollWithin(ExecutorCompletionService<ReplicaResult> completion,
                                            long deadlineNanos) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            return null;
        }
        try {
            Future<ReplicaResult> done = completion.poll(remaining, TimeUnit.NANOSECONDS);
            if (done == null) {
                return null; // deadline elapsed with no completion
            }
            return done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (java.util.concurrent.ExecutionException e) {
            return ReplicaResult.failed(null); // defensive: a task escaped its own catch
        }
    }

    private void awaitRepairWrites(List<Future<ReplicaResult>> repairFutures, long deadlineNanos) {
        try {
            for (Future<ReplicaResult> future : repairFutures) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    future.get(remaining, TimeUnit.NANOSECONDS);
                } catch (java.util.concurrent.ExecutionException
                         | java.util.concurrent.TimeoutException ex) {
                    // a repair write that failed or timed out is best-effort; the next read repairs it
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cancelAll(repairFutures);
        }
    }

    private static void cancelAll(List<Future<ReplicaResult>> futures) {
        for (Future<ReplicaResult> future : futures) {
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
    }

    /**
     * Elapsed milliseconds since {@code startNanos}, measured with the monotonic {@link System#nanoTime()}
     * clock so a wall-clock correction (NTP step, laptop sleep/resume) mid-operation can never produce
     * a negative duration — the bug that previously poisoned the {@code /metrics} endpoint.
     */
    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /** Shuts down the executor only if this cluster owns it (a caller-supplied pool is theirs). */
    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdownNow();
        }
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

    /** Carrier for a single replica's fan-out outcome. */
    private static final class ReplicaResult {
        final String nodeId;
        final boolean ok;
        final VersionedValue value;

        private ReplicaResult(String nodeId, boolean ok, VersionedValue value) {
            this.nodeId = nodeId;
            this.ok = ok;
            this.value = value;
        }

        static ReplicaResult ok(String nodeId, VersionedValue value) {
            return new ReplicaResult(nodeId, true, value);
        }

        static ReplicaResult failed(String nodeId) {
            return new ReplicaResult(nodeId, false, null);
        }
    }
}
