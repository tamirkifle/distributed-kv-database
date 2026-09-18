package com.ledgerkv.quorum;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;
import com.ledgerkv.VersionedValue;
import com.ledgerkv.failure.FailureCause;
import com.ledgerkv.failure.FailureContext;
import com.ledgerkv.consistency.VersionConflictResolver;
import com.ledgerkv.consistency.VersionMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    /** Default hedging delay for the simple factories — a small fraction sized to ~p95. */
    private static final Duration DEFAULT_HEDGING_DELAY = Duration.ofMillis(50);
    /**
     * At most one backup request per operation: the canonical "Tail at Scale" tactic (Dean &amp;
     * Barroso) — one hedge tames the slowest 1–5% of requests for a small (~2%) load increase. More
     * than one backup is YAGNI for this workload.
     */
    private static final int MAX_HEDGES = 1;

    private final ClusterMembership membership;
    private final QuorumConfig config;
    private final Map<String, ReplicaClient> clientsByNodeId;
    /**
     * The highest clock this coordinator has issued per key. Not a source of truth — the durable
     * context read from the replicas is — but a monotonicity guard with two jobs the read alone
     * cannot do: it serializes concurrent writers on this coordinator (the update is one atomic
     * {@code compute}, so two writes to the same key can never be stamped with the same counter),
     * and it covers the window before a just-issued write is visible to a subsequent context read.
     *
     * <p>Merged with the observed context and only ever advanced, so it cannot rewind a clock the
     * way the old authoritative map did. Losing an entry is safe: the observed context takes over,
     * which is exactly what happens after a restart.
     */
    private final ConcurrentMap<String, VersionMetadata> issuedClocks = new ConcurrentHashMap<>();
    private final List<HintedHandoff> pendingHints;
    /** Guards the multi-step clear+refill in {@link #replayPendingHints()} against concurrent adds. */
    private final Object hintsLock = new Object();
    private final ExecutorService executor;
    private final Duration requestDeadline;
    private final Duration hedgingDelay;
    private final AtomicLong hedgedRequestCount = new AtomicLong();
    /** Writes released at W whose remaining replicas are still being accounted for. */
    private final java.util.concurrent.atomic.AtomicInteger pendingReplications =
            new java.util.concurrent.atomic.AtomicInteger();
    private final Object replicationIdle = new Object();
    private final boolean ownsExecutor;

    LeaderlessKVCluster(ClusterMembership membership, QuorumConfig config,
                        Map<String, ReplicaClient> clientsByNodeId,
                        ExecutorService executor, Duration requestDeadline,
                        Duration hedgingDelay, boolean ownsExecutor) {
        this.membership = Objects.requireNonNull(membership, "membership must not be null");
        this.config = Objects.requireNonNull(config, "quorum config must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.requestDeadline = Objects.requireNonNull(requestDeadline, "request deadline must not be null");
        if (requestDeadline.isNegative() || requestDeadline.isZero()) {
            throw new IllegalArgumentException("request deadline must be positive");
        }
        this.hedgingDelay = Objects.requireNonNull(hedgingDelay, "hedging delay must not be null");
        if (hedgingDelay.isNegative()) {
            throw new IllegalArgumentException("hedging delay must not be negative");
        }
        this.ownsExecutor = ownsExecutor;
        this.clientsByNodeId = new LinkedHashMap<>();
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
            Executors.newCachedThreadPool(), DEFAULT_REQUEST_DEADLINE, DEFAULT_HEDGING_DELAY, true);
    }

    /**
     * Factory accepting a caller-supplied {@link ExecutorService} and per-request deadline (default
     * hedging delay). The executor is <em>not</em> owned by the cluster — {@link #close()} leaves it
     * running (the caller shuts it down).
     */
    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, ReplicaClient> clientsByNodeId,
                                             ExecutorService executor,
                                             Duration requestDeadline) {
        return new LeaderlessKVCluster(membership, config, clientsByNodeId,
            executor, requestDeadline, DEFAULT_HEDGING_DELAY, false);
    }

    /**
     * Factory accepting a caller-supplied {@link ExecutorService}, per-request deadline, and hedging
     * delay. The executor is <em>not</em> owned by the cluster.
     */
    public static LeaderlessKVCluster create(ClusterMembership membership,
                                             QuorumConfig config,
                                             Map<String, ReplicaClient> clientsByNodeId,
                                             ExecutorService executor,
                                             Duration requestDeadline,
                                             Duration hedgingDelay) {
        return new LeaderlessKVCluster(membership, config, clientsByNodeId,
            executor, requestDeadline, hedgingDelay, false);
    }

    public ClusterMembership getMembership() {
        return membership;
    }

    public Duration requestDeadline() {
        return requestDeadline;
    }

    public Duration hedgingDelay() {
        return hedgingDelay;
    }

    /**
     * Cumulative count of backup (hedge) requests this cluster has fired since construction. Read by
     * the coordinator per-op (delta) to feed {@code ledgerkv_hedged_requests_total}.
     */
    public long hedgedRequestCount() {
        return hedgedRequestCount.get();
    }

    /**
     * Hedge candidates for {@code key}: the preference-list entries beyond the primary {@code N}, in
     * ring order. The ring de-dups to distinct physical nodes, so these are the next distinct replicas
     * after the primaries. May be empty when the cluster is too small to hold an extra distinct node.
     */
    private List<ClusterNode> hedgeCandidates(String key, int primaryCount) {
        // Cap the request at the cluster size — the ring rejects asking for more distinct nodes than
        // exist. On a cluster with no node beyond the primaries this yields an empty list (no hedge).
        int requested = Math.min(primaryCount + MAX_HEDGES, membership.getNodes().size());
        if (requested <= primaryCount) {
            return List.of();
        }
        List<ClusterNode> withHedges = membership.getPreferenceList(key, requested);
        if (withHedges.size() <= primaryCount) {
            return List.of();
        }
        return withHedges.subList(primaryCount, withHedges.size());
    }

    public List<ClusterNode> selectReplicas(String key) {
        return membership.getPreferenceList(key, membership.getReplicationFactor());
    }

    /** Every value {@code nodeId} holds for {@code key}: empty if absent, many if conflicted. */
    public List<VersionedValue> getReplicaValues(String nodeId, String key) {
        ReplicaClient client = clientsByNodeId.get(nodeId);
        if (client == null) {
            return List.of();
        }
        return client.get(key);
    }

    /**
     * The single value {@code nodeId} holds for {@code key}, or empty when it is absent <em>or</em>
     * that replica holds unresolved siblings — a scalar accessor cannot represent a conflict.
     */
    public Optional<VersionedValue> getReplicaValue(String nodeId, String key) {
        List<VersionedValue> values = getReplicaValues(nodeId, key);
        return values.size() == 1 ? Optional.of(values.get(0)) : Optional.empty();
    }

    public List<HintedHandoff> getPendingHints() {
        synchronized (hintsLock) {
            return List.copyOf(pendingHints);
        }
    }

    public QuorumResponse write(int coordinatorIndex, String key, String value) {
        Objects.requireNonNull(value, "value must not be null");
        return writeVersion(coordinatorIndex, key, value, false);
    }

    /**
     * Replicates a <b>tombstone</b> for {@code key} through the ordinary quorum write path: a
     * versioned marker meaning "deleted at this clock", which repairs and resolves conflicts like
     * any other version (Cassandra and Riak both do this).
     *
     * <p>A delete has to be a write. Absence carries no version, so a replica that never held the
     * key and one that deleted it look identical, and read repair cannot tell a deletion from a gap
     * — it would copy the surviving value back over the delete. Deleting from one node's local
     * engine and reporting success, as the public endpoint used to, leaves the value readable
     * from every other replica.
     *
     * <p>Reclaiming tombstones (Cassandra's {@code gc_grace_seconds}) is not implemented: they are
     * retained indefinitely, which is safe and unbounded.
     */
    public QuorumResponse delete(int coordinatorIndex, String key) {
        return writeVersion(coordinatorIndex, key, "", true);
    }

    private QuorumResponse writeVersion(int coordinatorIndex, String key, String value,
                                        boolean deleted) {
        validateCoordinator(coordinatorIndex);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + requestDeadline.toNanos();
        String coordinatorNodeId = membership.getNodes().get(coordinatorIndex).getId();
        List<ClusterNode> replicas = selectReplicas(key);

        // Derive the new version from what the replicas actually hold, the way Riak's coordinating
        // vnode does: read the current object, merge its causal context, and increment this
        // coordinator's own entry in that clock. The counter therefore lives in the data.
        //
        // It used to live in a per-key map on this object, which is what made both of the version
        // bugs possible: the map started empty on restart, so a restarted coordinator reissued
        // counter 1 while a surviving replica still held counter 2 and won the comparison; and
        // read repair overwrote the map with metadata it had read earlier, rewinding a
        // concurrent writer's clock. Deleting the map removes both.
        CausalContext context = readCausalContext(key, replicas, deadlineNanos);
        if (!context.quorumMet) {
            // Without a read quorum this coordinator cannot know which counters are already in use,
            // and guessing is what produced the counter-reuse bug on restart. Refuse rather
            // than reissue.
            FailureContext.Builder contextFailure = FailureContext.builder();
            for (ClusterNode replica : replicas) {
                contextFailure.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
            }
            return new QuorumResponse(false, null, List.of(), context.responded, config.getW(),
                elapsedMs(startNanos), contextFailure.build());
        }
        VersionMetadata observed = context.metadata;
        VersionMetadata nextMetadata = issuedClocks.compute(key, (k, lastIssued) -> {
            VersionMetadata base;
            if (observed == null) {
                base = lastIssued;
            } else if (lastIssued == null) {
                base = observed;
            } else {
                base = observed.merge(lastIssued); // componentwise max: never moves backwards
            }
            return base == null
                ? VersionMetadata.initial(coordinatorNodeId)
                : base.increment(coordinatorNodeId);
        });
        VersionedValue versionedValue =
            new VersionedValue(value, versionFor(nextMetadata), nextMetadata, deleted);

        List<ClusterNode> hedgeCandidates = hedgeCandidates(key, replicas.size());
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> futures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            futures.add(submitPut(completion, replica.getId(), key, versionedValue));
        }

        int acknowledgments = 0;
        int hedgesFired = 0;
        FailureContext.Builder failureContext = FailureContext.builder();
        List<String> acked = new ArrayList<>();
        java.util.Set<String> primaryIds = new java.util.HashSet<>();
        for (ClusterNode replica : replicas) {
            primaryIds.add(replica.getId());
        }
        int primaryAcks = 0;
        int completed = 0;
        int submitted = replicas.size();
        long hedgeAtNanos = startNanos + hedgingDelay.toNanos();
        try {
            // Return as soon as the quorum is satisfied. Continuing to wait after W acks were
            // already durable held the client behind a slow minority for no benefit.
            // Cassandra's write handler likewise signals the client
            // at consistency-level-many acks and lets the remaining replicas finish behind it.
            // If the primaries have not produced W acks by the hedging delay, fire ONE backup
            // request to the next distinct replica (request hedging): whichever of the slow primary
            // or the hedge returns first counts.
            while (completed < submitted) {
                if (quorumMet(acknowledgments, primaryAcks)) {
                    break;
                }
                if (hedgesFired < MAX_HEDGES && acknowledgments < config.getW()
                        && System.nanoTime() >= hedgeAtNanos && !hedgeCandidates.isEmpty()) {
                    futures.add(submitPut(completion,
                        hedgeCandidates.get(hedgesFired).getId(), key, versionedValue));
                    hedgesFired++;
                    submitted++;
                    hedgedRequestCount.incrementAndGet();
                    continue; // loop bound grew — re-evaluate before blocking again
                }
                long waitUntil = (hedgesFired < MAX_HEDGES && !hedgeCandidates.isEmpty())
                    ? Math.min(hedgeAtNanos, deadlineNanos) : deadlineNanos;
                ReplicaResult result = pollWithin(completion, waitUntil);
                if (result == null) {
                    if (System.nanoTime() >= deadlineNanos) {
                        break; // final deadline reached
                    }
                    continue; // hit the hedge-tier boundary, not the deadline — loop to hedge
                }
                completed++;
                if (result.ok && !acked.contains(result.nodeId)) {
                    acknowledgments++;
                    acked.add(result.nodeId);
                    if (primaryIds.contains(result.nodeId)) {
                        primaryAcks++;
                    }
                    failureContext.responded(result.nodeId);
                }
            }
        } catch (RuntimeException e) {
            cancelAll(futures);
            throw e;
        }

        boolean successful = quorumMet(acknowledgments, primaryAcks);
        // Primaries that had not acknowledged at the instant the client was released. Reported as
        // failed here because that is what the coordinator knows when it answers; the background
        // task below keeps waiting and only files a hint for the ones that never arrive.
        for (ClusterNode replica : replicas) {
            if (!acked.contains(replica.getId())) {
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
            }
        }

        if (successful) {
            finishReplicationInBackground(completion, futures, submitted - completed,
                coordinatorIndex, key, value, nextMetadata, deleted, replicas,
                new ArrayList<>(acked), deadlineNanos);
        } else {
            cancelAll(futures);
        }
        VersionedValue responseValue = successful ? versionedValue : null;
        return new QuorumResponse(successful, responseValue, List.of(), acknowledgments,
            config.getW(), elapsedMs(startNanos), failureContext.build());
    }

    /** Whether a write has met both the total and the primary-only acknowledgement thresholds. */
    private boolean quorumMet(int acknowledgments, int primaryAcks) {
        return acknowledgments >= config.getW() && primaryAcks >= config.getPw();
    }

    /**
     * Whether a read has met both thresholds. {@code responders} counts distinct replicas that
     * answered — not values, since one replica can return several siblings.
     */
    private boolean readQuorumMet(int responders, int primaryResponses) {
        return responders >= config.getR() && primaryResponses >= config.getPr();
    }

    /**
     * Keeps draining the replicas that had not answered when the client was released, and files a
     * hinted handoff for each primary that never does.
     *
     * <p>Hint accounting has to outlive the client's call for the same reason Cassandra records
     * hints from a late callback: at the moment it returns at W the coordinator does not yet know
     * which of the remaining replicas will fail, and abandoning them there would lose the hint.
     */
    private void finishReplicationInBackground(
            ExecutorCompletionService<ReplicaResult> completion,
            List<Future<ReplicaResult>> futures, int outstanding, int coordinatorIndex, String key,
            String value, VersionMetadata metadata, boolean deleted, List<ClusterNode> replicas,
            List<String> ackedSoFar, long deadlineNanos) {
        if (outstanding <= 0) {
            cancelAll(futures);
            recordMissingPrimaryHints(coordinatorIndex, key, value, metadata, deleted, replicas,
                ackedSoFar);
            return;
        }
        pendingReplications.incrementAndGet();
        executor.execute(() -> {
            try {
                List<String> acked = new ArrayList<>(ackedSoFar);
                for (int i = 0; i < outstanding; i++) {
                    ReplicaResult result = pollWithin(completion, deadlineNanos);
                    if (result == null) {
                        break; // deadline reached; whoever is left becomes a hint
                    }
                    if (result.ok && !acked.contains(result.nodeId)) {
                        acked.add(result.nodeId);
                    }
                }
                cancelAll(futures);
                recordMissingPrimaryHints(coordinatorIndex, key, value, metadata, deleted, replicas,
                    acked);
            } finally {
                if (pendingReplications.decrementAndGet() == 0) {
                    synchronized (replicationIdle) {
                        replicationIdle.notifyAll();
                    }
                }
            }
        });
    }

    /**
     * Files a hint for every PRIMARY that never acknowledged. Hedge targets are not primaries: they
     * contribute an acknowledgement if they win, but are never hinted.
     */
    private void recordMissingPrimaryHints(int coordinatorIndex, String key, String value,
            VersionMetadata metadata, boolean deleted, List<ClusterNode> replicas,
            List<String> acked) {
        List<String> failedReplicaIds = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            if (!acked.contains(replica.getId())) {
                failedReplicaIds.add(replica.getId());
            }
        }
        recordHints(coordinatorIndex, key, value, metadata, deleted, failedReplicaIds);
    }

    /**
     * Blocks until every write released early at W has finished accounting for its remaining
     * replicas, so a caller (or a test) can observe hinted handoffs deterministically instead of
     * racing the background task.
     *
     * @return true if replication went idle within the timeout
     */
    public boolean awaitReplication(Duration timeout) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        synchronized (replicationIdle) {
            while (pendingReplications.get() > 0) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                replicationIdle.wait(Math.max(1L, remaining / 1_000_000L));
            }
        }
        return true;
    }

    public HintedHandoffReplayResult replayPendingHints() {
        List<HintedHandoff> toReplay;
        synchronized (hintsLock) {
            toReplay = new ArrayList<>(pendingHints);
        }
        int attemptedCount = toReplay.size();
        int appliedCount = 0;
        List<HintedHandoff> remainingHints = new ArrayList<>();
        List<HintedHandoffReplayResult.Failure> failures = new ArrayList<>();

        for (HintedHandoff hint : toReplay) {
            ReplicaClient client = clientsByNodeId.get(hint.getTargetNodeId());
            if (client == null) {
                remainingHints.add(hint);
                continue;
            }

            try {
                VersionMetadata hintMetadata = hint.getVersionMetadata();
                client.deliverHint(hint.getKey(), new VersionedValue(
                    hint.getValue(), versionFor(hintMetadata), hintMetadata, hint.isDeleted()));
                appliedCount++;
            } catch (RuntimeException undelivered) {
                // Keep the cause. A hint that fails because its replica is down and one that fails
                // because this coordinator cannot make any call at all produce the same count, and
                // discarding the exception is what makes the two indistinguishable after the fact.
                failures.add(new HintedHandoffReplayResult.Failure(
                    hint.getTargetNodeId(), hint.getKey(), undelivered));
                remainingHints.add(hint);
            }
        }

        int outstanding;
        synchronized (hintsLock) {
            // Drop exactly the hints we attempted (the delivered ones), preserving any hint added
            // concurrently during replay; re-queue the ones that still failed. (The prior clear()+
            // addAll(remaining) would have silently discarded a concurrently-added hint.)
            pendingHints.removeAll(toReplay);
            pendingHints.addAll(remainingHints);
            outstanding = pendingHints.size();
        }
        return new HintedHandoffReplayResult(attemptedCount, appliedCount, outstanding, failures);
    }

    public QuorumResponse read(int coordinatorIndex, String key) {
        validateCoordinator(coordinatorIndex);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + requestDeadline.toNanos();

        List<ClusterNode> replicas = selectReplicas(key);
        List<ClusterNode> hedgeCandidates = hedgeCandidates(key, replicas.size());
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> futures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            futures.add(submitGet(completion, replica.getId(), key));
        }

        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();
        List<String> responded = new ArrayList<>();
        java.util.Set<String> primaryIds = new java.util.HashSet<>();
        for (ClusterNode replica : replicas) {
            primaryIds.add(replica.getId());
        }
        int primaryResponses = 0;
        int completedCount = 0;
        int hedgesFired = 0;
        int submitted = replicas.size();
        long hedgeAtNanos = startNanos + hedgingDelay.toNanos();
        try {
            // Block only until R replicas have RESPONDED (or the deadline). The threshold counts
            // responders, not values: since replicas hold sibling sets, one replica returning two
            // concurrent values would otherwise satisfy R=2 on its own.
            // If R reads have not arrived by the hedging delay, fire ONE backup read to the next
            // distinct replica (request hedging); a slow replica does not hold up the read.
            while (completedCount < submitted && !readQuorumMet(responded.size(), primaryResponses)) {
                if (hedgesFired < MAX_HEDGES && !readQuorumMet(responded.size(), primaryResponses)
                        && System.nanoTime() >= hedgeAtNanos && !hedgeCandidates.isEmpty()) {
                    futures.add(submitGet(completion, hedgeCandidates.get(hedgesFired).getId(), key));
                    hedgesFired++;
                    submitted++;
                    hedgedRequestCount.incrementAndGet();
                    continue; // loop bound grew — re-evaluate before blocking again
                }
                long waitUntil = (hedgesFired < MAX_HEDGES && !hedgeCandidates.isEmpty())
                    ? Math.min(hedgeAtNanos, deadlineNanos) : deadlineNanos;
                ReplicaResult result = pollWithin(completion, waitUntil);
                if (result == null) {
                    if (System.nanoTime() >= deadlineNanos) {
                        break;
                    }
                    continue; // hedge-tier boundary, not the deadline — loop to hedge
                }
                completedCount++;
                if (result.ok && !responded.contains(result.nodeId)) {
                    // A replica contributes every value it holds, so a conflict that exists on one
                    // replica reaches the reader instead of being flattened at the transport.
                    if (result.values.isEmpty()) {
                        values.add(null); // this replica has no value for the key
                    } else {
                        values.addAll(result.values);
                    }
                    responded.add(result.nodeId);
                    if (primaryIds.contains(result.nodeId)) {
                        primaryResponses++;
                    }
                    failureContext.responded(result.nodeId);
                }
            }
        } finally {
            cancelAll(futures);
        }

        // Primaries we never heard a successful response from (threw, or too slow) are failures.
        // Hedge targets are not primaries, so they are not marked failed.
        for (ClusterNode replica : replicas) {
            if (!responded.contains(replica.getId())) {
                failureContext.failed(replica.getId(), FailureCause.UNAVAILABLE_NODE);
            }
        }

        boolean successful = readQuorumMet(responded.size(), primaryResponses);
        VersionedValue latest = successful
            ? values.stream()
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(VersionedValue::getVersion))
                .orElse(null)
            : null;

        return new QuorumResponse(successful, latest, values, responded.size(),
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
                    return ReplicaResult.ok(nodeId, clientsByNodeId.get(nodeId).get(key));
                } catch (RuntimeException e) {
                    return ReplicaResult.failed(nodeId);
                }
            }));
        }

        List<VersionedValue> values = new ArrayList<>();
        FailureContext.Builder failureContext = FailureContext.builder();
        Map<String, List<VersionedValue>> currentByNode = new LinkedHashMap<>();
        int completedCount = 0;
        try {
            while (completedCount < replicas.size()) {
                ReplicaResult result = pollWithin(completion, deadlineNanos);
                if (result == null) {
                    break;
                }
                completedCount++;
                if (result.ok) {
                    if (result.values.isEmpty()) {
                        values.add(null);
                    } else {
                        values.addAll(result.values);
                    }
                    currentByNode.put(result.nodeId, result.values);
                    failureContext.responded(result.nodeId);
                } else {
                    failureContext.failed(result.nodeId, FailureCause.UNAVAILABLE_NODE);
                }
            }
        } finally {
            cancelAll(readFutures);
        }

        // Resolve causally BEFORE touching storage. Picking a winner by scalar version cannot order
        // two concurrent vector clocks — the counter sums are frequently equal — so the previous
        // max-by-version choice was effectively decided by which response arrived first, and then
        // written over the other branch.
        List<VersionedValue> observed = values.stream()
            .filter(Objects::nonNull)
            .collect(java.util.stream.Collectors.toList());
        List<VersionedValue> frontier = VersionConflictResolver.causalFrontier(observed);
        VersionedValue latest = frontier.size() == 1 ? frontier.get(0) : null;

        if (latest != null) {
            // Phase 2: push the single causally dominant value to replicas that do not already hold
            // exactly it, concurrently within the same deadline budget. Repair writes do not gate
            // the response (matches prior behavior); failures only mark the failure context.
            long repairDeadlineNanos = System.nanoTime() + requestDeadline.toNanos();
            List<Future<ReplicaResult>> repairFutures = new ArrayList<>();
            for (ClusterNode replica : replicas) {
                String nodeId = replica.getId();
                List<VersionedValue> current = currentByNode.get(nodeId);
                if (current != null && current.size() == 1 && sameStoredValue(current.get(0), latest)) {
                    continue; // already up to date
                }
                VersionedValue toWrite = latest;
                repairFutures.add(executor.submit(() -> {
                    clientsByNodeId.get(nodeId).put(key, toWrite);
                    return ReplicaResult.written(nodeId);
                }));
            }
            awaitRepairWrites(repairFutures, repairDeadlineNanos);
        }
        // When the frontier holds more than one value the conflict is genuinely unresolved. Repair
        // reports it and writes nothing: siblings are the reader's to resolve, and collapsing them
        // here would destroy a branch no one ever chose to discard.

        boolean successful = !values.isEmpty();
        return new QuorumResponse(successful, latest, values, values.size(),
            config.getN(), elapsedMs(startNanos), failureContext.build());
    }

    /**
     * Reads the current causal context for {@code key} from its replica set: the componentwise
     * merge of every vector clock the replicas hold, which is the clock a new write must descend
     * from. Returns as soon as {@code R} replicas have answered.
     *
     * <p>This is the extra round trip that durable version allocation costs here. Riak avoids it by
     * coordinating the write on a node that is already in the key's preference list, so its read is
     * local; this coordinator may not be in the preference list at all.
     */
    private CausalContext readCausalContext(String key, List<ClusterNode> replicas,
                                            long deadlineNanos) {
        ExecutorCompletionService<ReplicaResult> completion = new ExecutorCompletionService<>(executor);
        List<Future<ReplicaResult>> futures = new ArrayList<>();
        for (ClusterNode replica : replicas) {
            futures.add(submitGet(completion, replica.getId(), key));
        }

        VersionMetadata merged = null;
        int completed = 0;
        int successes = 0;
        try {
            while (completed < replicas.size() && successes < config.getR()) {
                ReplicaResult result = pollWithin(completion, deadlineNanos);
                if (result == null) {
                    break;
                }
                completed++;
                if (!result.ok) {
                    continue;
                }
                successes++;
                for (VersionedValue observed : result.values) {
                    merged = merged == null
                        ? observed.getVersionMetadata()
                        : merged.merge(observed.getVersionMetadata());
                }
            }
        } finally {
            cancelAll(futures);
        }
        return new CausalContext(successes >= config.getR(), successes, merged);
    }

    /** The merged causal context observed before a write, plus how many replicas answered. */
    private static final class CausalContext {
        final boolean quorumMet;
        final int responded;
        final VersionMetadata metadata; // null when the key is absent everywhere observed

        CausalContext(boolean quorumMet, int responded, VersionMetadata metadata) {
            this.quorumMet = quorumMet;
            this.responded = responded;
            this.metadata = metadata;
        }
    }

    /** Submits a replica PUT task that catches its own failure and reports it as a {@link ReplicaResult}. */
    private Future<ReplicaResult> submitPut(ExecutorCompletionService<ReplicaResult> completion,
                                            String nodeId, String key, VersionedValue value) {
        return completion.submit(() -> {
            try {
                clientsByNodeId.get(nodeId).put(key, value);
                return ReplicaResult.written(nodeId);
            } catch (RuntimeException e) {
                return ReplicaResult.failed(nodeId);
            }
        });
    }

    /** Submits a replica GET task that catches its own failure and reports it as a {@link ReplicaResult}. */
    private Future<ReplicaResult> submitGet(ExecutorCompletionService<ReplicaResult> completion,
                                            String nodeId, String key) {
        return completion.submit(() -> {
            try {
                return ReplicaResult.ok(nodeId, clientsByNodeId.get(nodeId).get(key));
            } catch (RuntimeException e) {
                return ReplicaResult.failed(nodeId);
            }
        });
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

    /**
     * Stops waiting on outstanding replica tasks without interrupting them.
     *
     * <p>{@code cancel(true)} would interrupt a thread that may be inside a {@link
     * java.nio.channels.FileChannel} write, and interrupting a channel operation <em>closes the
     * channel</em> — permanently breaking that replica's write-ahead log for every later write, not
     * just this one. The coordinator only needs to stop waiting; a task that is already running is
     * left to finish on its own, and a task that has not started is prevented from starting.
     */
    private static void cancelAll(List<Future<ReplicaResult>> futures) {
        for (Future<ReplicaResult> future : futures) {
            if (!future.isDone()) {
                future.cancel(false);
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

    /**
     * Waits for replication released early at W to finish accounting, then shuts down the executor
     * if this cluster owns it (a caller-supplied pool is the caller's to shut down).
     *
     * <p>Draining first matters: tearing the pool down under a running replication task interrupts
     * it mid-write, which closes the replica's channel rather than merely abandoning the write.
     */
    @Override
    public void close() {
        try {
            awaitReplication(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (ownsExecutor) {
            executor.shutdown();
        }
    }

    private void validateCoordinator(int coordinatorIndex) {
        if (coordinatorIndex < 0 || coordinatorIndex >= membership.getNodes().size()) {
            throw new IllegalArgumentException("coordinator index must identify a cluster node");
        }
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
                             VersionMetadata versionMetadata, boolean deleted,
                             List<String> failedReplicaIds) {
        if (failedReplicaIds.isEmpty()) {
            return;
        }

        String coordinatorNodeId = membership.getNodes().get(coordinatorIndex).getId();
        synchronized (hintsLock) {
            for (String failedReplicaId : failedReplicaIds) {
                pendingHints.add(new HintedHandoff(
                    coordinatorNodeId,
                    failedReplicaId,
                    key,
                    value,
                    versionMetadata,
                    deleted
                ));
            }
        }
    }

    private static boolean sameStoredValue(VersionedValue left, VersionedValue right) {
        return left.getValue().equals(right.getValue())
            && left.getVersionMetadata().equals(right.getVersionMetadata());
    }

    /**
     * Carrier for a single replica's fan-out outcome. {@code values} holds every value that replica
     * returned — empty when the key is absent there, more than one when that replica itself holds
     * unresolved siblings.
     */
    private static final class ReplicaResult {
        final String nodeId;
        final boolean ok;
        final List<VersionedValue> values;

        private ReplicaResult(String nodeId, boolean ok, List<VersionedValue> values) {
            this.nodeId = nodeId;
            this.ok = ok;
            this.values = values;
        }

        static ReplicaResult ok(String nodeId, List<VersionedValue> values) {
            return new ReplicaResult(nodeId, true, values);
        }

        static ReplicaResult written(String nodeId) {
            return new ReplicaResult(nodeId, true, List.of());
        }

        static ReplicaResult failed(String nodeId) {
            return new ReplicaResult(nodeId, false, List.of());
        }
    }
}
