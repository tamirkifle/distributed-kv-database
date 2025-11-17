package com.ledgerkv;

import com.ledgerkv.checker.ConsistencyCheckResult;
import com.ledgerkv.checker.ConsistencyHistoryChecker;
import com.ledgerkv.checker.ConsistencyViolation;
import com.ledgerkv.checker.OperationHistory;
import com.ledgerkv.checker.OperationHistoryRecorder;
import com.ledgerkv.checker.OperationRecord;
import com.ledgerkv.checker.OperationType;
import com.ledgerkv.quorum.ClusterMembership;
import com.ledgerkv.quorum.ClusterNode;
import com.ledgerkv.quorum.InMemoryReplicaClient;
import com.ledgerkv.quorum.LeaderlessKVCluster;
import com.ledgerkv.quorum.ReplicaClient;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomizedFailureHistoryTest {
    private static final long SEED = 2026051507L;
    private static final String KEY = "trace:random-history";
    private static final Instant START = Instant.parse("2026-05-15T12:00:00Z");

    @Test
    void seededMixedOperationsRecordMinimalFailingHistorySummary() {
        QuorumConfig weakConfig = new QuorumConfig(3, 1, 1);
        ClusterMembership membership = ClusterMembership.create("random-history", 3, 3);
        MutableFailureController failures = new MutableFailureController();
        LeaderlessKVCluster cluster = LeaderlessKVCluster.create(
            membership,
            weakConfig,
            clientsFor(membership, failures)
        );
        OperationHistoryRecorder recorder = new OperationHistoryRecorder();

        List<ScenarioStep> steps = scenarioFor(SEED, membership.selectReplicas(KEY));
        for (int i = 0; i < steps.size(); i++) {
            runStep(cluster, recorder, weakConfig, failures, steps.get(i), i);
        }

        OperationHistory history = recorder.snapshot();
        ConsistencyCheckResult result = new ConsistencyHistoryChecker().check(history);
        String summary = minimalFailureSummary(SEED, result, history);
        System.out.println(summary);

        assertFalse(result.isValid(), summary);
        assertTrue(summary.contains("seed=" + SEED));
        assertTrue(summary.contains("firstViolation="));
        assertTrue(summary.contains("history="));
        assertTrue(history.getOperations().stream().anyMatch(record -> record.getType() == OperationType.READ));
        assertTrue(history.getOperations().stream().anyMatch(record -> record.getType() == OperationType.WRITE));
        assertTrue(history.getOperations().stream()
            .anyMatch(record -> !record.getFailureContext().getFailedNodeIds().isEmpty()));
    }

    private static void runStep(LeaderlessKVCluster cluster,
                                OperationHistoryRecorder recorder,
                                QuorumConfig config,
                                MutableFailureController failures,
                                ScenarioStep step,
                                int index) {
        failures.replaceUnavailableNodes(step.unavailableNodeIds);
        Instant start = START.plusMillis(index * 10L);
        Instant end = start.plusMillis(1L);

        if (step.type == OperationType.WRITE) {
            QuorumResponse response = cluster.write(step.coordinator, KEY, step.value);
            recorder.recordWrite(start, end, step.coordinator, config, KEY, step.value, response);
        } else {
            QuorumResponse response = cluster.read(step.coordinator, KEY);
            recorder.recordRead(start, end, step.coordinator, config, KEY, response);
        }
    }

    private static List<ScenarioStep> scenarioFor(long seed, List<ClusterNode> replicas) {
        Random random = new Random(seed);
        List<ScenarioStep> steps = new ArrayList<>();
        String firstReplicaId = replicas.get(0).getId();

        steps.add(ScenarioStep.write(random.nextInt(3), "v0", Set.of()));
        steps.add(ScenarioStep.write(random.nextInt(3), "v1", Set.of(firstReplicaId)));
        steps.add(ScenarioStep.read(random.nextInt(3), Set.of()));

        for (int i = 2; i < 10; i++) {
            String unavailableNode = replicas.get(random.nextInt(replicas.size())).getId();
            Set<String> unavailableNodes = random.nextBoolean() ? Set.of(unavailableNode) : Set.of();
            if (random.nextDouble() < 0.55) {
                steps.add(ScenarioStep.write(random.nextInt(3), "v" + i, unavailableNodes));
            } else {
                steps.add(ScenarioStep.read(random.nextInt(3), unavailableNodes));
            }
        }

        return steps;
    }

    private static String minimalFailureSummary(long seed,
                                                ConsistencyCheckResult result,
                                                OperationHistory history) {
        StringBuilder summary = new StringBuilder();
        summary.append("seed=").append(seed);
        int operationLimit = history.getOperations().size();
        if (!result.getViolations().isEmpty()) {
            ConsistencyViolation first = result.getViolations().get(0);
            operationLimit = history.getOperations().indexOf(first.getOperation()) + 1;
            summary.append(", firstViolation=")
                .append(first.getType())
                .append("[key=").append(first.getKey())
                .append(", expected=").append(first.getExpectedValue())
                .append(", observed=").append(first.getObservedValue())
                .append("]");
        } else {
            summary.append(", firstViolation=none");
        }

        summary.append(", history=");
        List<OperationRecord> operations = history.getOperations();
        for (int i = 0; i < operationLimit; i++) {
            if (i > 0) {
                summary.append(" -> ");
            }
            summary.append(compact(operations.get(i)));
        }
        return summary.toString();
    }

    private static String compact(OperationRecord record) {
        return record.getType()
            + "("
            + record.getValue()
            + ",c=" + record.getCoordinator()
            + ",failed=" + record.getFailureContext().getFailedNodeIds()
            + ")";
    }

    private static Map<String, ReplicaClient> clientsFor(ClusterMembership membership,
                                                         MutableFailureController failures) {
        Map<String, ReplicaClient> clients = new LinkedHashMap<>();
        for (ClusterNode node : membership.getNodes()) {
            clients.put(node.getId(), new ControlledFailureReplicaClient(node.getId(), failures));
        }
        return clients;
    }

    private static final class ScenarioStep {
        private final OperationType type;
        private final int coordinator;
        private final String value;
        private final Set<String> unavailableNodeIds;

        private ScenarioStep(OperationType type, int coordinator, String value, Set<String> unavailableNodeIds) {
            this.type = type;
            this.coordinator = coordinator;
            this.value = value;
            this.unavailableNodeIds = Set.copyOf(unavailableNodeIds);
        }

        private static ScenarioStep write(int coordinator, String value, Set<String> unavailableNodeIds) {
            return new ScenarioStep(OperationType.WRITE, coordinator, value, unavailableNodeIds);
        }

        private static ScenarioStep read(int coordinator, Set<String> unavailableNodeIds) {
            return new ScenarioStep(OperationType.READ, coordinator, null, unavailableNodeIds);
        }
    }

    private static final class MutableFailureController {
        private final Set<String> unavailableNodeIds = new LinkedHashSet<>();

        private void replaceUnavailableNodes(Set<String> nodeIds) {
            unavailableNodeIds.clear();
            unavailableNodeIds.addAll(nodeIds);
        }

        private void throwIfUnavailable(String nodeId) {
            if (unavailableNodeIds.contains(nodeId)) {
                throw new IllegalStateException("replica unavailable");
            }
        }
    }

    private static final class ControlledFailureReplicaClient implements ReplicaClient {
        private final String nodeId;
        private final MutableFailureController failures;
        private final InMemoryReplicaClient delegate;

        private ControlledFailureReplicaClient(String nodeId, MutableFailureController failures) {
            this.nodeId = nodeId;
            this.failures = failures;
            this.delegate = new InMemoryReplicaClient(nodeId);
        }

        @Override
        public String nodeId() {
            return nodeId;
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            failures.throwIfUnavailable(nodeId);
            return delegate.get(key);
        }

        @Override
        public void put(String key, VersionedValue value) {
            failures.throwIfUnavailable(nodeId);
            delegate.put(key, value);
        }

        @Override
        public void deliverHint(String key, VersionedValue value) {
            failures.throwIfUnavailable(nodeId);
            delegate.deliverHint(key, value);
        }
    }
}
