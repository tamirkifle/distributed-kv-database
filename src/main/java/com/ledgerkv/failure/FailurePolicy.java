package com.ledgerkv.failure;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic failure simulation policy for in-process cluster tests.
 */
public final class FailurePolicy {
    private final long seed;
    private final Set<String> unavailableNodeIds;
    private final Map<String, Integer> fixedLatencyByNodeId;
    private final double messageDropRate;

    private FailurePolicy(Builder builder) {
        this.seed = builder.seed;
        this.unavailableNodeIds = new LinkedHashSet<>(builder.unavailableNodeIds);
        this.fixedLatencyByNodeId = new LinkedHashMap<>(builder.fixedLatencyByNodeId);
        this.messageDropRate = builder.messageDropRate;
    }

    public static Builder builder(long seed) {
        return new Builder(seed);
    }

    public boolean isNodeAvailable(String nodeId) {
        validateNodeId(nodeId);
        return !unavailableNodeIds.contains(nodeId);
    }

    public int latencyMsFor(String nodeId) {
        validateNodeId(nodeId);
        return fixedLatencyByNodeId.getOrDefault(nodeId, 0);
    }

    public boolean shouldDropMessage(String messageId, String sourceNodeId, String destinationNodeId) {
        validateIdentifier(messageId, "message id");
        validateNodeId(sourceNodeId);
        validateNodeId(destinationNodeId);

        if (messageDropRate <= 0.0) {
            return false;
        }
        if (messageDropRate >= 1.0) {
            return true;
        }

        return deterministicUnitInterval(messageId, sourceNodeId, destinationNodeId) < messageDropRate;
    }

    public FailurePolicySnapshot snapshot() {
        return new FailurePolicySnapshot(
            seed,
            unavailableNodeIds,
            fixedLatencyByNodeId,
            messageDropRate
        );
    }

    private double deterministicUnitInterval(String messageId, String sourceNodeId, String destinationNodeId) {
        long hash = mix(seed);
        hash = mix(hash ^ messageId.hashCode());
        hash = mix(hash ^ sourceNodeId.hashCode());
        hash = mix(hash ^ destinationNodeId.hashCode());
        return (hash >>> 11) * 0x1.0p-53;
    }

    private static long mix(long value) {
        long mixed = value;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= mixed >>> 33;
        return mixed;
    }

    private static void validateNodeId(String nodeId) {
        validateIdentifier(nodeId, "node id");
    }

    private static void validateIdentifier(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    public static final class Builder {
        private final long seed;
        private final Set<String> unavailableNodeIds = new LinkedHashSet<>();
        private final Map<String, Integer> fixedLatencyByNodeId = new LinkedHashMap<>();
        private double messageDropRate;

        private Builder(long seed) {
            this.seed = seed;
        }

        public Builder unavailableNode(String nodeId) {
            validateNodeId(nodeId);
            unavailableNodeIds.add(nodeId);
            return this;
        }

        public Builder fixedLatency(String nodeId, int latencyMs) {
            validateNodeId(nodeId);
            if (latencyMs < 0) {
                throw new IllegalArgumentException("latency must be >= 0");
            }
            fixedLatencyByNodeId.put(nodeId, latencyMs);
            return this;
        }

        public Builder messageDropRate(double dropRate) {
            if (Double.isNaN(dropRate) || dropRate < 0.0 || dropRate > 1.0) {
                throw new IllegalArgumentException("message drop rate must be between 0.0 and 1.0");
            }
            this.messageDropRate = dropRate;
            return this;
        }

        public FailurePolicy build() {
            return new FailurePolicy(this);
        }
    }
}
