package com.ledgerkv.failure;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class FailurePolicySnapshot {
    private final long seed;
    private final Set<String> unavailableNodeIds;
    private final Map<String, Integer> fixedLatencyByNodeId;
    private final double messageDropRate;

    FailurePolicySnapshot(long seed,
                          Set<String> unavailableNodeIds,
                          Map<String, Integer> fixedLatencyByNodeId,
                          double messageDropRate) {
        this.seed = seed;
        this.unavailableNodeIds = Collections.unmodifiableSet(new LinkedHashSet<>(unavailableNodeIds));
        this.fixedLatencyByNodeId = Collections.unmodifiableMap(new LinkedHashMap<>(fixedLatencyByNodeId));
        this.messageDropRate = messageDropRate;
    }

    public long getSeed() {
        return seed;
    }

    public Set<String> getUnavailableNodeIds() {
        return unavailableNodeIds;
    }

    public Map<String, Integer> getFixedLatencyByNodeId() {
        return fixedLatencyByNodeId;
    }

    public double getMessageDropRate() {
        return messageDropRate;
    }
}
