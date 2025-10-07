package com.ledgerkv.failure;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class FailureContext {
    private static final FailureContext EMPTY = new FailureContext(
        Collections.emptySet(),
        Collections.emptyMap()
    );

    private final Set<String> respondingNodeIds;
    private final Map<String, FailureCause> failureCauses;

    private FailureContext(Set<String> respondingNodeIds, Map<String, FailureCause> failureCauses) {
        this.respondingNodeIds = Collections.unmodifiableSet(new LinkedHashSet<>(respondingNodeIds));
        this.failureCauses = Collections.unmodifiableMap(new LinkedHashMap<>(failureCauses));
    }

    public static FailureContext empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Set<String> getRespondingNodeIds() {
        return respondingNodeIds;
    }

    public Set<String> getFailedNodeIds() {
        return failureCauses.keySet();
    }

    public Map<String, FailureCause> getFailureCauses() {
        return failureCauses;
    }

    private static void validateNodeId(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
    }

    public static final class Builder {
        private final Set<String> respondingNodeIds = new LinkedHashSet<>();
        private final Map<String, FailureCause> failureCauses = new LinkedHashMap<>();

        public Builder responded(String nodeId) {
            validateNodeId(nodeId);
            respondingNodeIds.add(nodeId);
            return this;
        }

        public Builder failed(String nodeId, FailureCause cause) {
            validateNodeId(nodeId);
            if (cause == null) {
                throw new IllegalArgumentException("failure cause must not be null");
            }
            failureCauses.put(nodeId, cause);
            return this;
        }

        public FailureContext build() {
            return new FailureContext(respondingNodeIds, failureCauses);
        }
    }
}
