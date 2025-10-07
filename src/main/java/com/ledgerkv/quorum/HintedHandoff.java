package com.ledgerkv.quorum;

import com.ledgerkv.consistency.VersionMetadata;

import java.util.Objects;

public final class HintedHandoff {
    private final String coordinatorNodeId;
    private final String targetNodeId;
    private final String key;
    private final String value;
    private final VersionMetadata versionMetadata;

    public HintedHandoff(String coordinatorNodeId, String targetNodeId, String key,
                         String value, VersionMetadata versionMetadata) {
        this.coordinatorNodeId = requireNonBlank(coordinatorNodeId, "coordinator node id");
        this.targetNodeId = requireNonBlank(targetNodeId, "target node id");
        this.key = requireNonBlank(key, "key");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.versionMetadata = Objects.requireNonNull(versionMetadata, "version metadata must not be null");
    }

    public String getCoordinatorNodeId() {
        return coordinatorNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public VersionMetadata getVersionMetadata() {
        return versionMetadata;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
