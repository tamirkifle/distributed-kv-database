package com.ledgerkv.quorum;

import com.ledgerkv.consistency.VersionMetadata;

import java.util.Objects;

public final class HintedHandoff {
    private final String coordinatorNodeId;
    private final String targetNodeId;
    private final String key;
    private final String value;
    private final VersionMetadata versionMetadata;
    private final boolean deleted;

    public HintedHandoff(String coordinatorNodeId, String targetNodeId, String key,
                         String value, VersionMetadata versionMetadata) {
        this(coordinatorNodeId, targetNodeId, key, value, versionMetadata, false);
    }

    public HintedHandoff(String coordinatorNodeId, String targetNodeId, String key,
                         String value, VersionMetadata versionMetadata, boolean deleted) {
        this.deleted = deleted;
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

    /**
     * True when the hinted write is a tombstone. Without this the hint would replay as an ordinary
     * empty-string value and undo the delete on the replica it was meant to carry it to.
     */
    public boolean isDeleted() {
        return deleted;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
