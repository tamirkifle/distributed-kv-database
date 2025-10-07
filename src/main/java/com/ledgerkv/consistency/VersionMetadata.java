package com.ledgerkv.consistency;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class VersionMetadata {
    private static final String LEGACY_NODE_ID = "legacy";

    private final Map<String, Long> vectorClock;

    private VersionMetadata(Map<String, Long> vectorClock) {
        this.vectorClock = Collections.unmodifiableMap(new LinkedHashMap<>(vectorClock));
    }

    public static VersionMetadata initial(String nodeId) {
        validateNodeId(nodeId);
        Map<String, Long> clock = new LinkedHashMap<>();
        clock.put(nodeId, 1L);
        return new VersionMetadata(clock);
    }

    public static VersionMetadata legacy(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0");
        }
        Map<String, Long> clock = new LinkedHashMap<>();
        clock.put(LEGACY_NODE_ID, version);
        return new VersionMetadata(clock);
    }

    public VersionMetadata increment(String nodeId) {
        validateNodeId(nodeId);
        Map<String, Long> clock = new LinkedHashMap<>(vectorClock);
        clock.put(nodeId, getCounter(nodeId) + 1);
        return new VersionMetadata(clock);
    }

    public VersionMetadata merge(VersionMetadata other) {
        Objects.requireNonNull(other, "other metadata must not be null");
        Map<String, Long> merged = new LinkedHashMap<>(vectorClock);
        for (Map.Entry<String, Long> entry : other.vectorClock.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return new VersionMetadata(merged);
    }

    public boolean happensBefore(VersionMetadata other) {
        Objects.requireNonNull(other, "other metadata must not be null");
        boolean strictlyLess = false;

        for (String nodeId : unionNodeIds(other)) {
            long localCounter = getCounter(nodeId);
            long otherCounter = other.getCounter(nodeId);
            if (localCounter > otherCounter) {
                return false;
            }
            if (localCounter < otherCounter) {
                strictlyLess = true;
            }
        }

        return strictlyLess;
    }

    public boolean isConcurrentWith(VersionMetadata other) {
        Objects.requireNonNull(other, "other metadata must not be null");
        return !equals(other) && !happensBefore(other) && !other.happensBefore(this);
    }

    public long getCounter(String nodeId) {
        validateNodeId(nodeId);
        return vectorClock.getOrDefault(nodeId, 0L);
    }

    public Map<String, Long> getVectorClock() {
        return vectorClock;
    }

    private Iterable<String> unionNodeIds(VersionMetadata other) {
        Map<String, Long> nodeIds = new LinkedHashMap<>(vectorClock);
        for (String nodeId : other.vectorClock.keySet()) {
            nodeIds.putIfAbsent(nodeId, 0L);
        }
        return nodeIds.keySet();
    }

    private static void validateNodeId(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof VersionMetadata)) {
            return false;
        }
        VersionMetadata that = (VersionMetadata) other;
        return vectorClock.equals(that.vectorClock);
    }

    @Override
    public int hashCode() {
        return vectorClock.hashCode();
    }

    @Override
    public String toString() {
        return "VersionMetadata" + vectorClock;
    }
}
