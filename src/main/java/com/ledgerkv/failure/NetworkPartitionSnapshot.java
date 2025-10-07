package com.ledgerkv.failure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class NetworkPartitionSnapshot {
    private final boolean active;
    private final List<Set<String>> partitionGroups;
    private final Map<String, Integer> groupByNodeId;

    NetworkPartitionSnapshot(boolean active,
                             List<Set<String>> partitionGroups,
                             Map<String, Integer> groupByNodeId) {
        this.active = active;
        this.partitionGroups = copyGroups(partitionGroups);
        this.groupByNodeId = Collections.unmodifiableMap(new LinkedHashMap<>(groupByNodeId));
    }

    public boolean isActive() {
        return active;
    }

    public boolean isHealed() {
        return !active;
    }

    public List<Set<String>> getPartitionGroups() {
        return partitionGroups;
    }

    public Optional<Integer> getGroupIndex(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        return Optional.ofNullable(groupByNodeId.get(nodeId));
    }

    private static List<Set<String>> copyGroups(List<Set<String>> groups) {
        List<Set<String>> copies = new ArrayList<>();
        for (Set<String> group : groups) {
            copies.add(Collections.unmodifiableSet(new LinkedHashSet<>(group)));
        }
        return Collections.unmodifiableList(copies);
    }
}
