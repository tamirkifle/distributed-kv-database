package com.ledgerkv.failure;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class NetworkPartition {
    private final List<Set<String>> partitionGroups;
    private final Map<String, Integer> groupByNodeId;
    private boolean active;

    private NetworkPartition(List<Set<String>> partitionGroups) {
        this.partitionGroups = copyGroups(partitionGroups);
        this.groupByNodeId = indexGroups(this.partitionGroups);
        this.active = true;
    }

    public static NetworkPartition partitioned(List<Set<String>> partitionGroups) {
        validateGroups(partitionGroups);
        return new NetworkPartition(partitionGroups);
    }

    public boolean canCommunicate(String sourceNodeId, String destinationNodeId) {
        validateNodeId(sourceNodeId);
        validateNodeId(destinationNodeId);
        if (!active || sourceNodeId.equals(destinationNodeId)) {
            return true;
        }

        Integer sourceGroup = groupByNodeId.get(sourceNodeId);
        Integer destinationGroup = groupByNodeId.get(destinationNodeId);
        if (sourceGroup == null || destinationGroup == null) {
            return true;
        }
        return sourceGroup.equals(destinationGroup);
    }

    public void heal() {
        this.active = false;
    }

    public NetworkPartitionSnapshot snapshot() {
        return new NetworkPartitionSnapshot(active, partitionGroups, groupByNodeId);
    }

    private static void validateGroups(List<Set<String>> partitionGroups) {
        if (partitionGroups == null || partitionGroups.size() < 2) {
            throw new IllegalArgumentException("partition requires at least two groups");
        }

        Set<String> seenNodeIds = new LinkedHashSet<>();
        for (Set<String> group : partitionGroups) {
            if (group == null || group.isEmpty()) {
                throw new IllegalArgumentException("partition groups must not be empty");
            }
            for (String nodeId : group) {
                validateNodeId(nodeId);
                if (!seenNodeIds.add(nodeId)) {
                    throw new IllegalArgumentException("node cannot appear in multiple partition groups");
                }
            }
        }
    }

    private static List<Set<String>> copyGroups(List<Set<String>> groups) {
        List<Set<String>> copies = new ArrayList<>();
        for (Set<String> group : groups) {
            copies.add(new LinkedHashSet<>(group));
        }
        return copies;
    }

    private static Map<String, Integer> indexGroups(List<Set<String>> groups) {
        Map<String, Integer> index = new LinkedHashMap<>();
        for (int groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
            for (String nodeId : groups.get(groupIndex)) {
                index.put(nodeId, groupIndex);
            }
        }
        return index;
    }

    private static void validateNodeId(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
    }
}
