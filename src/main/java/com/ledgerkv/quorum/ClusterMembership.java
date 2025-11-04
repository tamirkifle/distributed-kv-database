package com.ledgerkv.quorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class ClusterMembership {
    private final String clusterId;
    private final List<ClusterNode> nodes;
    private final int replicationFactor;
    private final HashRing ring;
    private final Map<String, ClusterNode> nodesById;

    private ClusterMembership(String clusterId, List<ClusterNode> nodes, int replicationFactor) {
        this.clusterId = clusterId;
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.replicationFactor = replicationFactor;
        this.nodesById = new LinkedHashMap<>();
        HashRing builtRing = new HashRing();
        for (ClusterNode node : this.nodes) {
            this.nodesById.put(node.getId(), node);
            builtRing.addNode(node.getId());
        }
        this.ring = builtRing;
    }

    public static ClusterMembership create(String clusterId, int nodeCount, int replicationFactor) {
        validate(clusterId, nodeCount, replicationFactor);

        List<ClusterNode> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(new ClusterNode(clusterId + "-node-" + i, i));
        }

        return new ClusterMembership(clusterId, nodes, replicationFactor);
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<ClusterNode> getNodes() {
        return nodes;
    }

    public List<String> getNodeIds() {
        return nodes.stream()
            .map(ClusterNode::getId)
            .collect(Collectors.toUnmodifiableList());
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public List<ClusterNode> getPreferenceList(String key, int n) {
        List<ClusterNode> replicas = new ArrayList<>();
        for (String nodeId : ring.getPreferenceList(key, n)) {
            replicas.add(nodesById.get(nodeId));
        }
        return Collections.unmodifiableList(replicas);
    }

    private static void validate(String clusterId, int nodeCount, int replicationFactor) {
        if (clusterId == null || clusterId.trim().isEmpty()) {
            throw new IllegalArgumentException("cluster id must not be blank");
        }
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("node count must be positive");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replication factor must be positive");
        }
        if (replicationFactor > nodeCount) {
            throw new IllegalArgumentException("replication factor cannot exceed node count");
        }
    }
}
