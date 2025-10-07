package com.ledgerkv.quorum;

import java.util.Objects;

public final class ClusterNode {
    private final String id;
    private final int index;

    public ClusterNode(String id, int index) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        if (index < 0) {
            throw new IllegalArgumentException("node index must be non-negative");
        }

        this.id = id;
        this.index = index;
    }

    public String getId() {
        return id;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ClusterNode)) {
            return false;
        }
        ClusterNode that = (ClusterNode) other;
        return index == that.index && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index);
    }

    @Override
    public String toString() {
        return String.format("ClusterNode{id='%s', index=%d}", id, index);
    }
}
