package com.ledgerkv.quorum;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A 64-bit consistent-hash ring with virtual nodes. Each physical node is placed at K points
 * ("virtual nodes") on the ring by hashing {@code nodeId + "#" + i}. Key placement walks the ring
 * clockwise from the key's hash, collecting distinct physical nodes — so adding or removing a node
 * only moves the keys adjacent to that node's vnodes (~1/n of the keyspace), not the whole ring.
 *
 * <p>The hash is a hand-rolled 64-bit FNV-1a (no external dependency, and chosen over
 * {@link String#hashCode()} for far better distribution across the ring).
 */
public final class HashRing {
    /** Default virtual nodes per physical node. */
    public static final int DEFAULT_VIRTUAL_NODES = 150;

    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final int virtualNodes;
    private final NavigableMap<Long, String> ring = new TreeMap<>();
    private final Set<String> physicalNodes = new TreeSet<>();

    public HashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public HashRing(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtual node count must be positive");
        }
        this.virtualNodes = virtualNodes;
    }

    /** Hand-rolled 64-bit FNV-1a over the UTF-8 bytes of {@code value}. */
    public static long hash64(String value) {
        long hash = FNV_OFFSET_BASIS;
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xffL);
            hash *= FNV_PRIME;
        }
        return hash;
    }

    public void addNode(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        if (!physicalNodes.add(nodeId)) {
            return; // already present; ring already holds its vnodes
        }
        for (int i = 0; i < virtualNodes; i++) {
            ring.put(hash64(nodeId + "#" + i), nodeId);
        }
    }

    public void removeNode(String nodeId) {
        if (nodeId == null || !physicalNodes.remove(nodeId)) {
            return;
        }
        for (int i = 0; i < virtualNodes; i++) {
            ring.remove(hash64(nodeId + "#" + i));
        }
    }

    public Set<String> nodes() {
        return new TreeSet<>(physicalNodes);
    }

    public int virtualNodesPerNode() {
        return virtualNodes;
    }
}
