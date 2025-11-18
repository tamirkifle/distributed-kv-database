package com.ledgerkv.quorum;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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

    /**
     * Hand-rolled 64-bit hash: FNV-1a over the UTF-8 bytes of {@code value}, followed by a
     * SplitMix64-style finalizer. FNV-1a alone has weak avalanche on near-identical inputs
     * (e.g. {@code node-0#0}, {@code node-0#1}), which clusters virtual nodes and skews the ring;
     * the finalizer mixes the bits so vnodes and keys spread evenly. No external dependency.
     */
    public static long hash64(String value) {
        long hash = FNV_OFFSET_BASIS;
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xffL);
            hash *= FNV_PRIME;
        }
        // SplitMix64 / MurmurHash3 fmix64 finalizer for strong avalanche.
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);
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

    /**
     * The {@code n} distinct physical nodes reached by walking the ring clockwise from the key's
     * hash position, wrapping past the top of the ring. This is the key's replica preference list.
     */
    public List<String> getPreferenceList(String key, int n) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (n <= 0) {
            throw new IllegalArgumentException("preference count must be positive");
        }
        if (n > physicalNodes.size()) {
            throw new IllegalArgumentException(
                "preference count " + n + " exceeds node count " + physicalNodes.size());
        }

        long keyHash = hash64(key);
        LinkedHashSet<String> preferred = new LinkedHashSet<>();
        // Clockwise from the key: hashes >= keyHash, then wrap to the smallest hashes.
        for (String nodeId : ring.tailMap(keyHash, true).values()) {
            preferred.add(nodeId);
            if (preferred.size() == n) {
                return new ArrayList<>(preferred);
            }
        }
        for (String nodeId : ring.headMap(keyHash, false).values()) {
            preferred.add(nodeId);
            if (preferred.size() == n) {
                return new ArrayList<>(preferred);
            }
        }
        return new ArrayList<>(preferred); // n == physicalNodes.size(): every node collected
    }
}
