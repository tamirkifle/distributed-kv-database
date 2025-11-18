package com.ledgerkv.quorum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HashRingTest {

    @Test
    void hash64IsDeterministicAndDependsOnInput() {
        assertEquals(HashRing.hash64("alpha"), HashRing.hash64("alpha"));
        assertEquals(HashRing.hash64("trace:run-001#0"), HashRing.hash64("trace:run-001#0"));
        assertTrue(HashRing.hash64("alpha") != HashRing.hash64("beta"),
            "different inputs should (almost always) hash differently");
    }

    @Test
    void defaultRingUses150VirtualNodes() {
        assertEquals(150, HashRing.DEFAULT_VIRTUAL_NODES);
        assertEquals(150, new HashRing().virtualNodesPerNode());
    }

    @Test
    void addNodeRegistersPhysicalNodeAndIsIdempotent() {
        HashRing ring = new HashRing(8);
        ring.addNode("node-0");
        ring.addNode("node-0"); // idempotent
        ring.addNode("node-1");

        assertEquals(java.util.Set.of("node-0", "node-1"), ring.nodes());
    }

    @Test
    void removeNodeDropsPhysicalNodeAndIsIdempotent() {
        HashRing ring = new HashRing(8);
        ring.addNode("node-0");
        ring.addNode("node-1");

        ring.removeNode("node-1");
        ring.removeNode("node-1"); // idempotent, no throw

        assertEquals(java.util.Set.of("node-0"), ring.nodes());
        assertFalse(ring.nodes().contains("node-1"));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new HashRing(0));
        assertThrows(IllegalArgumentException.class, () -> new HashRing(-1));
        assertThrows(IllegalArgumentException.class, () -> new HashRing(8).addNode(""));
        assertThrows(IllegalArgumentException.class, () -> new HashRing(8).addNode("   "));
    }

    @Test
    void preferenceListReturnsRequestedNumberOfDistinctNodes() {
        HashRing ring = new HashRing(); // K=150
        for (int i = 0; i < 5; i++) {
            ring.addNode("node-" + i);
        }

        java.util.List<String> prefs = ring.getPreferenceList("trace:run-001", 3);

        assertEquals(3, prefs.size());
        assertEquals(3, new java.util.HashSet<>(prefs).size(), "nodes must be distinct");
        assertTrue(ring.nodes().containsAll(prefs));
    }

    @Test
    void preferenceListIsDeterministicForAKey() {
        HashRing ring = new HashRing();
        for (int i = 0; i < 5; i++) {
            ring.addNode("node-" + i);
        }

        assertEquals(ring.getPreferenceList("trace:run-001", 3),
            ring.getPreferenceList("trace:run-001", 3));
    }

    @Test
    void preferenceListWrapsAroundRingWithoutDuplicates() {
        HashRing ring = new HashRing();
        ring.addNode("node-0");
        ring.addNode("node-1");
        ring.addNode("node-2");

        // n == node count: must return every node exactly once regardless of key position.
        for (String key : new String[] {"a", "key-near-end-of-ring", "zzzzzz", "trace:9"}) {
            java.util.List<String> prefs = ring.getPreferenceList(key, 3);
            assertEquals(3, prefs.size());
            assertEquals(java.util.Set.of("node-0", "node-1", "node-2"),
                new java.util.HashSet<>(prefs), "key=" + key);
        }
    }

    @Test
    void preferenceListRejectsInvalidArguments() {
        HashRing ring = new HashRing();
        ring.addNode("node-0");
        ring.addNode("node-1");

        assertThrows(IllegalArgumentException.class, () -> ring.getPreferenceList("", 1));
        assertThrows(IllegalArgumentException.class, () -> ring.getPreferenceList("k", 0));
        assertThrows(IllegalArgumentException.class, () -> ring.getPreferenceList("k", 3)); // > 2 nodes
    }

    @Test
    void distributesKeysAcrossAllNodesRoughlyEvenly() {
        HashRing ring = new HashRing(); // K=150 → good spread
        int nodeCount = 5;
        for (int i = 0; i < nodeCount; i++) {
            ring.addNode("node-" + i);
        }

        java.util.Map<String, Integer> primaryCounts = new java.util.HashMap<>();
        int sample = 20_000;
        for (int i = 0; i < sample; i++) {
            String owner = ring.getPreferenceList("key-" + i, 1).get(0);
            primaryCounts.merge(owner, 1, Integer::sum);
        }

        assertEquals(nodeCount, primaryCounts.size(), "every node should own some keys");
        double ideal = (double) sample / nodeCount; // 4000
        for (int count : primaryCounts.values()) {
            // K=150 vnodes keeps each node within ~35% of the ideal share.
            assertTrue(count > ideal * 0.65 && count < ideal * 1.35,
                "node share " + count + " should be near ideal " + ideal);
        }
    }

    @Test
    void addingANodeMovesAboutOneOverMPlusOneKeysAndOnlyToTheNewNode() {
        int initialNodes = 10;
        HashRing ring = new HashRing(); // K=150
        for (int i = 0; i < initialNodes; i++) {
            ring.addNode("node-" + i);
        }

        int sample = 50_000;
        java.util.Map<String, String> ownerBefore = new java.util.HashMap<>();
        for (int i = 0; i < sample; i++) {
            String key = "rebalance-key-" + i;
            ownerBefore.put(key, ring.getPreferenceList(key, 1).get(0));
        }

        String newNode = "node-" + initialNodes; // "node-10"
        ring.addNode(newNode);

        int moved = 0;
        int movedToExistingNode = 0;
        for (java.util.Map.Entry<String, String> e : ownerBefore.entrySet()) {
            String ownerAfter = ring.getPreferenceList(e.getKey(), 1).get(0);
            if (!ownerAfter.equals(e.getValue())) {
                moved++;
                if (!ownerAfter.equals(newNode)) {
                    movedToExistingNode++;
                }
            }
        }

        // (a) Existing nodes never reshuffle keys among themselves: every moved key went to the new node.
        assertEquals(0, movedToExistingNode,
            "keys that moved must all land on the newly added node");

        // (b) The moved fraction is ~1/(M+1) = 1/11 ≈ 0.0909. Allow a generous band for vnode variance.
        double movedFraction = (double) moved / sample;
        double ideal = 1.0 / (initialNodes + 1);
        assertTrue(movedFraction > ideal * 0.5 && movedFraction < ideal * 1.5,
            "moved fraction " + movedFraction + " should be near ideal " + ideal);
    }
}
