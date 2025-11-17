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
}
