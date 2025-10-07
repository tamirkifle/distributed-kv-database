package com.ledgerkv;

import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.ledgerkv.consistency.VersionMetadata;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating read repair in action.
 * Shows how Cassandra, Discord, and Reddit heal their clusters.
 */
class ReadRepairTest {
    
    private QuorumKVStoreWithRepair cluster;
    
    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }
    
    @Test
    @DisplayName("Read repair preserves vector-clock metadata")
    void readRepairPreservesConflictAwareMetadata() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(3, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.ALWAYS);

        VersionMetadata metadata = VersionMetadata.initial("node-a").increment("node-a");
        VersionedValue sourceValue = new VersionedValue("value-v2", 7, metadata);
        cluster.getNodes().get(0).putVersionedValue("key1", sourceValue);

        QuorumResponse response = cluster.read("key1");

        assertTrue(response.isSuccessful());
        assertFalse(response.hasConflicts(), "Dominating value should not be reported as a conflict");
        waitForRepairs();

        for (int node = 0; node < 3; node++) {
            VersionedValue repairedValue = cluster.getNodes().get(node).get("key1").orElseThrow();
            assertEquals("value-v2", repairedValue.getValue());
            assertEquals(7, repairedValue.getVersion());
            assertEquals(metadata, repairedValue.getVersionMetadata());
        }
    }

    @Test
    @DisplayName("Read repair does not collapse unresolved conflicts")
    void readRepairPreservesUnresolvedConflicts() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(3, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.ALWAYS);

        VersionMetadata nodeAMetadata = VersionMetadata.initial("node-a");
        VersionMetadata nodeBMetadata = VersionMetadata.initial("node-b");
        cluster.getNodes().get(0).putVersionedValue("key1",
            new VersionedValue("value-a", 1, nodeAMetadata));
        cluster.getNodes().get(1).putVersionedValue("key1",
            new VersionedValue("value-b", 1, nodeBMetadata));

        QuorumResponse response = cluster.read("key1");

        assertTrue(response.isSuccessful());
        assertTrue(response.hasConflicts(), "Concurrent vector-clock values should remain siblings");
        waitForRepairs();

        assertEquals("value-a", cluster.getNodes().get(0).get("key1").orElseThrow().getValue());
        assertEquals(nodeAMetadata, cluster.getNodes().get(0).get("key1").orElseThrow().getVersionMetadata());
        assertEquals("value-b", cluster.getNodes().get(1).get("key1").orElseThrow().getValue());
        assertEquals(nodeBMetadata, cluster.getNodes().get(1).get("key1").orElseThrow().getVersionMetadata());
        assertTrue(cluster.getNodes().get(2).get("key1").isEmpty(),
            "Read repair should not invent a single value for an unresolved conflict");
        assertEquals(0, cluster.getRepairsTriggered(),
            "Unresolved conflicts should not be counted as repair work");
        assertEquals(0, cluster.getNodesRepaired(),
            "Unresolved conflicts should not update replicas");
    }

    @Test
    @DisplayName("Read repair heals inconsistent nodes")
    void testBasicReadRepair() throws InterruptedException {
        // Setup: N=5, W=1, R=3 (allows inconsistency)
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);  // Fast for testing
        
        // Create inconsistency: write directly to one node
        cluster.getNodes().get(0).set("key1", "value1");
        
        // Verify inconsistency exists
        assertTrue(cluster.getNodes().get(0).get("key1").isPresent());
        assertFalse(cluster.getNodes().get(1).get("key1").isPresent());
        assertFalse(cluster.getNodes().get(2).get("key1").isPresent());
        
        System.out.println("Initial state: 1 node has data, 4 nodes empty");
        
        // Read triggers repair
        cluster.setRepairStrategy(ReadRepairStrategy.ALWAYS);
        QuorumResponse response = cluster.read("key1");
        
        assertTrue(response.isSuccessful());
        assertTrue(response.hasInconsistency(), "Should detect inconsistency");
        
        // Wait for async repair to complete
        Thread.sleep(100);
        
        // Verify nodes are now consistent
        int consistentNodes = 0;
        for (int i = 0; i < 5; i++) {
            if (cluster.getNodes().get(i).get("key1").isPresent()) {
                consistentNodes++;
            }
        }
        
        System.out.printf("After read repair: %d/5 nodes have data%n", consistentNodes);
        assertTrue(consistentNodes > 1, "Should have repaired at least some nodes");
        
        // Check metrics
        System.out.println(cluster.getRepairStats());
        assertEquals(1, cluster.getRepairsTriggered(), "Should have triggered 1 repair");
    }
    
    @Test
    @DisplayName("Probabilistic repair reduces thundering herd")
    void testProbabilisticRepair() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        
        // Create inconsistency
        cluster.getNodes().get(0).set("hot-key", "popular-data");
        
        // Set probabilistic repair (10% chance)
        cluster.setRepairStrategy(ReadRepairStrategy.PROBABILISTIC);
        cluster.setRepairProbability(0.1);
        
        // Simulate 100 reads of same key (thundering herd scenario)
        int reads = 100;
        for (int i = 0; i < reads; i++) {
            cluster.read("hot-key");
        }
        
        Thread.sleep(200);  // Wait for repairs
        
        long repairs = cluster.getRepairsTriggered();
        System.out.printf("100 reads triggered %d repairs (expected ~10)%n", repairs);
        
        // Should trigger roughly 10% of reads (with some variance)
        assertTrue(repairs > 0, "Should trigger some repairs");
        assertTrue(repairs < 30, "Should not trigger too many repairs");
        
        // This is Uber's solution to thundering herd
        System.out.println("Uber's insight: Don't repair on every read!");
    }
    
    @Test
    @DisplayName("Time-based repair avoids repairing fresh writes")
    void testTimeBasedRepair() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        
        // Set time-based repair (only repair if > 100ms old)
        cluster.setRepairStrategy(ReadRepairStrategy.TIME_BASED);
        cluster.setRepairThresholdMs(100);
        
        // Fresh write (inconsistent but new)
        cluster.getNodes().get(0).set("fresh-key", "new-data");
        
        // Immediate read - should NOT trigger repair (too fresh)
        cluster.read("fresh-key");
        Thread.sleep(50);
        
        assertEquals(0, cluster.getRepairsTriggered(), 
            "Should not repair fresh data");
        
        // Wait for data to age
        Thread.sleep(150);
        
        // Now read should trigger repair (data is old enough)
        cluster.read("fresh-key");
        Thread.sleep(50);
        
        assertEquals(1, cluster.getRepairsTriggered(), 
            "Should repair aged data");
        
        System.out.println("LinkedIn's approach: Don't repair data still propagating");
    }
    
    @Test
    @DisplayName("Read repair improves consistency over time")
    void testConsistencyConvergence() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.ALWAYS);
        
        // Create many inconsistent keys
        for (int i = 0; i < 10; i++) {
            // Each key only on one random node
            int node = i % 5;
            cluster.getNodes().get(node).set("key" + i, "value" + i);
        }
        
        System.out.println("Initial: Each key on only 1 of 5 nodes");
        
        // Measure initial consistency
        int initialConsistentKeys = countFullyConsistentKeys(10);
        System.out.printf("Initially consistent keys: %d/10%n", initialConsistentKeys);
        
        // Perform reads to trigger repairs
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 10; i++) {
                cluster.read("key" + i);
            }
            Thread.sleep(100);  // Let repairs complete
            
            int consistentKeys = countFullyConsistentKeys(10);
            System.out.printf("After round %d: %d/10 keys consistent%n", 
                round + 1, consistentKeys);
        }
        
        // Final check
        int finalConsistentKeys = countFullyConsistentKeys(10);
        assertTrue(finalConsistentKeys > initialConsistentKeys,
            "Consistency should improve over time");
        
        System.out.println("\nThis is how Discord messages eventually appear correctly!");
    }
    
    @Test
    @DisplayName("Demonstrate thundering herd problem")
    void testThunderingHerdProblem() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        
        // Run built-in demonstration
        cluster.demonstrateThunderingHerd();
        
        // Verify the difference in repair counts
        assertTrue(cluster.getRepairsTriggered() > 0, 
            "Should have triggered repairs");
        
        System.out.println("\nUber's production incident:");
        System.out.println("- Popular key becomes inconsistent");
        System.out.println("- Thousands of reads trigger repairs");
        System.out.println("- Repair writes overload the cluster");
        System.out.println("- Solution: Probabilistic repair!");
    }
    
    @Test
    @DisplayName("Compare repair strategies performance")
    void testRepairStrategyComparison() throws InterruptedException {
        System.out.println("=== Repair Strategy Comparison ===\n");
        
        // Test each strategy
        for (ReadRepairStrategy strategy : ReadRepairStrategy.values()) {
            QuorumConfig config = new QuorumConfig(5, 1, 3);
            cluster = new QuorumKVStoreWithRepair("test-cluster", config);
            cluster.setNetworkDelayMs(1);
            cluster.setRepairStrategy(strategy);
            
            if (strategy == ReadRepairStrategy.PROBABILISTIC) {
                cluster.setRepairProbability(0.1);
            }
            
            // Create inconsistency
            cluster.getNodes().get(0).set("test-key", "test-value");
            
            // Perform 50 reads
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 50; i++) {
                cluster.read("test-key");
            }
            Thread.sleep(100);
            
            long duration = System.currentTimeMillis() - startTime;
            long repairs = cluster.getRepairsTriggered();
            
            System.out.printf("%s: %d repairs, %d ms%n", 
                strategy, repairs, duration);
            
            cluster.shutdown();
        }
        
        System.out.println("\nProduction choices:");
        System.out.println("- Cassandra: PROBABILISTIC (default 10%)");
        System.out.println("- DynamoDB: NONE (uses anti-entropy)");
        System.out.println("- Riak: ALWAYS (with rate limiting)");
    }
    
    @Test
    @DisplayName("Force repair fixes all inconsistencies")
    void testForceRepair() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        
        // Create severe inconsistency
        cluster.getNodes().get(0).set("key1", "value-A");
        cluster.getNodes().get(1).set("key1", "value-B");
        cluster.getNodes().get(2).set("key1", "value-C");
        // Nodes 3 and 4 have nothing
        
        System.out.println("Severe inconsistency: 3 different values!");
        
        // Force repair
        cluster.forceRepair("key1");
        Thread.sleep(200);
        
        // Check consistency
        Set<String> values = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Optional<VersionedValue> nodeValue = cluster.getNodes().get(i).get("key1");
            if (nodeValue.isPresent()) {
                values.add(nodeValue.get().getValue());
            }
        }
        
        System.out.printf("After force repair: %d unique values%n", values.size());
        assertEquals(1, values.size(), "Should converge to single value");
        
        System.out.println("Manual repair command useful for fixing known issues");
    }
    
    @Test
    @DisplayName("Reddit vote count convergence scenario")
    void testRedditVoteCountScenario() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 1, 3);
        cluster = new QuorumKVStoreWithRepair("test-cluster", config);
        cluster.setNetworkDelayMs(1);
        cluster.setRepairStrategy(ReadRepairStrategy.PROBABILISTIC);
        cluster.setRepairProbability(0.2);  // 20% repair rate
        
        // Initial vote count inconsistent across nodes
        cluster.getNodes().get(0).set("post:123:votes", "1500");
        cluster.getNodes().get(1).set("post:123:votes", "1498");
        cluster.getNodes().get(2).set("post:123:votes", "1502");
        
        System.out.println("Reddit scenario: Vote counts slightly different");
        
        // Multiple users viewing the post
        List<String> seenCounts = new ArrayList<>();
        for (int user = 0; user < 20; user++) {
            QuorumResponse response = cluster.read("post:123:votes");
            if (response.isSuccessful() && response.getValue() != null) {
                seenCounts.add(response.getValue().getValue());
            }
            Thread.sleep(10);
        }
        
        // Count unique values seen
        Set<String> uniqueCounts = new HashSet<>(seenCounts);
        System.out.printf("Users saw %d different vote counts%n", uniqueCounts.size());
        
        // After repairs, should converge
        Thread.sleep(200);
        
        Set<String> finalValues = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Optional<VersionedValue> nodeValue = cluster.getNodes().get(i).get("post:123:votes");
            if (nodeValue.isPresent()) {
                finalValues.add(nodeValue.get().getValue());
            }
        }
        
        System.out.printf("Final state: %d unique values across nodes%n", finalValues.size());
        System.out.println("Reddit uses eventual consistency - exact counts not critical");
    }
    
    // Helper method
    private int countFullyConsistentKeys(int numKeys) {
        int consistent = 0;
        
        for (int k = 0; k < numKeys; k++) {
            String key = "key" + k;
            Set<String> values = new HashSet<>();
            int nodesWithData = 0;
            
            for (int n = 0; n < 5; n++) {
                Optional<VersionedValue> nodeValue = cluster.getNodes().get(n).get(key);
                if (nodeValue.isPresent()) {
                    values.add(nodeValue.get().getValue());
                    nodesWithData++;
                }
            }
            
            // Consistent if all nodes have same value
            if (values.size() == 1 && nodesWithData == 5) {
                consistent++;
            }
        }
        
        return consistent;
    }

    private void waitForRepairs() throws InterruptedException {
        Thread.sleep(100);
    }
}
