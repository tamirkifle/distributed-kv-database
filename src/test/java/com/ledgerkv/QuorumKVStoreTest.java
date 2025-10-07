package com.ledgerkv;

import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating quorum consistency and availability trade-offs.
 * Shows how Netflix survived AWS AZ failure with Cassandra quorum.
 */
class QuorumKVStoreTest {
    
    private QuorumKVStore cluster;
    
    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }
    
    @Test
    @DisplayName("W+R>N guarantees strong consistency")
    void testQuorumConsistencyGuarantee() {
        // Setup: N=5, W=3, R=3 (W+R=6 > N=5)
        QuorumConfig config = QuorumConfig.STRONG_CONSISTENCY;
        cluster = new QuorumKVStore("test-cluster", config);
        
        // Write value
        QuorumResponse writeResp = cluster.write("key1", "value1");
        assertTrue(writeResp.isSuccessful(), "Write should succeed with 3 nodes");
        
        // Read should always see latest value
        QuorumResponse readResp = cluster.read("key1");
        assertTrue(readResp.isSuccessful());
        assertEquals("value1", readResp.getValue().getValue());
        
        // This works because:
        // W=3 nodes have the write
        // R=3 nodes are read
        // At least 1 node must be in both sets (3+3 > 5)
        // That overlapping node has the latest value!
        
        System.out.println("Strong consistency proven: W+R>N guarantees latest value");
        System.out.printf("Config: %s%n", config);
    }
    
    @Test
    @DisplayName("W+R<=N allows stale reads but higher availability")
    void testEventualConsistency() {
        // Setup: N=5, W=1, R=1 (W+R=2 <= N=5)
        QuorumConfig config = QuorumConfig.EVENTUAL_CONSISTENCY;
        cluster = new QuorumKVStore("test-cluster", config);
        
        // Write with W=1 (only 1 node needs to acknowledge)
        QuorumResponse writeResp = cluster.write("key1", "value1");
        assertTrue(writeResp.isSuccessful());
        
        // Read might get node that hasn't seen write yet
        // In production, this causes "eventual consistency"
        
        System.out.println("Eventual consistency: Fast but may see stale data");
        System.out.printf("Config: %s%n", config);
        System.out.println("Amazon shopping cart uses this - deleted items might reappear!");
    }
    
    @Test
    @DisplayName("Survive node failures with quorum")
    void testNodeFailureTolerance() {
        // Setup: N=5, W=3, R=3
        QuorumConfig config = new QuorumConfig(5, 3, 3);
        cluster = new QuorumKVStore("test-cluster", config);
        
        // Can tolerate N-W = 2 failures for writes
        // Can tolerate N-R = 2 failures for reads
        
        // Simulate exactly 2 nodes failing
        cluster.failNodes(2);  
        
        // Should still work with 3 nodes available
        QuorumResponse writeResp = cluster.write("key1", "value1");
        assertTrue(writeResp.isSuccessful(), "Should survive 2 node failures");
        
        QuorumResponse readResp = cluster.read("key1");
        assertTrue(readResp.isSuccessful(), "Should read with 2 nodes down");
        
        // But if 3 nodes fail, operations fail
        cluster.failNodes(3);
        
        writeResp = cluster.write("key2", "value2");
        assertFalse(writeResp.isSuccessful(), "Should fail with 3 nodes down");
        
        System.out.printf("With N=5, W=3: Can survive %d node failures%n", 
            config.getWriteFailureTolerance());
        System.out.println("This is how Netflix survived AWS AZ failure!");
    }
    
    @Test
    @DisplayName("Demonstrate Netflix's 2011 AWS survival")
    void testNetflixAWSFailureScenario() {
        // Netflix's actual Cassandra config during 2011 AWS outage
        // N=3 (across 3 availability zones)
        // W=2, R=2 (quorum consistency)
        QuorumConfig config = new QuorumConfig(3, 2, 2);
        cluster = new QuorumKVStore("netflix-cluster", config);
        
        System.out.println("=== Netflix 2011 AWS Outage Simulation ===");
        
        // Normal operation
        QuorumResponse write = cluster.write("movie:1", "Inception");
        assertTrue(write.isSuccessful());
        System.out.println("Normal: Write succeeded across 3 AZs");
        
        // AWS us-east-1 fails (1 of 3 AZs down)
        cluster.simulatePartition(Set.of(0));  // First node unreachable
        
        // Can still write with W=2 (need 2 of remaining 2 nodes)
        write = cluster.write("movie:2", "Avatar");
        assertTrue(write.isSuccessful(), "Should work with 1 AZ down");
        System.out.println("During outage: Still writing with 2 of 3 AZs");
        
        // Can still read with R=2
        QuorumResponse read = cluster.read("movie:1");
        assertTrue(read.isSuccessful(), "Should read with 1 AZ down");
        System.out.println("During outage: Still reading with 2 of 3 AZs");
        
        // Competitors using traditional leader-follower went down
        // Netflix stayed up because quorum doesn't need all nodes!
        
        cluster.healPartition();
        System.out.println("After recovery: All 3 AZs back online");
        System.out.println("\nLesson: Quorum systems survive partial failures!");
    }
    
    @Test
    @DisplayName("Compare consistency levels like Cassandra")
    void testCassandraConsistencyLevels() {
        System.out.println("=== Cassandra Consistency Levels ===\n");
        
        // ONE - highest availability, weakest consistency guarantee
        QuorumConfig one = QuorumConfig.ProductionExamples.CASSANDRA_ONE;
        assertEquals(1, one.getW());
        assertEquals(1, one.getR());
        assertFalse(one.isStronglyConsistent(), "ONE should not guarantee W+R>N consistency");
        assertEquals(2, one.getWriteFailureTolerance(), "ONE should tolerate two write failures in N=3");
        assertEquals(2, one.getReadFailureTolerance(), "ONE should tolerate two read failures in N=3");

        cluster = new QuorumKVStore("cassandra", one);
        cluster.failNodes(2);
        assertTrue(cluster.write("one-key", "one-value").isSuccessful(),
            "ONE should write with two failed replicas because W=1");
        assertTrue(cluster.read("one-key").isSuccessful(),
            "ONE should read with two failed replicas because R=1");
        cluster.failNodes(3);
        assertFalse(cluster.write("one-key-failed", "one-value").isSuccessful(),
            "ONE should fail writes when no replicas are reachable");
        cluster.shutdown();
        
        // QUORUM - balanced availability and consistency
        QuorumConfig quorum = QuorumConfig.ProductionExamples.CASSANDRA_QUORUM;
        assertEquals(2, quorum.getW());
        assertEquals(2, quorum.getR());
        assertTrue(quorum.isStronglyConsistent(), "QUORUM should guarantee W+R>N consistency");
        assertEquals(1, quorum.getWriteFailureTolerance(), "QUORUM should tolerate one write failure in N=3");
        assertEquals(1, quorum.getReadFailureTolerance(), "QUORUM should tolerate one read failure in N=3");

        cluster = new QuorumKVStore("cassandra", quorum);
        cluster.failNodes(1);
        assertTrue(cluster.write("quorum-key", "quorum-value").isSuccessful(),
            "QUORUM should write with one failed replica because W=2");
        assertTrue(cluster.read("quorum-key").isSuccessful(),
            "QUORUM should read with one failed replica because R=2");
        cluster.failNodes(2);
        assertFalse(cluster.write("quorum-key-failed", "quorum-value").isSuccessful(),
            "QUORUM should fail writes when only one replica is reachable");
        cluster.shutdown();
        
        // ALL - lowest availability, strongest acknowledgement requirement
        QuorumConfig all = QuorumConfig.ProductionExamples.CASSANDRA_ALL;
        assertEquals(3, all.getW());
        assertEquals(3, all.getR());
        assertTrue(all.isStronglyConsistent(), "ALL should guarantee W+R>N consistency");
        assertEquals(0, all.getWriteFailureTolerance(), "ALL should not tolerate write failures in N=3");
        assertEquals(0, all.getReadFailureTolerance(), "ALL should not tolerate read failures in N=3");

        cluster = new QuorumKVStore("cassandra", all);
        assertTrue(cluster.write("all-key", "all-value").isSuccessful(),
            "ALL should write when every replica is reachable");
        assertTrue(cluster.read("all-key").isSuccessful(),
            "ALL should read when every replica is reachable");
        cluster.failNodes(1);
        assertFalse(cluster.write("all-key-failed", "all-value").isSuccessful(),
            "ALL should fail writes when any replica is unreachable");
        
        // Analysis
        System.out.println("\nTrade-offs:");
        System.out.println("ONE: Most available, but W+R<=N allows stale reads");
        System.out.println("QUORUM: Balanced - W+R>N with one failure tolerated");
        System.out.println("ALL: Requires every replica, so any failure blocks writes and reads");
    }
    
    @Test
    @DisplayName("Amazon's shopping cart vs order placement")
    void testAmazonUseCases() {
        System.out.println("=== Amazon's Different Consistency Requirements ===\n");
        
        // Shopping cart: Availability >> Consistency
        QuorumConfig cart = QuorumConfig.ProductionExamples.AMAZON_CART;
        cluster = new QuorumKVStore("cart-cluster", cart);
        
        System.out.printf("Shopping Cart: %s%n", cart);
        System.out.println("- W=1, R=1: Always available");
        System.out.println("- Trade-off: Deleted items might reappear");
        System.out.println("- Business decision: Better to show extra item than lose sale\n");
        
        // Simulate cart operations
        cluster.write("cart:user1", "iPhone,iPad");
        QuorumResponse cartRead = cluster.read("cart:user1");
        assertTrue(cartRead.isSuccessful());
        cluster.shutdown();
        
        // Order placement: Consistency >> Availability  
        QuorumConfig orders = QuorumConfig.ProductionExamples.AMAZON_ORDERS;
        cluster = new QuorumKVStore("order-cluster", orders);
        
        System.out.printf("Order Placement: %s%n", orders);
        System.out.println("- W=3, R=3: Strong consistency");
        System.out.println("- Trade-off: Might fail during outages");
        System.out.println("- Business decision: Never double-charge customer");
        
        // Order must be consistent
        QuorumResponse orderWrite = cluster.write("order:12345", "$999.99");
        assertTrue(orderWrite.isSuccessful());
        
        // Even with failures, never see wrong order amount
        cluster.setFailureRate(0.2);
        QuorumResponse orderRead = cluster.read("order:12345");
        if (orderRead.isSuccessful()) {
            assertEquals("$999.99", orderRead.getValue().getValue());
        }
    }
    
    @Test
    @DisplayName("Detect inconsistency between nodes")
    void testInconsistencyDetection() {
        QuorumConfig config = new QuorumConfig(5, 1, 5);  // W=1, R=5
        cluster = new QuorumKVStore("test-cluster", config);
        cluster.setNetworkDelayMs(1);  // Fast for testing
        
        // Directly write to only one node to ensure inconsistency
        cluster.getNodes().get(0).set("key1", "value1");
        
        // Verify setup: only node 0 has value, others don't
        assertTrue(cluster.getNodes().get(0).get("key1").isPresent(), "Node 0 should have value");
        assertFalse(cluster.getNodes().get(1).get("key1").isPresent(), "Node 1 should not have value");
        assertFalse(cluster.getNodes().get(2).get("key1").isPresent(), "Node 2 should not have value");
        assertFalse(cluster.getNodes().get(3).get("key1").isPresent(), "Node 3 should not have value");
        assertFalse(cluster.getNodes().get(4).get("key1").isPresent(), "Node 4 should not have value");
        
        // Now read with R=5 (read from all nodes)
        // Node 0 returns value, nodes 1-4 return null
        QuorumResponse response = cluster.read("key1");
        
        // Should successfully read (got 5 responses)
        assertTrue(response.isSuccessful(), "Should get 5 responses");
        
        // Debug output
        System.out.printf("Read response: collected %d values from %d nodes%n", 
            response.getAllValues().size(), response.getRespondingNodes());
        
        // Count nulls and non-nulls
        long nullCount = response.getAllValues().stream().filter(Objects::isNull).count();
        long nonNullCount = response.getAllValues().stream().filter(Objects::nonNull).count();
        System.out.printf("Values breakdown: %d nulls, %d non-nulls%n", nullCount, nonNullCount);
        
        // Should detect inconsistency (1 node has value, 4 don't)
        assertTrue(response.hasInconsistency(), 
            "Should detect inconsistency when nodes have different values (1 with data, 4 without)");
        
        int staleNodes = response.getStaleNodeCount();
        System.out.printf("Detected %d stale/empty nodes out of %d total%n", 
            staleNodes, config.getR());
        
        assertEquals(4, staleNodes, "Should have 4 nodes without data");
        
        // This is why Cassandra does read repair!
        System.out.println("In production: This triggers read repair to sync all nodes");
    }
    
    @Test
    @DisplayName("Concurrent writes with quorum")
    void testConcurrentWrites() throws InterruptedException {
        QuorumConfig config = new QuorumConfig(5, 3, 3);
        cluster = new QuorumKVStore("test-cluster", config);
        
        int numThreads = 10;
        int writesPerThread = 10;
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        
        // Many concurrent writes
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                for (int i = 0; i < writesPerThread; i++) {
                    QuorumResponse resp = cluster.write(
                        "key-" + threadId + "-" + i, 
                        "value-" + threadId + "-" + i
                    );
                    if (resp.isSuccessful()) {
                        successCount.incrementAndGet();
                    }
                }
                latch.countDown();
            }).start();
        }
        
        latch.await();
        
        System.out.printf("Concurrent writes: %d/%d succeeded%n",
            successCount.get(), numThreads * writesPerThread);
        
        // All should succeed with healthy cluster
        assertEquals(numThreads * writesPerThread, successCount.get());
        
        // Check final statistics
        System.out.println(cluster.getStats());
    }
    
    @Test
    @DisplayName("Latency comparison for different quorum sizes")
    void testLatencyImpact() {
        System.out.println("=== Latency Impact of Quorum Size ===\n");
        
        // Test different W values
        for (int w = 1; w <= 5; w++) {
            QuorumConfig config = new QuorumConfig(5, w, 1);
            cluster = new QuorumKVStore("test-cluster", config);
            cluster.setNetworkDelayMs(10);  // 10ms network delay
            
            long totalLatency = 0;
            int operations = 20;
            
            for (int i = 0; i < operations; i++) {
                QuorumResponse resp = cluster.write("key" + i, "value" + i);
                totalLatency += resp.getLatencyMs();
            }
            
            long avgLatency = totalLatency / operations;
            System.out.printf("W=%d: Average latency %d ms%n", w, avgLatency);
            
            cluster.shutdown();
        }
        
        System.out.println("\nObservation: Latency increases with quorum size");
        System.out.println("This is why MongoDB defaults to W=1 for non-critical data");
    }
}
