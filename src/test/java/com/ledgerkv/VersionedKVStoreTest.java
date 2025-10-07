package com.ledgerkv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating version-based conflict detection.
 * Shows why versions > timestamps for distributed systems.
 */
class VersionedKVStoreTest {
    
    private VersionedKVStore store;
    
    @BeforeEach
    void setUp() {
        store = new VersionedKVStore();
    }
    
    @Test
    @DisplayName("Versions increment monotonically per key")
    void testVersionIncrement() {
        long v1 = store.set("key1", "value1");
        assertEquals(1, v1);
        
        long v2 = store.set("key1", "value2");
        assertEquals(2, v2);
        
        long v3 = store.set("key1", "value3");
        assertEquals(3, v3);
        
        // Different key has independent version sequence
        long otherV1 = store.set("key2", "otherValue");
        assertEquals(1, otherV1);
    }
    
    @Test
    @DisplayName("Compare-and-set prevents lost updates")
    void testCompareAndSetPreventsLostUpdate() {
        // Initial value
        store.set("balance", "100");
        
        // Two concurrent transactions read the same version
        VersionedValue read1 = store.get("balance").orElseThrow();
        VersionedValue read2 = store.get("balance").orElseThrow();
        assertEquals(1, read1.getVersion());
        assertEquals(1, read2.getVersion());
        
        // Transaction 1: Add 50 (100 -> 150)
        boolean success1 = store.compareAndSet("balance", "150", read1.getVersion());
        assertTrue(success1, "First update should succeed");
        
        // Transaction 2: Subtract 30 (100 -> 70) 
        // This WOULD BE a lost update without version checking!
        boolean success2 = store.compareAndSet("balance", "70", read2.getVersion());
        assertFalse(success2, "Second update should fail - version mismatch!");
        
        // Final balance is 150, not 70 - Transaction 1's update preserved
        assertEquals("150", store.get("balance").orElseThrow().getValue());
    }
    
    @Test
    @DisplayName("Simulate distributed database read-modify-write conflict")
    void testReadModifyWriteConflict() throws InterruptedException {
        String productId = "iPhone-15";
        store.set(productId, "10");  // 10 items in stock
        
        AtomicInteger successfulPurchases = new AtomicInteger(0);
        AtomicInteger failedPurchases = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(20);
        
        // 20 concurrent customers trying to buy 1 item each
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                // Read current stock
                VersionedValue current = store.get(productId).orElseThrow();
                int stock = Integer.parseInt(current.getValue());
                
                if (stock > 0) {
                    // Try to decrease stock by 1
                    boolean success = store.compareAndSet(
                        productId, 
                        String.valueOf(stock - 1), 
                        current.getVersion()
                    );
                    
                    if (success) {
                        successfulPurchases.incrementAndGet();
                    } else {
                        failedPurchases.incrementAndGet();
                        // In real system: retry with fresh read
                    }
                }
                latch.countDown();
            }).start();
        }
        
        latch.await();
        
        // Without version checking, we might sell more than 10 items!
        // With version checking, at most 10 succeed
        assertTrue(successfulPurchases.get() <= 10, 
            "Should not sell more items than in stock");
        
        System.out.printf("Successful purchases: %d, Failed due to version conflict: %d%n",
            successfulPurchases.get(), failedPurchases.get());
    }
    
    @Test
    @DisplayName("Version vs Timestamp: The Instagram resurrection bug")
    void testVersionVsTimestamp() throws InterruptedException {
        String photoId = "photo123";
        
        // Scenario: User uploads photo
        long v1 = store.set(photoId, "cat.jpg");
        Thread.sleep(10);
        
        // User updates photo
        long v2 = store.set(photoId, "dog.jpg");
        Thread.sleep(10);
        
        // User deletes photo
        VersionedValue beforeDelete = store.get(photoId).orElseThrow();
        boolean deleted = store.compareAndDelete(photoId, beforeDelete.getVersion());
        assertTrue(deleted);
        
        // Now simulate Instagram's problem:
        // Old replica with clock skew tries to replicate old version
        
        // With timestamps (Instagram's old approach):
        // If replica's clock is ahead, old "cat.jpg" with future timestamp
        // could resurrect after delete!
        
        // With versions (our approach):
        // Old version 1 can never override version 2 or deletion
        boolean lateWrite = store.compareAndSet(photoId, "cat.jpg", v1);
        assertFalse(lateWrite, "Old version cannot resurrect!");
        
        // Photo stays deleted
        assertTrue(store.get(photoId).isEmpty(), "Photo should stay deleted");
    }
    
    @Test
    @DisplayName("Demonstrate clock skew immunity")
    void testClockSkewImmunity() {
        // Setup: Current state
        store.set("user:status", "online");
        
        // Simulate two replicas with clock skew
        // Replica A: Correct time
        VersionedValue replicaA = store.get("user:status").orElseThrow();
        
        // Replica B: Clock is 1 hour behind (common in distributed systems)
        // In timestamp system, B's writes would be ignored as "old"
        // In version system, B can still write if version matches
        
        // Replica B tries to update with old timestamp but correct version
        boolean success = store.compareAndSet("user:status", "offline", replicaA.getVersion());
        assertTrue(success, "Version-based update succeeds despite clock skew");
        
        // Demonstrate the problem with timestamps
        store.demonstrateClockSkewProblem("user:status", "zombie-status", 
            System.currentTimeMillis() - 3600000);  // 1 hour old timestamp
    }
    
    @Test
    @DisplayName("Concurrent increments with retry - eventually consistent counter")
    void testEventuallyConsistentCounter() throws InterruptedException {
        String counterId = "page-views";
        store.set(counterId, "0");
        
        int numThreads = 10;
        int incrementsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger totalRetries = new AtomicInteger(0);
        
        for (int t = 0; t < numThreads; t++) {
            new Thread(() -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    boolean success = false;
                    int retries = 0;
                    
                    // Retry loop - this is what real distributed databases do
                    while (!success) {
                        VersionedValue current = store.get(counterId).orElseThrow();
                        int value = Integer.parseInt(current.getValue());
                        success = store.compareAndSet(
                            counterId, 
                            String.valueOf(value + 1), 
                            current.getVersion()
                        );
                        if (!success) {
                            retries++;
                            totalRetries.incrementAndGet();
                        }
                    }
                }
                latch.countDown();
            }).start();
        }
        
        latch.await();
        
        // Verify exactly the right number of increments
        int finalCount = Integer.parseInt(store.get(counterId).orElseThrow().getValue());
        assertEquals(numThreads * incrementsPerThread, finalCount,
            "Should have exactly 1000 increments");
        
        System.out.printf("Total retries due to version conflicts: %d%n", 
            totalRetries.get());
        System.out.printf("Average retries per successful write: %.2f%n",
            (double) totalRetries.get() / (numThreads * incrementsPerThread));
        
        // This demonstrates optimistic concurrency control:
        // - No locks held during computation
        // - Conflicts detected at write time
        // - Automatic retry ensures eventual success
    }
}