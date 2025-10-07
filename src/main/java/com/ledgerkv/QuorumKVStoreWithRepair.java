package com.ledgerkv;

import com.ledgerkv.metrics.RepairMetrics;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quorum KV store with read repair capability.
 * 
 * Real-world: This is how Cassandra heals inconsistencies automatically.
 * Discord uses this to ensure messages eventually appear correctly.
 * Reddit uses this to fix vote count discrepancies.
 */
public class QuorumKVStoreWithRepair extends QuorumKVStore {
    
    private ReadRepairStrategy repairStrategy = ReadRepairStrategy.ALWAYS;
    private double repairProbability = 0.1;  // For PROBABILISTIC strategy
    private long repairThresholdMs = 5000;   // For TIME_BASED strategy
    private final Random random = new Random();
    
    // Metrics
    private final AtomicLong repairsTriggered = new AtomicLong(0);
    private final AtomicLong nodesRepaired = new AtomicLong(0);
    private final AtomicLong repairLatencyTotal = new AtomicLong(0);
    
    // Background repair executor
    private final ExecutorService repairExecutor = Executors.newFixedThreadPool(2);
    
    public QuorumKVStoreWithRepair(String clusterId, QuorumConfig config) {
        super(clusterId, config);
    }
    
    /**
     * Read with automatic repair based on strategy.
     * This is the key innovation - healing during normal operations.
     */
    @Override
    public QuorumResponse read(String key) {
        // Perform normal quorum read
        QuorumResponse response = super.read(key);
        
        // Check if repair is needed
        if (response.isSuccessful() && response.hasInconsistency()) {
            if (!response.hasConflicts() && shouldRepair(response)) {
                performReadRepair(key, response);
            }
        }
        
        return response;
    }
    
    /**
     * Determine if repair should happen based on strategy.
     * This prevents thundering herd problem Uber discovered.
     */
    private boolean shouldRepair(QuorumResponse response) {
        switch (repairStrategy) {
            case ALWAYS:
                return true;
                
            case PROBABILISTIC:
                // Uber's solution: Only repair 10% of inconsistent reads
                return random.nextDouble() < repairProbability;
                
            case TIME_BASED:
                // Only repair if data is old enough
                if (response.getValue() != null) {
                    long age = System.currentTimeMillis() - response.getValue().getTimestamp();
                    return age > repairThresholdMs;
                }
                return true;
                
            case NONE:
                return false;
                
            default:
                return false;
        }
    }
    
    /**
     * Perform read repair - update stale nodes with latest value.
     * This is how Cassandra heals its cluster automatically.
     */
    private Future<?> performReadRepair(String key, QuorumResponse response) {
        repairsTriggered.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        // Get the latest value (highest version)
        VersionedValue latestValue = response.getValue();
        
        // If no value exists (all nulls), nothing to repair
        if (latestValue == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        // Identify which nodes need repair
        List<Integer> staleNodes = identifyStaleNodes(key, latestValue);
        
        if (staleNodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);  // All nodes already consistent
        }
        
        // Perform repair asynchronously to not block the read
        return repairExecutor.submit(() -> {
            System.out.printf("READ REPAIR: Fixing %d stale nodes for key '%s'%n", 
                staleNodes.size(), key);
            
            for (Integer nodeIndex : staleNodes) {
                try {
                    // Update stale node with the exact winning version and metadata.
                    getNodes().get(nodeIndex).putVersionedValue(key, latestValue);
                    nodesRepaired.incrementAndGet();
                    
                    System.out.printf("  Repaired node %d: %s -> %s (v%d)%n",
                        nodeIndex, "stale", latestValue.getValue(), latestValue.getVersion());
                        
                } catch (Exception e) {
                    System.err.printf("  Failed to repair node %d: %s%n", 
                        nodeIndex, e.getMessage());
                }
            }
            
            long repairTime = System.currentTimeMillis() - startTime;
            repairLatencyTotal.addAndGet(repairTime);
            
            System.out.printf("READ REPAIR: Completed in %d ms%n", repairTime);
        });
    }
    
    /**
     * Identify which nodes have stale or missing data.
     * These nodes will be targets for repair.
     */
    private List<Integer> identifyStaleNodes(String key, VersionedValue latestValue) {
        List<Integer> staleNodes = new ArrayList<>();
        
        for (int i = 0; i < getNodes().size(); i++) {
            Optional<VersionedValue> nodeValueOpt = getNodes().get(i).get(key);
            
            // A node is stale if it's empty, has a lower version, OR has the
            // same version but a different value.
            if (nodeValueOpt.isEmpty()) {
                staleNodes.add(i);
            } else {
                VersionedValue nodeValue = nodeValueOpt.get();
                if (nodeValue.getVersion() < latestValue.getVersion() ||
                    (nodeValue.getVersion() == latestValue.getVersion() &&
                        (!nodeValue.getValue().equals(latestValue.getValue()) ||
                            !nodeValue.getVersionMetadata().equals(latestValue.getVersionMetadata())))) {
                    staleNodes.add(i);
                }
            }

        }
        
        return staleNodes;
    }
    
    /**
     * Force repair of a specific key.
     * Used for testing and manual intervention.
     */
    public void forceRepair(String key) {
        // Read from all nodes
        QuorumConfig fullRead = new QuorumConfig(
            getConfig().getN(), 
            getConfig().getW(), 
            getConfig().getN()  // R=N to read from all
        );
        
        QuorumKVStore tempCluster = new QuorumKVStore("temp", fullRead);
        tempCluster.getNodes().clear();
        tempCluster.getNodes().addAll(this.getNodes());
        
        QuorumResponse response = tempCluster.read(key);
        
        if (response.hasInconsistency()) {
            Future<?> repairFuture = performReadRepair(key, response);
            try {
                // Block and wait for the async repair to finish
                repairFuture.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                Thread.currentThread().interrupt(); // Restore interruption status
                System.err.println("Forced repair failed to complete in time: " + e.getMessage());
            }
        }
        
        tempCluster.shutdown();
    }
    
    /**
     * Background anti-entropy process.
     * This is what DynamoDB uses instead of read repair.
     */
    public void runAntiEntropy() {
        System.out.println("ANTI-ENTROPY: Starting background repair process");
        
        repairExecutor.submit(() -> {
            // In production: Merkle trees to efficiently find differences
            // Simplified: Check all keys
            
            Set<String> allKeys = new TreeSet<>();
            
            // Collect all keys from all nodes
            for (VersionedKVStore node : getNodes()) {
                allKeys.addAll(node.keysSnapshot());
            }
            
            // For each key, ensure consistency
            for (String key : allKeys) {
                forceRepair(key);
            }
            
            System.out.println("ANTI-ENTROPY: Completed background repair");
        });
    }
    
    /**
     * Get repair statistics.
     * Operations teams monitor these metrics.
     */
    public String getRepairStats() {
        RepairMetrics metrics = getRepairMetrics();
        
        return String.format(
            "Read Repair Stats: Strategy=%s, Repairs=%d, Nodes Fixed=%d, Avg Latency=%dms",
            repairStrategy,
            metrics.getRepairCount(),
            metrics.getReplicasRepaired(),
            metrics.getAverageRepairLatencyMs()
        );
    }

    public RepairMetrics getRepairMetrics() {
        return new RepairMetrics(
            repairsTriggered.get(),
            nodesRepaired.get(),
            repairLatencyTotal.get()
        );
    }
    
    /**
     * Demonstrate the thundering herd problem.
     * This is what Uber discovered with aggressive read repair.
     */
    public void demonstrateThunderingHerd() {
        System.out.println("\n=== THUNDERING HERD DEMONSTRATION ===");
        
        // Write to single node (creating inconsistency)
        getNodes().get(0).set("popular-key", "hot-data");
        
        // Simulate 100 concurrent reads of same key
        System.out.println("100 clients reading same inconsistent key...");
        
        // With ALWAYS strategy
        setRepairStrategy(ReadRepairStrategy.ALWAYS);
        long alwaysRepairs = repairsTriggered.get();
        
        for (int i = 0; i < 100; i++) {
            read("popular-key");
        }
        
        long alwaysTriggered = repairsTriggered.get() - alwaysRepairs;
        System.out.printf("ALWAYS strategy: %d repairs triggered (thundering herd!)%n", 
            alwaysTriggered);
        
        // Reset and try with PROBABILISTIC
        getNodes().get(0).set("popular-key2", "hot-data2");
        setRepairStrategy(ReadRepairStrategy.PROBABILISTIC);
        setRepairProbability(0.1);  // 10% probability
        
        long probRepairs = repairsTriggered.get();
        
        for (int i = 0; i < 100; i++) {
            read("popular-key2");
        }
        
        long probTriggered = repairsTriggered.get() - probRepairs;
        System.out.printf("PROBABILISTIC (10%%): %d repairs triggered (controlled)%n", 
            probTriggered);
        
        System.out.println("\nLesson: Probabilistic repair prevents thundering herd!");
    }
    
    // Setters for configuration
    public void setRepairStrategy(ReadRepairStrategy strategy) {
        this.repairStrategy = strategy;
    }
    
    public void setRepairProbability(double probability) {
        this.repairProbability = Math.max(0.0, Math.min(1.0, probability));
    }
    
    public void setRepairThresholdMs(long thresholdMs) {
        this.repairThresholdMs = thresholdMs;
    }
    
    public ReadRepairStrategy getRepairStrategy() {
        return repairStrategy;
    }
    
    public long getRepairsTriggered() {
        return repairsTriggered.get();
    }
    
    public long getNodesRepaired() {
        return nodesRepaired.get();
    }
    
    @Override
    public void shutdown() {
        repairExecutor.shutdown();
        try {
            repairExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            repairExecutor.shutdownNow();
        }
        super.shutdown();
    }
}
