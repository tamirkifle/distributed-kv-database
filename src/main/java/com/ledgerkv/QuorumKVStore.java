package com.ledgerkv;

import com.ledgerkv.metrics.OperationMetrics;
import com.ledgerkv.metrics.OperationMetricsCollector;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Quorum-based replicated KV store.
 * Demonstrates W+R>N consistency guarantee.
 * 
 * Real-world: This is how DynamoDB, Cassandra, and Riak work internally.
 * Netflix survived AWS AZ failure because Cassandra quorum kept working.
 */
public class QuorumKVStore {
    
    private final String clusterId;
    private final List<VersionedKVStore> nodes;
    private final QuorumConfig config;
    private final ExecutorService executor;
    private final Random random = new Random();
    
    // Simulate network delays (production: 1-10ms same DC, 50-100ms cross-region)
    private long networkDelayMs = 5;
    private double failureRate = 0.0;  // For chaos testing
    private Set<Integer> failedNodes = new HashSet<>();  // Track explicitly failed nodes
    
    // Metrics
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong readCount = new AtomicLong(0);
    private final AtomicLong inconsistencyDetected = new AtomicLong(0);
    private final OperationMetricsCollector operationMetricsCollector = new OperationMetricsCollector();
    
    public QuorumKVStore(String clusterId, QuorumConfig config) {
        this.clusterId = clusterId;
        this.config = config;
        this.nodes = new ArrayList<>();
        this.executor = Executors.newFixedThreadPool(config.getN());
        
        // Initialize N nodes
        for (int i = 0; i < config.getN(); i++) {
            nodes.add(new VersionedKVStore());
        }
    }
    
    /**
     * Write with quorum.
     * Must get W acknowledgments to succeed.
     * 
     * This is how DynamoDB handles PutItem with ConsistentWrite=true.
     */
    public QuorumResponse write(String key, String value) {
        writeCount.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        // Send write to all N nodes in parallel
        List<CompletableFuture<WriteResult>> futures = new ArrayList<>();
        
        for (int i = 0; i < nodes.size(); i++) {
            final int nodeIndex = i;
            CompletableFuture<WriteResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    // Check if node is explicitly failed
                    if (failedNodes.contains(nodeIndex)) {
                        throw new RuntimeException("Node " + nodeIndex + " is partitioned");
                    }
                    
                    // Simulate network delay
                    Thread.sleep(networkDelayMs);
                    
                    // Simulate random node failure
                    if (random.nextDouble() < failureRate) {
                        throw new RuntimeException("Node " + nodeIndex + " failed");
                    }
                    
                    // Perform write
                    long version = nodes.get(nodeIndex).set(key, value);
                    return new WriteResult(nodeIndex, true, version);
                    
                } catch (Exception e) {
                    return new WriteResult(nodeIndex, false, -1);
                }
            }, executor);
            
            futures.add(future);
        }
        
        // Wait for W nodes to acknowledge
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);
        List<WriteResult> results = new CopyOnWriteArrayList<>();  // Thread-safe list
        
        // Use countdown latch to wait for W successes
        CountDownLatch latch = new CountDownLatch(1);
        
        for (CompletableFuture<WriteResult> future : futures) {
            future.thenAccept(result -> {
                results.add(result);
                completedCount.incrementAndGet();
                
                if (result.success) {
                    if (successCount.incrementAndGet() >= config.getW()) {
                        latch.countDown();  // Got enough acknowledgments
                    }
                }
                
                // Also release if all nodes responded (even if not enough successes)
                if (completedCount.get() >= config.getN()) {
                    latch.countDown();
                }
            });
        }
        
        try {
            // Wait for quorum or timeout
            boolean gotQuorum = latch.await(1000, TimeUnit.MILLISECONDS);
            
            long latency = System.currentTimeMillis() - startTime;
            
            if (successCount.get() >= config.getW()) {
                // Success! Got write quorum
                long maxVersion = results.stream()
                    .filter(r -> r.success)
                    .mapToLong(r -> r.version)
                    .max().orElse(0);
                
                QuorumResponse response = new QuorumResponse(true,
                    new VersionedValue(value, maxVersion),
                    Collections.emptyList(),
                    successCount.get(), config.getW(), latency);
                operationMetricsCollector.recordWrite(response);
                return response;
            } else {
                // Failed to get quorum
                QuorumResponse response = new QuorumResponse(false, null, Collections.emptyList(),
                    successCount.get(), config.getW(), latency);
                operationMetricsCollector.recordWrite(response);
                return response;
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            QuorumResponse response = new QuorumResponse(false, null, Collections.emptyList(),
                0, config.getW(), 0);
            operationMetricsCollector.recordWrite(response);
            return response;
        }
    }
    
    /**
     * Read with quorum.
     * Must read from R nodes and return highest version.
     * 
     * This demonstrates why W+R>N guarantees consistency:
     * At least one node in R must have seen the latest write from W.
     */
    public QuorumResponse read(String key) {
        readCount.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        // Read from all N nodes in parallel
        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        
        for (int i = 0; i < nodes.size(); i++) {
            final int nodeIndex = i;
            CompletableFuture<ReadResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    // Check if node is explicitly failed
                    if (failedNodes.contains(nodeIndex)) {
                        throw new RuntimeException("Node " + nodeIndex + " is partitioned");
                    }
                    
                    // Simulate network delay
                    Thread.sleep(networkDelayMs);
                    
                    // Simulate random node failure
                    if (random.nextDouble() < failureRate) {
                        throw new RuntimeException("Node " + nodeIndex + " failed");
                    }
                    
                    // Perform read
                    Optional<VersionedValue> value = nodes.get(nodeIndex).get(key);
                    return new ReadResult(nodeIndex, true, value.orElse(null));
                    
                } catch (Exception e) {
                    return new ReadResult(nodeIndex, false, null);
                }
            }, executor);
            
            futures.add(future);
        }
        
        // Collect R responses
        List<VersionedValue> values = new CopyOnWriteArrayList<>();  // Thread-safe list
        AtomicInteger responseCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        for (CompletableFuture<ReadResult> future : futures) {
            future.thenAccept(result -> {
                if (result.success) {
                    values.add(result.value);  // Add even if null
                    
                    if (responseCount.incrementAndGet() >= config.getR()) {
                        latch.countDown();  // Got enough responses
                    }
                }
            });
        }
        
        try {
            // Wait for R responses
            boolean gotQuorum = latch.await(1000, TimeUnit.MILLISECONDS);
            
            long latency = System.currentTimeMillis() - startTime;
            
            if (responseCount.get() >= config.getR()) {
                // Find highest version (latest value)
                VersionedValue latest = values.stream()
                    .filter(Objects::nonNull)
                    .max(Comparator.comparingLong(VersionedValue::getVersion))
                    .orElse(null);
                
                // Create response with ALL collected values (including nulls)
                List<VersionedValue> allCollectedValues = new ArrayList<>(values);
                
                // Check for inconsistency
                QuorumResponse response = new QuorumResponse(true, latest, allCollectedValues,
                    responseCount.get(), config.getR(), latency);
                
                if (response.hasInconsistency()) {
                    inconsistencyDetected.incrementAndGet();
                }
                
                operationMetricsCollector.recordRead(response);
                return response;
            } else {
                // Failed to get read quorum
                QuorumResponse response = new QuorumResponse(false, null, values,
                    responseCount.get(), config.getR(), latency);
                operationMetricsCollector.recordRead(response);
                return response;
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            QuorumResponse response = new QuorumResponse(false, null, Collections.emptyList(),
                0, config.getR(), 0);
            operationMetricsCollector.recordRead(response);
            return response;
        }
    }
    
    /**
     * Demonstrate what happens during network partition.
     * Some nodes become unreachable.
     */
    public void simulatePartition(Set<Integer> partitionedNodes) {
        System.out.printf("PARTITION: Nodes %s are unreachable%n", partitionedNodes);
        failedNodes.addAll(partitionedNodes);
    }
    
    /**
     * Heal partition - nodes become reachable again.
     */
    public void healPartition() {
        System.out.println("PARTITION HEALED: All nodes reachable");
        failedNodes.clear();
        this.failureRate = 0.0;
    }
    
    /**
     * Simulate specific number of node failures.
     */
    public void failNodes(int count) {
        failedNodes.clear();
        for (int i = 0; i < count && i < nodes.size(); i++) {
            failedNodes.add(i);
        }
        System.out.printf("Failed %d nodes: %s%n", count, failedNodes);
    }
    
    /**
     * Get cluster statistics.
     */
    public String getStats() {
        return String.format(
            "Cluster %s: Config=%s, Writes=%d, Reads=%d, Inconsistencies=%d",
            clusterId, config, writeCount.get(), readCount.get(), 
            inconsistencyDetected.get()
        );
    }
    
    // Helper classes
    private static class WriteResult {
        final int nodeIndex;
        final boolean success;
        final long version;
        
        WriteResult(int nodeIndex, boolean success, long version) {
            this.nodeIndex = nodeIndex;
            this.success = success;
            this.version = version;
        }
    }
    
    private static class ReadResult {
        final int nodeIndex;
        final boolean success;
        final VersionedValue value;
        
        ReadResult(int nodeIndex, boolean success, VersionedValue value) {
            this.nodeIndex = nodeIndex;
            this.success = success;
            this.value = value;
        }
    }
    
    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
    
    public QuorumConfig getConfig() { return config; }
    public long getWriteCount() { return writeCount.get(); }
    public long getReadCount() { return readCount.get(); }
    public long getInconsistencyCount() { return inconsistencyDetected.get(); }
    public OperationMetrics getOperationMetrics() { return operationMetricsCollector.snapshot(); }
    
    public void setNetworkDelayMs(long delay) { this.networkDelayMs = delay; }
    public void setFailureRate(double rate) { this.failureRate = rate; }
    
    // Direct node access for testing
    public List<VersionedKVStore> getNodes() { return nodes; }
}
