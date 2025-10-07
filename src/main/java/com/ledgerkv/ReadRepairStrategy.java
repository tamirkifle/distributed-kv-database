package com.ledgerkv;

/**
 * Read repair strategies used by production databases.
 * 
 * Real-world: Cassandra, DynamoDB, and Riak all implement
 * variations of these strategies to heal inconsistencies.
 */
public enum ReadRepairStrategy {
    
    /**
     * Always repair when inconsistency detected.
     * Pro: Fastest consistency convergence
     * Con: High write amplification, thundering herd
     * Used by: Early Cassandra versions
     */
    ALWAYS,
    
    /**
     * Repair with probability (e.g., 10% of reads).
     * Pro: Reduces thundering herd problem
     * Con: Slower convergence
     * Used by: Uber's Cassandra fork, modern Cassandra
     */
    PROBABILISTIC,
    
    /**
     * Only repair if data is older than threshold.
     * Pro: Avoids repairing recently written data
     * Con: Complex to tune threshold
     * Used by: LinkedIn's Voldemort
     */
    TIME_BASED,
    
    /**
     * Never repair during reads, use background process.
     * Pro: No read latency impact
     * Con: Slower convergence, more complex
     * Used by: DynamoDB (anti-entropy), Riak (AAE)
     */
    NONE
}