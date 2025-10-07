package com.ledgerkv;

/**
 * Quorum configuration - the heart of tunable consistency.
 * 
 * Real-world: DynamoDB, Cassandra, and Riak all use this model.
 * W+R>N guarantees strong consistency.
 * W+R<=N allows eventual consistency but higher availability.
 */
public class QuorumConfig {
    
    private final int n;  // Total nodes (replicas)
    private final int w;  // Write quorum (nodes that must acknowledge write)
    private final int r;  // Read quorum (nodes to read from)
    
    // Common production configurations
    public static final QuorumConfig STRONG_CONSISTENCY = new QuorumConfig(5, 3, 3);  // W+R>N
    public static final QuorumConfig EVENTUAL_CONSISTENCY = new QuorumConfig(5, 1, 1);  // Fast
    public static final QuorumConfig WRITE_HEAVY = new QuorumConfig(5, 1, 5);  // Fast writes
    public static final QuorumConfig READ_HEAVY = new QuorumConfig(5, 5, 1);  // Fast reads
    
    public QuorumConfig(int n, int w, int r) {
        if (n < 1) throw new IllegalArgumentException("N must be >= 1");
        if (w < 1 || w > n) throw new IllegalArgumentException("W must be between 1 and N");
        if (r < 1 || r > n) throw new IllegalArgumentException("R must be between 1 and N");
        
        this.n = n;
        this.w = w;
        this.r = r;
    }
    
    /**
     * Check if configuration guarantees strong consistency.
     * This is the fundamental equation from Amazon's Dynamo paper.
     */
    public boolean isStronglyConsistent() {
        return (w + r) > n;
    }
    
    /**
     * Maximum failures tolerable for writes.
     * If more than this many nodes fail, writes will fail.
     */
    public int getWriteFailureTolerance() {
        return n - w;
    }
    
    /**
     * Maximum failures tolerable for reads.
     * If more than this many nodes fail, reads will fail.
     */
    public int getReadFailureTolerance() {
        return n - r;
    }
    
    /**
     * Check if system remains available during partition.
     * True if minority partition can still serve requests.
     */
    public boolean isAvailableDuringPartition() {
        // Can operate with minority partition?
        int minoritySize = n / 2;  // Smaller partition in split
        return w <= minoritySize || r <= minoritySize;
    }
    
    public int getN() { return n; }
    public int getW() { return w; }
    public int getR() { return r; }
    
    @Override
    public String toString() {
        return String.format("QuorumConfig{N=%d, W=%d, R=%d, consistent=%s, writeTolerance=%d, readTolerance=%d}",
            n, w, r, isStronglyConsistent(), getWriteFailureTolerance(), getReadFailureTolerance());
    }
    
    /**
     * Real-world configurations used in production.
     */
    public static class ProductionExamples {
        // Amazon DynamoDB defaults
        public static final QuorumConfig DYNAMODB_DEFAULT = new QuorumConfig(3, 2, 2);
        
        // Amazon Shopping Cart (availability > consistency)
        public static final QuorumConfig AMAZON_CART = new QuorumConfig(3, 1, 1);
        
        // Amazon Order Placement (consistency > availability)
        public static final QuorumConfig AMAZON_ORDERS = new QuorumConfig(5, 3, 3);
        
        // Cassandra defaults
        public static final QuorumConfig CASSANDRA_QUORUM = new QuorumConfig(3, 2, 2);
        public static final QuorumConfig CASSANDRA_ONE = new QuorumConfig(3, 1, 1);
        public static final QuorumConfig CASSANDRA_ALL = new QuorumConfig(3, 3, 3);
        
        // Netflix's Cassandra for viewing history
        public static final QuorumConfig NETFLIX_HISTORY = new QuorumConfig(3, 1, 1);
    }
}