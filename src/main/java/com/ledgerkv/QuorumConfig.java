package com.ledgerkv;

/**
 * Quorum configuration - the heart of tunable consistency.
 * 
 * Real-world: DynamoDB, Cassandra, and Riak all use this model.
 *
 * <p>N, W and R are the Dynamo dials. PW and PR are Riak's primary-only counts: how many of the
 * acknowledgements must come from the key's <em>primary</em> replicas rather than from a fallback
 * accepted under sloppy quorum. They exist because {@code W + R > N} only implies overlap when the
 * replica set is fixed, and hinted handoff and hedging deliberately break that assumption.
 */
public class QuorumConfig {
    
    private final int n;  // Total nodes (replicas)
    private final int w;  // Write quorum (nodes that must acknowledge write)
    private final int r;  // Read quorum (nodes to read from)
    private final int pw; // Primary write quorum: acks that must come from primary replicas
    private final int pr; // Primary read quorum: responses that must come from primary replicas
    
    // Common production configurations
    public static final QuorumConfig STRONG_CONSISTENCY = new QuorumConfig(5, 3, 3);  // W+R>N
    public static final QuorumConfig EVENTUAL_CONSISTENCY = new QuorumConfig(5, 1, 1);  // Fast
    public static final QuorumConfig WRITE_HEAVY = new QuorumConfig(5, 1, 5);  // Fast writes
    public static final QuorumConfig READ_HEAVY = new QuorumConfig(5, 5, 1);  // Fast reads
    
    public QuorumConfig(int n, int w, int r) {
        this(n, w, r, 0, 0);
    }

    /**
     * @param pw primary-write count: acknowledgements that must come from the key's <em>primary</em>
     *     replicas rather than from a hedge or hinted-handoff fallback. 0 means no requirement.
     * @param pr primary-read count, likewise for reads.
     */
    public QuorumConfig(int n, int w, int r, int pw, int pr) {
        if (n < 1) throw new IllegalArgumentException("N must be >= 1");
        if (w < 1 || w > n) throw new IllegalArgumentException("W must be between 1 and N");
        if (r < 1 || r > n) throw new IllegalArgumentException("R must be between 1 and N");
        if (pw < 0 || pw > w) throw new IllegalArgumentException("PW must be between 0 and W");
        if (pr < 0 || pr > r) throw new IllegalArgumentException("PR must be between 0 and R");

        this.n = n;
        this.w = w;
        this.r = r;
        this.pw = pw;
        this.pr = pr;
    }

    /**
     * Whether the numbers satisfy the Dynamo inequality {@code W + R > N}.
     *
     * <p>This is a statement about three integers and nothing else. It is <em>not</em> a promise
     * that a read sees the latest write, because the inequality's overlap argument silently assumes
     * a fixed replica set. Under sloppy quorum — hinted handoff and request hedging — an
     * acknowledgement can come from a node outside the key's primary replicas, and a write set and
     * a read set can then be entirely disjoint while both hit their thresholds. Use
     * {@link #guaranteesReadYourWrites()} for the guarantee.
     */
    public boolean hasQuorumOverlap() {
        return (w + r) > n;
    }

    /**
     * Whether this configuration actually guarantees that a read observes the latest acknowledged
     * write: {@code PW + PR > N}, counting only primary replicas.
     *
     * <p>Requiring primaries is what makes the overlap argument valid, because primaries are the
     * fixed set the inequality assumes. Riak exposes the same two knobs under the same names.
     * Both default to 0, so a configuration that has not opted in reports false — which is the
     * honest answer for a system that will accept a fallback acknowledgement.
     */
    public boolean guaranteesReadYourWrites() {
        return (pw + pr) > n;
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
    public int getPw() { return pw; }
    public int getPr() { return pr; }
    
    @Override
    public String toString() {
        return String.format(
            "QuorumConfig{N=%d, W=%d, R=%d, PW=%d, PR=%d, overlap=%s, readYourWrites=%s, "
                + "writeTolerance=%d, readTolerance=%d}",
            n, w, r, pw, pr, hasQuorumOverlap(), guaranteesReadYourWrites(),
            getWriteFailureTolerance(), getReadFailureTolerance());
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