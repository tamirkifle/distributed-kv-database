package com.ledgerkv;

import com.ledgerkv.failure.FailureContext;
import com.ledgerkv.consistency.ConflictResolutionPolicy;
import com.ledgerkv.consistency.VersionConflictResolver;

import java.util.*;

/**
 * Response from a quorum operation.
 * Contains all values seen and metadata about the operation.
 * 
 * Real-world: This is what Cassandra returns internally when
 * doing quorum reads - multiple versions that need resolution.
 */
public class QuorumResponse {
    
    private final boolean successful;
    private final VersionedValue value;  // Highest version seen
    private final List<VersionedValue> allValues;  // All values from nodes
    private final List<VersionedValue> conflictingValues;
    private final int respondingNodes;
    private final int requiredNodes;
    private final long latencyMs;
    private final FailureContext failureContext;
    
    public QuorumResponse(boolean successful, VersionedValue value, 
                          List<VersionedValue> allValues, int respondingNodes, 
                          int requiredNodes, long latencyMs) {
        this(successful, value, allValues, respondingNodes, requiredNodes, latencyMs,
            FailureContext.empty());
    }

    public QuorumResponse(boolean successful, VersionedValue value,
                          List<VersionedValue> allValues, int respondingNodes,
                          int requiredNodes, long latencyMs,
                          FailureContext failureContext) {
        this(successful, value, allValues, respondingNodes, requiredNodes, latencyMs, failureContext,
            ConflictResolutionPolicy.PRESERVE_CONFLICTS);
    }

    public QuorumResponse(boolean successful, VersionedValue value,
                          List<VersionedValue> allValues, int respondingNodes,
                          int requiredNodes, long latencyMs,
                          ConflictResolutionPolicy conflictResolutionPolicy) {
        this(successful, value, allValues, respondingNodes, requiredNodes, latencyMs,
            FailureContext.empty(), conflictResolutionPolicy);
    }

    public QuorumResponse(boolean successful, VersionedValue value,
                          List<VersionedValue> allValues, int respondingNodes,
                          int requiredNodes, long latencyMs,
                          FailureContext failureContext,
                          ConflictResolutionPolicy conflictResolutionPolicy) {
        this.successful = successful;
        this.allValues = new ArrayList<>(allValues);
        ConflictResolutionPolicy policy = Objects.requireNonNull(conflictResolutionPolicy,
            "conflict resolution policy must not be null");
        this.conflictingValues = successful
            && policy == ConflictResolutionPolicy.PRESERVE_CONFLICTS
            ? VersionConflictResolver.conflictingSiblings(this.allValues)
            : List.of();
        this.value = successful
            ? VersionConflictResolver.resolvedValue(value, this.allValues, policy)
            : value;
        this.respondingNodes = respondingNodes;
        this.requiredNodes = requiredNodes;
        this.latencyMs = latencyMs;
        this.failureContext = Objects.requireNonNull(failureContext, "failure context must not be null");
    }
    
    /**
     * Check if nodes have diverged (different values).
     * This indicates need for read repair.
     */
    public boolean hasInconsistency() {
        if (allValues.size() <= 1) return false;
        
        // Count nulls and non-nulls
        int nullCount = 0;
        int nonNullCount = 0;
        Long firstVersion = null;
        String firstValue = null;
        
        for (VersionedValue v : allValues) {
            if (v == null) {
                nullCount++;
            } else {
                nonNullCount++;
                if (firstVersion == null) {
                    firstVersion = v.getVersion();
                    firstValue = v.getValue();
                } else {
                    // Check if non-null values differ
                    if (v.getVersion() != firstVersion || !v.getValue().equals(firstValue)) {
                        return true;  // Different non-null values
                    }
                }
            }
        }
        
        // If we have both nulls and non-nulls, that's inconsistency
        return nullCount > 0 && nonNullCount > 0;
    }
    
    /**
     * Get the number of nodes with stale data.
     * Used for monitoring inconsistency levels.
     */
    public int getStaleNodeCount() {
        if (allValues.isEmpty()) return 0;
        
        // If no value exists (all nulls), no stale nodes
        if (value == null) {
            return 0;
        }
        
        int staleCount = 0;
        long highestVersion = value.getVersion();
        
        for (VersionedValue v : allValues) {
            if (v == null || v.getVersion() < highestVersion) {
                staleCount++;
            }
        }
        return staleCount;
    }
    
    public boolean isSuccessful() { return successful; }
    public VersionedValue getValue() { return value; }
    public List<VersionedValue> getAllValues() { return new ArrayList<>(allValues); }
    public boolean hasConflicts() { return !conflictingValues.isEmpty(); }
    public List<VersionedValue> getConflictingValues() { return new ArrayList<>(conflictingValues); }
    public int getRespondingNodes() { return respondingNodes; }
    public int getRequiredNodes() { return requiredNodes; }
    public long getLatencyMs() { return latencyMs; }
    public FailureContext getFailureContext() { return failureContext; }
    
    @Override
    public String toString() {
        return String.format("QuorumResponse{success=%s, value=%s, responding=%d/%d, latency=%dms, inconsistent=%s}",
            successful, value, respondingNodes, requiredNodes, latencyMs, hasInconsistency());
    }
}
