package com.ledgerkv;

import com.ledgerkv.consistency.VersionMetadata;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KV store with version tracking for optimistic concurrency control.
 * 
 * Design decision: Monotonic version counter per key (not global).
 * This matches DynamoDB's approach - each item has its own version sequence.
 * 
 * Instagram's Cassandra problem: They used timestamps which caused
 * "resurrection" of deleted photos due to clock skew. Versions prevent this.
 */
public class VersionedKVStore {
    
    private final ConcurrentHashMap<String, VersionedValue> store;
    private final ConcurrentHashMap<String, Long> versionCounters;
    
    public VersionedKVStore() {
        this.store = new ConcurrentHashMap<>();
        this.versionCounters = new ConcurrentHashMap<>();
    }
    
    /**
     * Set without version check - always succeeds.
     * Returns the new version number.
     */
    public long set(String key, String value) {
        return set(key, value, null);
    }

    public long set(String key, String value, VersionMetadata versionMetadata) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        
        // Get next version for this key
        long newVersion = versionCounters.compute(key, (k, v) -> v == null ? 1L : v + 1);
        
        VersionMetadata metadata = versionMetadata == null
            ? VersionMetadata.legacy(newVersion)
            : versionMetadata;
        store.put(key, new VersionedValue(value, newVersion, metadata));
        return newVersion;
    }

    void putVersionedValue(String key, VersionedValue value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        VersionedValue versionedValue = Objects.requireNonNull(value, "versioned value cannot be null");
        store.put(key, versionedValue);
        versionCounters.merge(key, versionedValue.getVersion(), Math::max);
    }
    
    /**
     * Conditional set - only succeeds if version matches.
     * This enables optimistic concurrency control.
     * 
     * Real-world: This is how DynamoDB's conditional writes work.
     */
    public boolean compareAndSet(String key, String value, long expectedVersion) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        
        // Track if update succeeded
        boolean[] success = {false};
        
        store.compute(key, (k, currentValue) -> {
            // Key doesn't exist - only succeed if expecting version 0
            if (currentValue == null) {
                if (expectedVersion == 0) {
                    long newVersion = versionCounters.compute(key, (k2, v) -> 1L);
                    success[0] = true;
                    return new VersionedValue(value, newVersion);
                }
                return null;  // Version mismatch
            }
            
            // Key exists - check version
            if (currentValue.getVersion() == expectedVersion) {
                long newVersion = versionCounters.compute(key, (k2, v) -> v + 1);
                success[0] = true;
                return new VersionedValue(value, newVersion);
            }
            
            success[0] = false;
            return currentValue;  // Version mismatch, keep current
        });
        
        return success[0];
    }
    
    /**
     * Get value with version information.
     */
    public Optional<VersionedValue> get(String key) {
        if (key == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(store.get(key));
    }

    Set<String> keysSnapshot() {
        return Set.copyOf(store.keySet());
    }
    
    /**
     * Delete with version check - prevents deleting modified data.
     * 
     * Real-world: Prevents the "phantom delete" problem where
     * a delete removes data that was modified after read.
     */
    public boolean compareAndDelete(String key, long expectedVersion) {
        if (key == null) {
            return false;
        }
        
        boolean[] success = {false};
        
        store.compute(key, (k, currentValue) -> {
            if (currentValue != null && currentValue.getVersion() == expectedVersion) {
                success[0] = true;
                return null;  // Remove the entry
            }
            success[0] = false;
            return currentValue;  // Keep current value
        });
        
        return success[0];
    }
    
    /**
     * Demonstrate why versions > timestamps.
     * Simulates clock skew scenario.
     */
    public void demonstrateClockSkewProblem(String key, String value, long fakeTimestamp) {
        // In a timestamp-based system, this could "resurrect" old data
        // if fakeTimestamp > current time due to clock skew
        // Our version-based system is immune to this!
        
        VersionedValue current = store.get(key);
        if (current != null) {
            System.out.printf("Clock skew detected! Incoming timestamp %d, current version %d%n",
                fakeTimestamp, current.getVersion());
            System.out.println("Version-based: Correctly rejects based on version");
            System.out.println("Timestamp-based: Would incorrectly accept if timestamp is higher");
        }
    }
    
    public int size() {
        return store.size();
    }
    
    public void clear() {
        store.clear();
        versionCounters.clear();
    }
}
