package com.ledgerkv;

import com.ledgerkv.consistency.VersionMetadata;

/**
 * Value with version tracking.
 * 
 * Real-world parallel: DynamoDB's version tracking, Cassandra's timestamps,
 * Git's commit hashes - all solving the same problem: ordering without synchronized clocks.
 */
public class VersionedValue {
    private final String value;
    private final long version;
    private final long timestamp;  // For comparison with version-based ordering
    private final VersionMetadata versionMetadata;
    
    public VersionedValue(String value, long version) {
        this(value, version, VersionMetadata.legacy(version));
    }

    public VersionedValue(String value, long version, VersionMetadata versionMetadata) {
        this.value = value;
        this.version = version;
        this.timestamp = System.currentTimeMillis();
        this.versionMetadata = versionMetadata;
    }
    
    public String getValue() {
        return value;
    }
    
    public long getVersion() {
        return version;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    public VersionMetadata getVersionMetadata() {
        return versionMetadata;
    }
    
    @Override
    public String toString() {
        return String.format("VersionedValue{value='%s', version=%d, timestamp=%d, metadata=%s}",
            value, version, timestamp, versionMetadata);
    }
}
