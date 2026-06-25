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
    private final boolean deleted;
    
    public VersionedValue(String value, long version) {
        this(value, version, VersionMetadata.legacy(version));
    }

    public VersionedValue(String value, long version, VersionMetadata versionMetadata) {
        this(value, version, versionMetadata, false);
    }

    public VersionedValue(String value, long version, VersionMetadata versionMetadata,
                          boolean deleted) {
        this.value = value;
        this.version = version;
        this.timestamp = System.currentTimeMillis();
        this.versionMetadata = versionMetadata;
        this.deleted = deleted;
    }

    /** A tombstone: a versioned marker meaning "deleted at this clock". */
    public static VersionedValue tombstone(long version, VersionMetadata versionMetadata) {
        return new VersionedValue("", version, versionMetadata, true);
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

    /**
     * True when this version records a deletion rather than a value. A tombstone replicates,
     * repairs, and resolves conflicts exactly like a value — that is the point of it. Absence
     * carries no version, so a replica that never had the key and one that deleted it would
     * otherwise be indistinguishable, and read repair would resurrect the value.
     */
    public boolean isDeleted() {
        return deleted;
    }
    
    @Override
    public String toString() {
        return String.format(
            "VersionedValue{value='%s', version=%d, timestamp=%d, metadata=%s, deleted=%s}",
            value, version, timestamp, versionMetadata, deleted);
    }
}
