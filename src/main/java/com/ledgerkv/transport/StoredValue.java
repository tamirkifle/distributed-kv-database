package com.ledgerkv.transport;

import com.ledgerkv.consistency.VersionMetadata;
import java.util.Objects;

/**
 * The versioned value a replica conceptually stores: opaque value bytes plus the version, a
 * tombstone marker, and the {@link VersionMetadata} vector clock. {@link LsmEngine} persists only
 * {@code byte[]}, so {@link StoredValueCodec} serializes this object into the engine's value bytes.
 *
 * <p>Immutable; the {@code value} array is copied on construction and on access.
 */
public final class StoredValue {

    private final byte[] value;
    private final long version;
    private final boolean tombstone;
    private final VersionMetadata metadata;

    public StoredValue(byte[] value, long version, boolean tombstone, VersionMetadata metadata) {
        Objects.requireNonNull(value, "value must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        this.value = value.clone();
        this.version = version;
        this.tombstone = tombstone;
        this.metadata = metadata;
    }

    public byte[] value() {
        return value.clone();
    }

    public long version() {
        return version;
    }

    public boolean tombstone() {
        return tombstone;
    }

    public VersionMetadata metadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "StoredValue{version=" + version + ", tombstone=" + tombstone
                + ", valueLen=" + value.length + ", metadata=" + metadata + '}';
    }
}
