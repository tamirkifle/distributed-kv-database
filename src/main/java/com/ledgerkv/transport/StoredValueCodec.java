package com.ledgerkv.transport;

import com.ledgerkv.consistency.VersionMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes the values a replica holds for one key to and from the opaque {@code byte[]} that
 * {@link LsmEngine} persists. This is the single point where the versioned-value model meets byte
 * storage.
 *
 * <p><b>Format v2</b> stores a <em>sibling set</em> rather than a single value, because a replica
 * that can hold only one value has nowhere to put two causally concurrent writes and must discard
 * one. Layout (big-endian): {@code byte formatVersion}
 * (2), {@code int siblingCount}, then per sibling {@code long version}, {@code byte flags}
 * (bit 0 = tombstone), {@code int clockEntryCount}, per clock entry {@code UTF nodeId} +
 * {@code long counter}, then {@code int valueLength} and the value bytes.
 *
 * <p>v1 stored a bare single record with no version prefix. There is no persisted external data to
 * migrate, so a v1 record is rejected loudly rather than guessed at — the same posture the SSTable
 * footer bump took in 6c.
 */
public final class StoredValueCodec {

    private static final int FLAG_TOMBSTONE = 0x01;
    private static final byte FORMAT_V2 = 2;

    private StoredValueCodec() {
    }

    /** Encodes a single value as a one-element sibling set. */
    public static byte[] encode(StoredValue v) {
        return encodeAll(java.util.Collections.singletonList(v));
    }

    public static byte[] encodeAll(List<StoredValue> siblings) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.writeByte(FORMAT_V2);
            out.writeInt(siblings.size());
            for (StoredValue v : siblings) {
                out.writeLong(v.version());
                out.writeByte(v.tombstone() ? FLAG_TOMBSTONE : 0);

                Map<String, Long> clock = v.metadata().getVectorClock();
                out.writeInt(clock.size());
                for (Map.Entry<String, Long> entry : clock.entrySet()) {
                    out.writeUTF(entry.getKey());
                    out.writeLong(entry.getValue());
                }

                byte[] value = v.value();
                out.writeInt(value.length);
                out.write(value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode StoredValue", e);
        }
        return buffer.toByteArray();
    }

    /**
     * Decodes a record that is expected to hold exactly one value. Throws when the record holds
     * concurrent siblings, so no caller can silently collapse a conflict by reading it as a scalar.
     */
    public static StoredValue decode(byte[] bytes) {
        List<StoredValue> siblings = decodeAll(bytes);
        if (siblings.size() != 1) {
            throw new IllegalStateException(
                    "expected a single stored value but found " + siblings.size() + " siblings");
        }
        return siblings.get(0);
    }

    public static List<StoredValue> decodeAll(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte formatVersion = in.readByte();
            if (formatVersion != FORMAT_V2) {
                throw new IllegalStateException(
                        "unsupported stored-value format version " + formatVersion
                                + " (expected " + FORMAT_V2 + ")");
            }
            int siblingCount = in.readInt();
            List<StoredValue> siblings = new ArrayList<>(siblingCount);
            for (int s = 0; s < siblingCount; s++) {
                long version = in.readLong();
                boolean tombstone = (in.readByte() & FLAG_TOMBSTONE) != 0;

                int clockEntryCount = in.readInt();
                Map<String, Long> clock = new LinkedHashMap<>();
                for (int i = 0; i < clockEntryCount; i++) {
                    String nodeId = in.readUTF();
                    long counter = in.readLong();
                    clock.put(nodeId, counter);
                }

                int valueLength = in.readInt();
                byte[] value = new byte[valueLength];
                in.readFully(value);

                siblings.add(new StoredValue(value, version, tombstone, VersionMetadata.of(clock)));
            }
            return siblings;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode StoredValue", e);
        }
    }
}
