package com.ledgerkv.transport;

import com.ledgerkv.consistency.VersionMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serializes a {@link StoredValue} to and from the opaque {@code byte[]} that {@link LsmEngine}
 * persists. This is the single point where the versioned-value model meets byte storage.
 *
 * <p>Wire format (big-endian): {@code long version}, {@code byte flags} (bit 0 = tombstone),
 * {@code int clockEntryCount}, then per clock entry {@code UTF nodeId} + {@code long counter},
 * then {@code int valueLength} and the value bytes.
 */
public final class StoredValueCodec {

    private static final int FLAG_TOMBSTONE = 0x01;

    private StoredValueCodec() {
    }

    public static byte[] encode(StoredValue v) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
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
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode StoredValue", e);
        }
        return buffer.toByteArray();
    }

    public static StoredValue decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
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

            return new StoredValue(value, version, tombstone, VersionMetadata.of(clock));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode StoredValue", e);
        }
    }
}
