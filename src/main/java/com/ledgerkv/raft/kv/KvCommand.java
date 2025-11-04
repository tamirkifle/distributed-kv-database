package com.ledgerkv.raft.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * An immutable, deterministically-encoded Raft KV command. Writes (PUT/DELETE) are the only
 * commands that enter the Raft log; reads are served at the leader and never logged. Each command
 * carries a {@code (clientId, sequenceNumber)} pair so the state machine can apply it at most once.
 */
public final class KvCommand {

    public enum Op { PUT, DELETE }

    private final Op op;
    private final String clientId;
    private final long sequenceNumber;
    private final String key;
    private final byte[] value; // null for DELETE

    private KvCommand(Op op, String clientId, long sequenceNumber, String key, byte[] value) {
        this.op = Objects.requireNonNull(op, "op");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.sequenceNumber = sequenceNumber;
        this.key = Objects.requireNonNull(key, "key");
        this.value = value == null ? null : value.clone();
    }

    public static KvCommand put(String clientId, long sequenceNumber, String key, byte[] value) {
        return new KvCommand(Op.PUT, clientId, sequenceNumber, key,
                Objects.requireNonNull(value, "value"));
    }

    public static KvCommand delete(String clientId, long sequenceNumber, String key) {
        return new KvCommand(Op.DELETE, clientId, sequenceNumber, key, null);
    }

    public Op op() {
        return op;
    }

    public String clientId() {
        return clientId;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public byte[] encode() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(op == Op.PUT ? 1 : 0);
            writeString(out, clientId);
            out.writeLong(sequenceNumber);
            writeString(out, key);
            if (value == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(value.length);
                out.write(value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("KvCommand encode failed", e);
        }
        return baos.toByteArray();
    }

    public static KvCommand decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            Op op = in.readByte() == 1 ? Op.PUT : Op.DELETE;
            String clientId = readString(in);
            long seq = in.readLong();
            String key = readString(in);
            int len = in.readInt();
            byte[] value = null;
            if (len >= 0) {
                value = new byte[len];
                in.readFully(value);
            }
            return new KvCommand(op, clientId, seq, key, value);
        } catch (IOException e) {
            throw new UncheckedIOException("KvCommand decode failed", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KvCommand)) {
            return false;
        }
        KvCommand other = (KvCommand) o;
        return sequenceNumber == other.sequenceNumber
                && op == other.op
                && clientId.equals(other.clientId)
                && key.equals(other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return (Objects.hash(op, clientId, sequenceNumber, key) * 31) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "KvCommand{" + op + " client=" + clientId + " seq=" + sequenceNumber
                + " key=" + key + " valueBytes=" + (value == null ? -1 : value.length) + '}';
    }
}
