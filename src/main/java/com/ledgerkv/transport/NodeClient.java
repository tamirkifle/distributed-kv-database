package com.ledgerkv.transport;

import com.google.protobuf.ByteString;
import com.ledgerkv.transport.proto.DeleteRequest;
import com.ledgerkv.transport.proto.GetRequest;
import com.ledgerkv.transport.proto.GetResponse;
import com.ledgerkv.transport.proto.HintRequest;
import com.ledgerkv.transport.proto.LedgerKvNodeGrpc;
import com.ledgerkv.transport.proto.PutRequest;
import com.ledgerkv.transport.proto.PutResponse;
import com.ledgerkv.transport.proto.ReplicaGetRequest;
import com.ledgerkv.transport.proto.ReplicaGetResponse;
import com.ledgerkv.transport.proto.ReplicaPutRequest;
import com.ledgerkv.transport.proto.ScanEntry;
import com.ledgerkv.transport.proto.ScanRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A blocking client for the {@code LedgerKvNode} service. Wraps a {@link ManagedChannel}
 * and the generated blocking stub, mapping to/from the proto messages so callers work in
 * plain Java types.
 */
public final class NodeClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final LedgerKvNodeGrpc.LedgerKvNodeBlockingStub stub;

    private NodeClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = LedgerKvNodeGrpc.newBlockingStub(channel);
    }

    public static NodeClient connect(String host, int port) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        return new NodeClient(channel);
    }

    /** Returns the value for {@code key}, or empty if absent. */
    public Optional<byte[]> get(String key) {
        GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());
        if (!response.getFound()) {
            return Optional.empty();
        }
        return Optional.of(response.getValue().getValue().toByteArray());
    }

    /** Writes {@code value} under {@code key}; returns the assigned version. */
    public long put(String key, byte[] value) {
        PutResponse response = stub.put(PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build());
        return response.getVersion();
    }

    /** Deletes {@code key}; returns true if it existed. */
    public boolean delete(String key) {
        return stub.delete(DeleteRequest.newBuilder().setKey(key).build()).getExisted();
    }

    /** Scans {@code [start, end)} in ascending key order, up to {@code limit} entries. */
    public List<ScanEntry> scan(String start, String end, int limit) {
        Iterator<ScanEntry> stream = stub.scan(ScanRequest.newBuilder()
                .setStartKey(start)
                .setEndKey(end)
                .setLimit(limit)
                .build());
        List<ScanEntry> entries = new ArrayList<>();
        while (stream.hasNext()) {
            entries.add(stream.next());
        }
        return entries;
    }

    /** Internal replica read: returns the stored versioned value for {@code key}, or empty. */
    public Optional<StoredValue> replicaGet(String key) {
        ReplicaGetResponse response =
                stub.replicaGet(ReplicaGetRequest.newBuilder().setKey(key).build());
        if (!response.getFound()) {
            return Optional.empty();
        }
        return Optional.of(VersionedValueProtos.fromProto(response.getValue()));
    }

    /** Internal replica write: stores {@code value} under {@code key} verbatim (no re-versioning). */
    public void replicaPut(String key, StoredValue value) {
        stub.replicaPut(ReplicaPutRequest.newBuilder()
                .setKey(key)
                .setValue(VersionedValueProtos.toProto(value))
                .build());
    }

    /** Delivers a hinted-handoff write for {@code targetNodeId} to this node, stored verbatim. */
    public void deliverHint(String targetNodeId, String key, StoredValue value) {
        stub.deliverHint(HintRequest.newBuilder()
                .setTargetNode(targetNodeId)
                .setKey(key)
                .setValue(VersionedValueProtos.toProto(value))
                .build());
    }

    @Override
    public void close() throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
        }
    }
}
