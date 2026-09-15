package com.ledgerkv.transport;

import com.google.protobuf.ByteString;
import com.ledgerkv.transport.proto.DeleteRequest;
import com.ledgerkv.transport.proto.DeleteResponse;
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
import com.ledgerkv.transport.proto.VersionedValuePb;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A blocking client for the {@code LedgerKvNode} service. Wraps a {@link ManagedChannel}
 * and the generated blocking stub, mapping to/from the proto messages so callers work in
 * plain Java types.
 *
 * <p>Every call carries a deadline. A blocking stub without one waits forever, and "forever" is
 * reachable: a node that is partitioned but still running accepts the connection and then never
 * answers. A caller that wraps this in its own retry budget does not help, because the budget is
 * only consulted between attempts and the first attempt never returns.
 */
public final class NodeClient implements AutoCloseable {

    /** Generous next to a healthy request, finite next to a hang. */
    private static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(10);

    private final ManagedChannel channel;
    private final LedgerKvNodeGrpc.LedgerKvNodeBlockingStub blockingStub;
    private final Duration deadline;

    private NodeClient(ManagedChannel channel, Duration deadline) {
        this.channel = channel;
        this.blockingStub = LedgerKvNodeGrpc.newBlockingStub(channel);
        this.deadline = deadline;
    }

    public static NodeClient connect(String host, int port) {
        return connect(host, port, DEFAULT_DEADLINE);
    }

    /** Connects with an explicit per-RPC deadline. */
    public static NodeClient connect(String host, int port, Duration deadline) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        return new NodeClient(channel, Objects.requireNonNull(deadline, "deadline"));
    }

    /** The stub with this client's deadline applied; deadlines are per-call, not per-stub. */
    private LedgerKvNodeGrpc.LedgerKvNodeBlockingStub stub() {
        return blockingStub.withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the value for {@code key}, or empty if absent.
     *
     * @throws ConflictingValuesException when the key holds concurrent siblings — an Optional
     *     cannot represent "exists, with two competing values", and reporting that as empty made a
     *     live key look missing. Callers that want to resolve
     *     the conflict use {@link #getSiblings(String)}.
     */
    public Optional<byte[]> get(String key) {
        List<StoredValue> siblings = getSiblings(key);
        if (siblings.isEmpty()) {
            return Optional.empty();
        }
        if (siblings.size() > 1) {
            throw new ConflictingValuesException(key, siblings);
        }
        return Optional.of(siblings.get(0).value());
    }

    /**
     * Every value the cluster holds for {@code key}: empty if absent, one in the ordinary case, and
     * more than one when concurrent writes have not been reconciled. The analogue of Riak returning
     * HTTP 300 Multiple Choices with the sibling list.
     */
    public List<StoredValue> getSiblings(String key) {
        GetResponse response = stub().get(GetRequest.newBuilder().setKey(key).build());
        throwIfRedirected(response.hasNotLeader() ? response.getNotLeader() : null);
        List<StoredValue> siblings = new ArrayList<>();
        for (VersionedValuePb sibling : response.getSiblingsList()) {
            siblings.add(VersionedValueProtos.fromProto(sibling));
        }
        return siblings;
    }

    /** Writes {@code value} under {@code key}; returns the assigned version. */
    public long put(String key, byte[] value) {
        return put(key, value, MutationId.absent());
    }

    /**
     * Writes {@code value} under {@code key} carrying {@code id}, and returns the assigned version.
     * Raft mode requires an id and rejects the write without one; quorum mode ignores it.
     *
     * @throws NotLeaderException if this node is not the Raft leader, carrying the leader's
     *     endpoint when it knows one. Retry there, reusing the same {@code id}.
     */
    public long put(String key, byte[] value, MutationId id) {
        PutRequest.Builder request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value));
        if (id.isPresent()) {
            request.setClientId(id.clientId()).setSequence(id.sequence());
        }
        PutResponse response = stub().put(request.build());
        throwIfRedirected(response.hasNotLeader() ? response.getNotLeader() : null);
        return response.getVersion();
    }

    /** Deletes {@code key}; returns true if it existed. */
    public boolean delete(String key) {
        return delete(key, MutationId.absent());
    }

    /** Deletes {@code key} carrying {@code id}; returns true if a live value existed. */
    public boolean delete(String key, MutationId id) {
        DeleteRequest.Builder request = DeleteRequest.newBuilder().setKey(key);
        if (id.isPresent()) {
            request.setClientId(id.clientId()).setSequence(id.sequence());
        }
        DeleteResponse response = stub().delete(request.build());
        throwIfRedirected(response.hasNotLeader() ? response.getNotLeader() : null);
        return response.getExisted();
    }

    /**
     * Turns a {@code NotLeader} hint into a {@link NotLeaderException}. The hint travels as a field
     * on an otherwise-successful response, TiKV-style, so without this a caller would read a
     * redirect as a real answer: version 0, or a key that looks absent.
     */
    private static void throwIfRedirected(com.ledgerkv.transport.proto.NotLeader hint) {
        if (hint == null) {
            return;
        }
        String leaderId = hint.getLeaderId().isEmpty() ? null : hint.getLeaderId();
        String endpoint = hint.getLeaderEndpoint().isEmpty() ? null : hint.getLeaderEndpoint();
        throw new NotLeaderException(leaderId, endpoint);
    }

    /** Scans {@code [start, end)} in ascending key order, up to {@code limit} entries. */
    public List<ScanEntry> scan(String start, String end, int limit) {
        Iterator<ScanEntry> stream = stub().scan(ScanRequest.newBuilder()
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

    /**
     * Internal replica read: every value the replica holds for {@code key} — empty if absent, more
     * than one if that replica holds concurrent siblings.
     */
    public List<StoredValue> replicaGet(String key) {
        ReplicaGetResponse response =
                stub().replicaGet(ReplicaGetRequest.newBuilder().setKey(key).build());
        List<StoredValue> siblings = new ArrayList<>();
        for (VersionedValuePb sibling : response.getSiblingsList()) {
            siblings.add(VersionedValueProtos.fromProto(sibling));
        }
        return siblings;
    }

    /** Internal replica write: stores {@code value} under {@code key} verbatim (no re-versioning). */
    public void replicaPut(String key, StoredValue value) {
        stub().replicaPut(ReplicaPutRequest.newBuilder()
                .setKey(key)
                .setValue(VersionedValueProtos.toProto(value))
                .build());
    }

    /** Delivers a hinted-handoff write for {@code targetNodeId} to this node, stored verbatim. */
    public void deliverHint(String targetNodeId, String key, StoredValue value) {
        stub().deliverHint(HintRequest.newBuilder()
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
