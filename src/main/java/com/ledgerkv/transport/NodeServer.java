package com.ledgerkv.transport;

import com.google.protobuf.ByteString;
import com.ledgerkv.transport.proto.DeleteRequest;
import com.ledgerkv.transport.proto.DeleteResponse;
import com.ledgerkv.transport.proto.GetRequest;
import com.ledgerkv.transport.proto.GetResponse;
import com.ledgerkv.transport.proto.LedgerKvNodeGrpc;
import com.ledgerkv.transport.proto.PutRequest;
import com.ledgerkv.transport.proto.PutResponse;
import com.ledgerkv.transport.proto.ScanEntry;
import com.ledgerkv.transport.proto.ScanRequest;
import com.ledgerkv.transport.proto.VersionedValuePb;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A gRPC server exposing the {@code LedgerKvNode} service over a real channel.
 *
 * <p>In 2a the service delegates to a temporary in-process store — an ordered map of
 * key to value plus a per-key monotonic version counter. This stand-in is
 * <strong>replaced by the LSM engine in 2b</strong>; the transport surface stays the same.
 */
public final class NodeServer implements AutoCloseable {

    private final int requestedPort;
    private final Server server;

    private NodeServer(int requestedPort) {
        this.requestedPort = requestedPort;
        this.server = ServerBuilder.forPort(requestedPort)
                .addService(new LedgerKvNodeService())
                .build();
    }

    public static Builder builder(int port) {
        return new Builder(port);
    }

    /** Starts serving; returns this for chaining. */
    public NodeServer start() throws IOException {
        server.start();
        return this;
    }

    /** The actual bound port, valid after {@link #start()} (resolves OS-assigned port 0). */
    public int port() {
        return server.getPort();
    }

    /** Gracefully shuts the server down, waiting briefly for in-flight RPCs. */
    @Override
    public void close() throws InterruptedException {
        server.shutdown();
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
    }

    public static final class Builder {
        private final int port;

        private Builder(int port) {
            this.port = port;
        }

        public NodeServer build() {
            return new NodeServer(port);
        }
    }

    /**
     * Temporary in-memory implementation of the node service. Keys are held in a sorted
     * map so {@code Scan} can stream a key range in ascending order without re-sorting.
     */
    private static final class LedgerKvNodeService extends LedgerKvNodeGrpc.LedgerKvNodeImplBase {

        private final ConcurrentSkipListMap<String, byte[]> store = new ConcurrentSkipListMap<>();
        private final Map<String, AtomicLong> versions = new ConcurrentHashMap<>();

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            byte[] value = store.get(request.getKey());
            GetResponse.Builder resp = GetResponse.newBuilder();
            if (value != null) {
                resp.setFound(true)
                        .setValue(VersionedValuePb.newBuilder()
                                .setValue(ByteString.copyFrom(value))
                                .setVersion(currentVersion(request.getKey()))
                                .build());
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            store.put(request.getKey(), request.getValue().toByteArray());
            long version = versions
                    .computeIfAbsent(request.getKey(), k -> new AtomicLong())
                    .incrementAndGet();
            responseObserver.onNext(PutResponse.newBuilder().setVersion(version).build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            boolean existed = store.remove(request.getKey()) != null;
            responseObserver.onNext(DeleteResponse.newBuilder().setExisted(existed).build());
            responseObserver.onCompleted();
        }

        @Override
        public void scan(ScanRequest request, StreamObserver<ScanEntry> responseObserver) {
            int limit = request.getLimit();
            int emitted = 0;
            for (Map.Entry<String, byte[]> entry :
                    store.subMap(request.getStartKey(), request.getEndKey()).entrySet()) {
                if (limit > 0 && emitted >= limit) {
                    break;
                }
                responseObserver.onNext(ScanEntry.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(VersionedValuePb.newBuilder()
                                .setValue(ByteString.copyFrom(entry.getValue()))
                                .setVersion(currentVersion(entry.getKey()))
                                .build())
                        .build());
                emitted++;
            }
            responseObserver.onCompleted();
        }

        private long currentVersion(String key) {
            AtomicLong v = versions.get(key);
            return v == null ? 0L : v.get();
        }
    }
}
