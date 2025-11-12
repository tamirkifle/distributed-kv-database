package com.ledgerkv.transport;

import com.google.protobuf.ByteString;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.storage.CloseableIterator;
import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.LsmEngine;
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
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC server exposing the {@code LedgerKvNode} service backed by a per-node {@link LsmEngine}.
 *
 * <p>Values are persisted as {@link StoredValue}s (value bytes + version + tombstone +
 * {@link VersionMetadata}) serialized through {@link StoredValueCodec}. The replica/quorum layer
 * that supplies real version metadata is wired in 2c; in 2b the node assigns a simple per-key
 * monotonic version derived from the current live value.
 */
public final class NodeServer implements AutoCloseable {

    private final Server server;
    private final LsmEngine engine;

    private NodeServer(int requestedPort, LsmEngine engine) {
        this.engine = engine;
        this.server = ServerBuilder.forPort(requestedPort)
                .addService(new LedgerKvNodeService(engine))
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

    /** Gracefully shuts the server down (waiting briefly for in-flight RPCs), then the engine. */
    @Override
    public void close() throws IOException, InterruptedException {
        server.shutdown();
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
        engine.close();
    }

    public static final class Builder {
        private final int port;
        private Path dataDir;

        private Builder(int port) {
            this.port = port;
        }

        /** The directory the backing {@link LsmEngine} stores its WAL + SSTables in. */
        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public NodeServer build() throws IOException {
            Objects.requireNonNull(dataDir, "dataDir must be set");
            return new NodeServer(port, LsmEngine.open(dataDir));
        }
    }

    /**
     * Implements the node service by delegating to the {@link LsmEngine}, encoding values through
     * {@link StoredValueCodec} on the way in and decoding on the way out.
     */
    private static final class LedgerKvNodeService extends LedgerKvNodeGrpc.LedgerKvNodeImplBase {

        private final LsmEngine engine;

        LedgerKvNodeService(LsmEngine engine) {
            this.engine = engine;
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            Optional<byte[]> stored = engine.get(request.getKey());
            GetResponse.Builder resp = GetResponse.newBuilder();
            if (stored.isPresent()) {
                StoredValue value = StoredValueCodec.decode(stored.get());
                resp.setFound(true).setValue(toProto(value));
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            long nextVersion = currentVersion(request.getKey()) + 1;
            StoredValue value = new StoredValue(
                    request.getValue().toByteArray(),
                    nextVersion,
                    false,
                    VersionMetadata.legacy(nextVersion));
            engine.put(request.getKey(), StoredValueCodec.encode(value));
            responseObserver.onNext(PutResponse.newBuilder().setVersion(nextVersion).build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
            boolean existed = engine.get(request.getKey()).isPresent();
            engine.delete(request.getKey());
            responseObserver.onNext(DeleteResponse.newBuilder().setExisted(existed).build());
            responseObserver.onCompleted();
        }

        @Override
        public void scan(ScanRequest request, StreamObserver<ScanEntry> responseObserver) {
            int limit = request.getLimit();
            int emitted = 0;
            try (CloseableIterator<Entry> entries =
                    engine.scan(request.getStartKey(), request.getEndKey())) {
                while (entries.hasNext()) {
                    if (limit > 0 && emitted >= limit) {
                        break;
                    }
                    Entry entry = entries.next();
                    StoredValue value = StoredValueCodec.decode(entry.value());
                    responseObserver.onNext(ScanEntry.newBuilder()
                            .setKey(entry.key())
                            .setValue(toProto(value))
                            .build());
                    emitted++;
                }
            }
            responseObserver.onCompleted();
        }

        /** The version of the current live value for {@code key}, or 0 if absent. */
        private long currentVersion(String key) {
            return engine.get(key)
                    .map(bytes -> StoredValueCodec.decode(bytes).version())
                    .orElse(0L);
        }

        /**
         * Maps a {@link StoredValue} onto the wire message. Only value + version are populated in
         * 2b; the {@code VersionedValuePb} metadata block (timestamp + origin_node) cannot carry
         * the vector clock and is reconciled in 2c.
         */
        private static VersionedValuePb toProto(StoredValue value) {
            return VersionedValuePb.newBuilder()
                    .setValue(ByteString.copyFrom(value.value()))
                    .setVersion(value.version())
                    .setTombstone(value.tombstone())
                    .build();
        }
    }
}
