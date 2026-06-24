package com.ledgerkv.transport;

import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.storage.CloseableIterator;
import com.ledgerkv.storage.lsm.Entry;
import com.ledgerkv.storage.lsm.LsmEngine;
import com.ledgerkv.transport.proto.DeleteRequest;
import com.ledgerkv.transport.proto.DeleteResponse;
import com.ledgerkv.transport.proto.GetRequest;
import com.ledgerkv.transport.proto.GetResponse;
import com.ledgerkv.transport.proto.HintRequest;
import com.ledgerkv.transport.proto.HintResponse;
import com.ledgerkv.transport.proto.LedgerKvNodeGrpc;
import com.ledgerkv.transport.proto.PutRequest;
import com.ledgerkv.transport.proto.PutResponse;
import com.ledgerkv.transport.proto.ReplicaGetRequest;
import com.ledgerkv.transport.proto.ReplicaGetResponse;
import com.ledgerkv.transport.proto.ReplicaPutRequest;
import com.ledgerkv.transport.proto.ReplicaPutResponse;
import com.ledgerkv.transport.proto.ScanEntry;
import com.ledgerkv.transport.proto.ScanRequest;
import com.ledgerkv.transport.proto.VersionedValuePb;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
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
    private final LedgerKvNodeService service;

    private NodeServer(int requestedPort, LsmEngine engine) {
        this.engine = engine;
        this.service = new LedgerKvNodeService(engine);
        this.server = ServerBuilder.forPort(requestedPort)
                .addService(service)
                .build();
    }

    /**
     * Routes this node's <em>public</em> Get/Put through {@code coordinator} (a quorum coordinator)
     * instead of straight to local disk. Internal replica RPCs always hit the local engine.
     */
    public void useCoordinator(ClientCoordinator coordinator) {
        service.setCoordinator(coordinator);
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
        private final ReplicaStore store;
        private volatile ClientCoordinator coordinator;

        LedgerKvNodeService(LsmEngine engine) {
            this.engine = engine;
            this.store = new ReplicaStore(engine);
        }

        void setCoordinator(ClientCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            ClientCoordinator coord = coordinator;
            if (coord != null) {
                try {
                    List<StoredValue> values = coord.get(request.getKey());
                    GetResponse.Builder resp = GetResponse.newBuilder();
                    for (StoredValue value : values) {
                        resp.addSiblings(VersionedValueProtos.toProto(value));
                    }
                    if (!values.isEmpty()) {
                        // found is true whenever the key exists, conflicted or not. value carries a
                        // winner only when there is exactly one, so a caller that reads the scalar
                        // field can never mistake a conflict for a resolved value.
                        resp.setFound(true);
                        if (values.size() == 1) {
                            resp.setValue(VersionedValueProtos.toProto(values.get(0)));
                        }
                    }
                    responseObserver.onNext(resp.build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    responseObserver.onError(
                            Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
                }
                return;
            }
            List<StoredValue> stored = store.get(request.getKey());
            GetResponse.Builder resp = GetResponse.newBuilder();
            for (StoredValue value : stored) {
                resp.addSiblings(VersionedValueProtos.toProto(value));
            }
            if (!stored.isEmpty()) {
                resp.setFound(true);
                if (stored.size() == 1) {
                    resp.setValue(VersionedValueProtos.toProto(stored.get(0)));
                }
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            ClientCoordinator coord = coordinator;
            if (coord != null) {
                try {
                    StoredValue stored = coord.put(request.getKey(), request.getValue().toByteArray());
                    responseObserver.onNext(
                            PutResponse.newBuilder().setVersion(stored.version()).build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    responseObserver.onError(
                            Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
                }
                return;
            }
            long nextVersion = currentVersion(request.getKey()) + 1;
            StoredValue value = new StoredValue(
                    request.getValue().toByteArray(),
                    nextVersion,
                    false,
                    VersionMetadata.legacy(nextVersion));
            // Administrative single-node path: this write supersedes whatever was there.
            store.replace(request.getKey(), java.util.Collections.singletonList(value));
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
                    // A conflicted key streams its highest-versioned sibling; scan has no field to
                    // carry a conflict, and inventing one is out of scope for a range read.
                    List<StoredValue> siblings = StoredValueCodec.decodeAll(entry.value());
                    StoredValue value = siblings.stream()
                            .max(java.util.Comparator.comparingLong(StoredValue::version))
                            .orElseThrow(() -> new IllegalStateException("empty sibling set"));
                    responseObserver.onNext(ScanEntry.newBuilder()
                            .setKey(entry.key())
                            .setValue(VersionedValueProtos.toProto(value))
                            .build());
                    emitted++;
                }
            }
            responseObserver.onCompleted();
        }

        @Override
        public void replicaGet(ReplicaGetRequest request,
                StreamObserver<ReplicaGetResponse> responseObserver) {
            List<StoredValue> stored = store.get(request.getKey());
            ReplicaGetResponse.Builder resp = ReplicaGetResponse.newBuilder();
            for (StoredValue value : stored) {
                resp.addSiblings(VersionedValueProtos.toProto(value));
            }
            if (!stored.isEmpty()) {
                resp.setFound(true);
                if (stored.size() == 1) {
                    resp.setValue(VersionedValueProtos.toProto(stored.get(0)));
                }
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void replicaPut(ReplicaPutRequest request,
                StreamObserver<ReplicaPutResponse> responseObserver) {
            storeVerbatim(request.getKey(), request.getValue());
            responseObserver.onNext(ReplicaPutResponse.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void deliverHint(HintRequest request, StreamObserver<HintResponse> responseObserver) {
            // The hint's target_node is this server; store the carried value verbatim.
            storeVerbatim(request.getKey(), request.getValue());
            responseObserver.onNext(HintResponse.newBuilder().setAccepted(true).build());
            responseObserver.onCompleted();
        }

        /**
         * Merges the coordinator-supplied versioned value into whatever this replica holds, under a
         * per-key lock. The coordinator still owns versioning — the value is stored with the clock
         * it arrived with — but a causally older or duplicate arrival no longer overwrites a newer
         * one, and two concurrent values are both kept.
         */
        private void storeVerbatim(String key, VersionedValuePb value) {
            store.merge(key, VersionedValueProtos.fromProto(value));
        }

        /** The highest version currently stored for {@code key}, or 0 if absent. */
        private long currentVersion(String key) {
            return store.get(key).stream().mapToLong(StoredValue::version).max().orElse(0L);
        }
    }
}
