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
import java.util.concurrent.TimeUnit;

/**
 * A gRPC server exposing the {@code LedgerKvNode} service backed by a per-node {@link LsmEngine}.
 *
 * <p>Values are persisted as {@link StoredValue}s (value bytes + version + tombstone +
 * {@link VersionMetadata}) serialized through {@link StoredValueCodec}. The replica/quorum layer
 * that supplies real version metadata is wired in 2c; in 2b the node assigns a simple per-key
 * monotonic version derived from the current live value.
 *
 * <p>A server built with {@link Builder#coordinatorOnly()} has no engine at all. That is Raft mode:
 * the state machine is the Raft group's own map, so opening an LSM engine would create a second,
 * unused, still-fsyncing store on the same volume. Without an engine the local-storage paths —
 * Scan and the replica RPCs — answer {@code UNIMPLEMENTED} rather than pretending to be empty.
 */
public final class NodeServer implements AutoCloseable {

    private final Server server;
    /** Null in Raft mode: the Raft state machine is the store, and a second one would be a bug. */
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
        if (engine != null) {
            engine.close();
        }
    }

    public static final class Builder {
        private final int port;
        private Path dataDir;
        private boolean coordinatorOnly;

        private Builder(int port) {
            this.port = port;
        }

        /** The directory the backing {@link LsmEngine} stores its WAL + SSTables in. */
        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        /**
         * Builds a server with no local storage, whose public requests must all go through a
         * {@link ClientCoordinator}. Used by Raft mode, where the replicated state machine already
         * holds the data.
         */
        public Builder coordinatorOnly() {
            this.coordinatorOnly = true;
            return this;
        }

        public NodeServer build() throws IOException {
            if (coordinatorOnly) {
                if (dataDir != null) {
                    throw new IllegalStateException(
                            "coordinatorOnly opens no engine, so dataDir would be ignored");
                }
                return new NodeServer(port, null);
            }
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
            this.store = engine == null ? null : new ReplicaStore(engine);
        }

        /** The coordinator, or a failed RPC when this server has neither coordinator nor engine. */
        private ClientCoordinator requireCoordinator(StreamObserver<?> observer) {
            ClientCoordinator coord = coordinator;
            if (coord == null && engine == null) {
                observer.onError(Status.FAILED_PRECONDITION
                        .withDescription("node has no storage and no coordinator attached yet")
                        .asRuntimeException());
            }
            return coord;
        }

        /** Maps a coordinator failure onto a status, preserving a leader hint when there is one. */
        private static void failClientRequest(StreamObserver<?> observer, RuntimeException e) {
            if (e instanceof IllegalArgumentException) {
                // A malformed request: a missing client id, or a sequence the client already used.
                // Retrying it unchanged cannot help, so it must not look retriable.
                observer.onError(Status.INVALID_ARGUMENT
                        .withDescription(e.getMessage()).asRuntimeException());
                return;
            }
            observer.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
        }

        /**
         * The at-most-once identity on a request, or {@link MutationId#absent()} when the client
         * sent none. Quorum mode ignores it; the Raft coordinator refuses a mutation without one.
         */
        private static MutationId mutationId(String clientId, long sequence) {
            return clientId.isEmpty() ? MutationId.absent() : MutationId.of(clientId, sequence);
        }

        /** The NotLeader hint to attach to a response, or null when this node served the request. */
        private static com.ledgerkv.transport.proto.NotLeader hintFrom(RuntimeException e) {
            if (!(e instanceof NotLeaderException)) {
                return null;
            }
            NotLeaderException notLeader = (NotLeaderException) e;
            com.ledgerkv.transport.proto.NotLeader.Builder hint =
                    com.ledgerkv.transport.proto.NotLeader.newBuilder();
            if (notLeader.leaderId() != null) {
                hint.setLeaderId(notLeader.leaderId());
            }
            if (notLeader.leaderEndpoint() != null) {
                hint.setLeaderEndpoint(notLeader.leaderEndpoint());
            }
            return hint.build();
        }

        void setCoordinator(ClientCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            ClientCoordinator coord = requireCoordinator(responseObserver);
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
                    com.ledgerkv.transport.proto.NotLeader hint = hintFrom(e);
                    if (hint != null) {
                        responseObserver.onNext(GetResponse.newBuilder().setNotLeader(hint).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    failClientRequest(responseObserver, e);
                }
                return;
            }
            if (engine == null) {
                return; // requireCoordinator already failed the call
            }
            // A tombstone is a version internally but an absence to a client.
            List<StoredValue> stored = live(store.get(request.getKey()));
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

        /** Drops tombstones, which are storage-level records rather than client-visible values. */
        private static List<StoredValue> live(List<StoredValue> values) {
            List<StoredValue> result = new java.util.ArrayList<>();
            for (StoredValue value : values) {
                if (!value.tombstone()) {
                    result.add(value);
                }
            }
            return result;
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            ClientCoordinator coord = requireCoordinator(responseObserver);
            if (coord != null) {
                try {
                    StoredValue stored = coord.put(request.getKey(),
                            request.getValue().toByteArray(),
                            mutationId(request.getClientId(), request.getSequence()));
                    responseObserver.onNext(
                            PutResponse.newBuilder().setVersion(stored.version()).build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    com.ledgerkv.transport.proto.NotLeader hint = hintFrom(e);
                    if (hint != null) {
                        responseObserver.onNext(PutResponse.newBuilder().setNotLeader(hint).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    failClientRequest(responseObserver, e);
                }
                return;
            }
            if (engine == null) {
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
            ClientCoordinator coord = requireCoordinator(responseObserver);
            if (coord != null) {
                // Replicate a tombstone across the key's replica set. Deleting only this node's
                // local engine and reporting success left the value readable from every other
                // replica.
                try {
                    boolean existed = coord.delete(request.getKey(),
                            mutationId(request.getClientId(), request.getSequence()));
                    responseObserver.onNext(
                            DeleteResponse.newBuilder().setExisted(existed).build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    com.ledgerkv.transport.proto.NotLeader hint = hintFrom(e);
                    if (hint != null) {
                        responseObserver.onNext(
                                DeleteResponse.newBuilder().setNotLeader(hint).build());
                        responseObserver.onCompleted();
                        return;
                    }
                    failClientRequest(responseObserver, e);
                }
                return;
            }
            if (engine == null) {
                return;
            }
            boolean existed = engine.get(request.getKey()).isPresent();
            engine.delete(request.getKey());
            responseObserver.onNext(DeleteResponse.newBuilder().setExisted(existed).build());
            responseObserver.onCompleted();
        }

        @Override
        public void scan(ScanRequest request, StreamObserver<ScanEntry> responseObserver) {
            ClientCoordinator coord = coordinator;
            if (engine == null || (coord != null && !coord.supportsScan())) {
                responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("scan is not available in this mode")
                        .asRuntimeException());
                return;
            }
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
                    List<StoredValue> siblings = live(StoredValueCodec.decodeAll(entry.value()));
                    if (siblings.isEmpty()) {
                        continue; // every version of this key is a tombstone
                    }
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
            if (engine == null) {
                responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("this node has no local replica store")
                        .asRuntimeException());
                return;
            }
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
            if (engine == null) {
                responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("this node has no local replica store")
                        .asRuntimeException());
                return;
            }
            storeVerbatim(request.getKey(), request.getValue());
            responseObserver.onNext(ReplicaPutResponse.newBuilder().setOk(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void deliverHint(HintRequest request, StreamObserver<HintResponse> responseObserver) {
            if (engine == null) {
                responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("this node has no local replica store")
                        .asRuntimeException());
                return;
            }
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

        /** The highest version stored for {@code key} including tombstones, or 0 if absent. */
        private long currentVersion(String key) {
            return store.get(key).stream().mapToLong(StoredValue::version).max().orElse(0L);
        }
    }
}
