package com.ledgerkv.raft;

import com.ledgerkv.transport.proto.AppendEntriesResponse;
import com.ledgerkv.transport.proto.LedgerKvRaftGrpc;
import com.ledgerkv.transport.proto.RequestVoteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** A gRPC server exposing the {@code LedgerKvRaft} service for one local {@link RaftNode}. */
public final class RaftServer implements AutoCloseable {

    private final Server server;

    private RaftServer(int port, RaftNode node) {
        this.server = ServerBuilder.forPort(port).addService(new Service(node)).build();
    }

    public static RaftServer create(int port, RaftNode node) {
        return new RaftServer(port, node);
    }

    public RaftServer start() throws IOException {
        server.start();
        return this;
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void close() throws InterruptedException {
        server.shutdown();
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
    }

    private static final class Service extends LedgerKvRaftGrpc.LedgerKvRaftImplBase {
        private final RaftNode node;

        Service(RaftNode node) {
            this.node = node;
        }

        @Override
        public void requestVote(com.ledgerkv.transport.proto.RequestVoteRequest request,
                StreamObserver<RequestVoteResponse> obs) {
            obs.onNext(RaftProtos.toProto(node.handleRequestVote(RaftProtos.fromProto(request))));
            obs.onCompleted();
        }

        @Override
        public void appendEntries(com.ledgerkv.transport.proto.AppendEntriesRequest request,
                StreamObserver<AppendEntriesResponse> obs) {
            obs.onNext(RaftProtos.toProto(node.handleAppendEntries(RaftProtos.fromProto(request))));
            obs.onCompleted();
        }

        @Override
        public void installSnapshot(com.ledgerkv.transport.proto.InstallSnapshotRequest request,
                StreamObserver<com.ledgerkv.transport.proto.InstallSnapshotResponse> obs) {
            obs.onNext(RaftProtos.toProto(node.handleInstallSnapshot(RaftProtos.fromProto(request))));
            obs.onCompleted();
        }
    }
}
