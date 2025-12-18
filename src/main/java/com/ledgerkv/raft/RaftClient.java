package com.ledgerkv.raft;

import com.ledgerkv.transport.proto.LedgerKvRaftGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

/** Blocking client for the {@code LedgerKvRaft} service (mirrors {@code NodeClient}). */
public final class RaftClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final LedgerKvRaftGrpc.LedgerKvRaftBlockingStub stub;

    private RaftClient(ManagedChannel channel) {
        this.channel = channel;
        this.stub = LedgerKvRaftGrpc.newBlockingStub(channel);
    }

    public static RaftClient connect(String host, int port) {
        return new RaftClient(NettyChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return RaftProtos.fromProto(stub.requestVote(RaftProtos.toProto(request)));
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return RaftProtos.fromProto(stub.appendEntries(RaftProtos.toProto(request)));
    }

    @Override
    public void close() throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
        }
    }
}
