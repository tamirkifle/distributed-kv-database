package com.ledgerkv.raft;

import com.ledgerkv.transport.proto.LedgerKvRaftGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Blocking client for the {@code LedgerKvRaft} service (mirrors {@code NodeClient}).
 *
 * <p>Every call carries an explicit deadline. Without one a blocking stub waits indefinitely, so a
 * peer that accepts a connection and then stops responding stalls the caller forever. That is how
 * a single unresponsive follower could hold a Raft leader's replication round open. A deadline
 * turns that into an ordinary failed RPC, which the
 * node already treats as a dropped message and retries on the next heartbeat.
 */
public final class RaftClient implements AutoCloseable {

    /** Default per-RPC deadline: generous next to an election timeout, finite next to a hang. */
    private static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(2);

    private final ManagedChannel channel;
    private final LedgerKvRaftGrpc.LedgerKvRaftBlockingStub stub;
    private final Duration deadline;

    private RaftClient(ManagedChannel channel, Duration deadline) {
        this.channel = channel;
        this.stub = LedgerKvRaftGrpc.newBlockingStub(channel);
        this.deadline = deadline;
    }

    public static RaftClient connect(String host, int port) {
        return connect(host, port, DEFAULT_DEADLINE);
    }

    public static RaftClient connect(String host, int port, Duration deadline) {
        return new RaftClient(
                NettyChannelBuilder.forAddress(host, port).usePlaintext().build(), deadline);
    }

    /** The stub with this client's per-RPC deadline applied (deadlines are per-call, not per-stub). */
    private LedgerKvRaftGrpc.LedgerKvRaftBlockingStub stub() {
        return stub.withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }

    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return RaftProtos.fromProto(stub().requestVote(RaftProtos.toProto(request)));
    }

    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return RaftProtos.fromProto(stub().appendEntries(RaftProtos.toProto(request)));
    }

    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        return RaftProtos.fromProto(stub().installSnapshot(RaftProtos.toProto(request)));
    }

    @Override
    public void close() throws InterruptedException {
        channel.shutdown();
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
        }
    }
}
