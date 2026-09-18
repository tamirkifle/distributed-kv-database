package com.ledgerkv.node;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.transport.LedgerKvClusterClient;
import com.ledgerkv.transport.MutationId;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import com.ledgerkv.transport.NotLeaderException;
import com.ledgerkv.transport.StoredValue;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A real three-node Raft deployment in one JVM: {@link RaftRuntime} per node over loopback gRPC,
 * each behind the same storage-free {@link NodeServer} that {@code NodeMain} builds, driven through
 * the ordinary {@link NodeClient}. Nothing here reaches past the public surface.
 */
class RaftModeEndToEndTest {

    private static final Duration SETTLE = Duration.ofSeconds(15);

    /**
     * Ports are picked by binding and releasing, because a Raft member has to publish its peer
     * address before its peers start. Port 0 resolves too late for that.
     *
     * <p>That leaves a window, and on a busy machine something else wins it: a run of this test
     * once reached a member whose port had been taken by an unrelated HTTP server and got back
     * {@code UNIMPLEMENTED: HTTP status code 404}. {@link Cluster#build} closes the window by
     * checking every member is really ours and starting over if one is not.
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    /** Thrown when a port we reserved turns out to be serving something that is not LedgerKV. */
    private static final class PortStolenException extends RuntimeException {
        PortStolenException(String message) {
            super(message);
        }
    }

    private static final class Cluster implements AutoCloseable {
        final List<RaftRuntime> runtimes = new ArrayList<>();
        final List<NodeServer> servers = new ArrayList<>();
        final List<NodeClient> clients = new ArrayList<>();
        final Map<String, Integer> clientIndexById = new LinkedHashMap<>();
        final List<String> endpoints = new ArrayList<>();
        final java.util.Set<String> stopped = new java.util.HashSet<>();

        Cluster(Path root, int size) throws IOException {
            this(root, allocate(size), allocate(size));
        }

        private static List<Integer> allocate(int size) throws IOException {
            List<Integer> ports = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                ports.add(freePort());
            }
            return ports;
        }

        Cluster(Path root, List<Integer> clientPorts, List<Integer> raftPorts) throws IOException {
            int size = clientPorts.size();
            List<String> peers = new ArrayList<>();
            List<String> raftPeers = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                peers.add("localhost:" + clientPorts.get(i));
                raftPeers.add("localhost:" + raftPorts.get(i));
            }
            endpoints.addAll(peers);

            for (int i = 0; i < size; i++) {
                Map<String, String> env = new HashMap<>();
                env.put("LEDGERKV_MODE", "raft");
                env.put("LEDGERKV_NODE_INDEX", String.valueOf(i));
                env.put("LEDGERKV_PEERS", String.join(",", peers));
                env.put("LEDGERKV_RAFT_PEERS", String.join(",", raftPeers));
                env.put("LEDGERKV_GRPC_PORT", String.valueOf(clientPorts.get(i)));
                env.put("LEDGERKV_RAFT_PORT", String.valueOf(raftPorts.get(i)));
                env.put("LEDGERKV_DATA_DIR", root.resolve("node" + i).toString());
                // Tighter than production so an election resolves inside a test's patience.
                env.put("LEDGERKV_RAFT_HEARTBEAT_MS", "20");
                env.put("LEDGERKV_RAFT_ELECTION_MIN_TICKS", "5");
                env.put("LEDGERKV_RAFT_ELECTION_MAX_TICKS", "12");
                env.put("LEDGERKV_RAFT_RPC_DEADLINE_MS", "80");
                env.put("LEDGERKV_REQUEST_DEADLINE_MS", "5000");

                NodeConfig config = NodeConfig.fromEnv(env);
                RaftRuntime runtime = RaftRuntime.start(config);
                runtimes.add(runtime);

                NodeServer server =
                        NodeServer.builder(config.grpcPort()).coordinatorOnly().build().start();
                server.useCoordinator(new RaftClientCoordinator(runtime.node(), runtime.driver(),
                        runtime.state(), config.requestDeadline(), runtime.clientEndpoints()));
                servers.add(server);
                clients.add(NodeClient.connect("localhost", clientPorts.get(i)));
                clientIndexById.put(config.nodeId(), i);
            }
        }

        /**
         * Confirms each published endpoint is one of our own servers. A foreign process on a
         * reserved port answers a gRPC call with UNIMPLEMENTED or UNKNOWN; ours answers, or
         * redirects because it is not the leader.
         */
        void verifyMembersAreOurs() {
            for (int i = 0; i < clients.size(); i++) {
                try {
                    clients.get(i).getSiblings("__probe__");
                } catch (NotLeaderException ours) {
                    continue; // a follower redirecting is proof enough that it is ours
                } catch (StatusRuntimeException e) {
                    Status.Code code = e.getStatus().getCode();
                    if (code == Status.Code.UNIMPLEMENTED || code == Status.Code.UNKNOWN) {
                        throw new PortStolenException(
                                endpoints.get(i) + " is not a LedgerKV node: " + e.getStatus());
                    }
                }
            }
        }

        /** Blocks until a leader exists and every member agrees who it is. */
        NodeClient awaitLeaderClient() throws InterruptedException {
            long deadline = System.nanoTime() + SETTLE.toNanos();
            while (System.nanoTime() < deadline) {
                String leader = runtimes.get(0).node().leaderId();
                boolean settled = leader != null && runtimes.stream()
                        .allMatch(r -> leader.equals(r.node().leaderId()));
                if (settled) {
                    return clients.get(clientIndexById.get(leader));
                }
                Thread.sleep(20);
            }
            throw new AssertionError("no leader within " + SETTLE);
        }

        String leaderEndpoint() throws InterruptedException {
            awaitLeaderClient();
            return endpoints.get(clientIndexById.get(runtimes.get(0).node().leaderId()));
        }

        /** Stops the current leader outright, the way a SIGKILL would. */
        void stopLeader() throws Exception {
            stop(clientIndexById.get(awaitSettledLeader()));
        }

        private void stop(int index) throws Exception {
            servers.get(index).close();
            runtimes.get(index).close();
            stopped.add(runtimes.get(index).node().nodeId());
        }

        String awaitSettledLeader() throws InterruptedException {
            awaitLeaderClient();
            return runtimes.get(0).node().leaderId();
        }

        /**
         * Stops every member but the leader and one follower, then returns a client pointed at the
         * stranded leader. Two of five cannot reach a majority.
         */
        NodeClient stripToMinorityAroundLeader() throws Exception {
            String leader = awaitSettledLeader();
            int leaderIndex = clientIndexById.get(leader);
            int spared = leaderIndex == 0 ? 1 : 0;
            for (int i = 0; i < runtimes.size(); i++) {
                if (i != leaderIndex && i != spared) {
                    stop(i);
                }
            }
            return clients.get(leaderIndex);
        }

        RaftRuntime runtimeFor(String nodeId) {
            return runtimes.get(clientIndexById.get(nodeId));
        }

        /** The runtimes that have not been stopped. */
        List<RaftRuntime> runningRuntimes() {
            List<RaftRuntime> running = new ArrayList<>();
            for (RaftRuntime runtime : runtimes) {
                if (!stopped.contains(runtime.node().nodeId())) {
                    running.add(runtime);
                }
            }
            return running;
        }

        NodeClient anyFollowerClient() throws InterruptedException {
            awaitLeaderClient();
            String leader = runtimes.get(0).node().leaderId();
            for (Map.Entry<String, Integer> entry : clientIndexById.entrySet()) {
                if (!entry.getKey().equals(leader)) {
                    return clients.get(entry.getValue());
                }
            }
            throw new AssertionError("a three-node group always has a follower");
        }

        @Override
        public void close() {
            clients.forEach(c -> {
                try { c.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            servers.forEach(s -> {
                try { s.close(); } catch (Exception e) { /* best effort, may already be stopped */ }
            });
            runtimes.forEach(RaftRuntime::close); // idempotent: some may already be stopped
        }
    }

    /**
     * Builds a cluster, retrying if a reserved port was taken by a foreign process. Three attempts
     * because losing the race twice in a row is already implausible; losing it three times means
     * something other than luck and the test should say so rather than hang.
     */
    private static Cluster build(Path root, int size) throws IOException {
        PortStolenException last = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            Cluster cluster = new Cluster(root.resolve("attempt" + attempt), size);
            try {
                cluster.verifyMembersAreOurs();
                return cluster;
            } catch (PortStolenException stolen) {
                last = stolen;
                cluster.close();
            }
        }
        throw new AssertionError("could not get a clean set of ports in three attempts", last);
    }

    @Test
    void servesBinaryValuesAndDeletesThroughTheLeader(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient leader = cluster.awaitLeaderClient();
            byte[] binary = new byte[] {0, 1, 2, (byte) 0xff, 0, 'x'};

            long version = leader.put("k", binary, MutationId.of("c1", 1));
            assertTrue(version > 0, "a Raft version is the applied log index");

            List<StoredValue> read = leader.getSiblings("k");
            assertEquals(1, read.size(), "consensus leaves no siblings to resolve");
            assertArrayEquals(binary, read.get(0).value());
            assertEquals(version, read.get(0).version());

            assertTrue(leader.delete("k", MutationId.of("c1", 2)));
            assertTrue(leader.getSiblings("k").isEmpty());
            assertFalse(leader.delete("k", MutationId.of("c1", 3)), "already gone");
        }
    }

    @Test
    void aFollowerRedirectsRatherThanServing(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient follower = cluster.anyFollowerClient();

            NotLeaderException redirect = assertThrows(NotLeaderException.class,
                    () -> follower.put("k", "v".getBytes(UTF_8), MutationId.of("c1", 1)));

            assertNotNull(redirect.leaderId(), "the follower knows who leads");
            assertNotNull(redirect.leaderEndpoint(), "and where to reach them");

            // Following the hint must land on a node that actually serves the write.
            String[] hostPort = redirect.leaderEndpoint().split(":", 2);
            try (NodeClient leader =
                    NodeClient.connect(hostPort[0], Integer.parseInt(hostPort[1]))) {
                assertTrue(leader.put("k", "v".getBytes(UTF_8), MutationId.of("c1", 1)) > 0);
            }
        }
    }

    @Test
    void aRetriedMutationIsNotAppliedTwice(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient leader = cluster.awaitLeaderClient();
            leader.put("counter", "first".getBytes(UTF_8), MutationId.of("c1", 1));

            // The same id again is the retry a client makes after a timeout it cannot interpret.
            long replayed = leader.put("counter", "first".getBytes(UTF_8), MutationId.of("c1", 1));

            assertEquals(1, leader.getSiblings("counter").size());
            assertArrayEquals("first".getBytes(UTF_8), leader.getSiblings("counter").get(0).value());
            assertTrue(replayed > 0, "the retry is answered from the session record, not refused");
        }
    }

    @Test
    void reusingASequenceForADifferentValueIsRejected(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient leader = cluster.awaitLeaderClient();
            leader.put("k", "first".getBytes(UTF_8), MutationId.of("c1", 1));

            StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                    () -> leader.put("k", "second".getBytes(UTF_8), MutationId.of("c1", 1)));

            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode(),
                    "retrying this unchanged cannot help, so it must not look retriable");
            assertArrayEquals("first".getBytes(UTF_8), leader.getSiblings("k").get(0).value());
        }
    }

    @Test
    void aMutationWithoutAClientIdIsRefused(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient leader = cluster.awaitLeaderClient();

            StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                    () -> leader.put("k", "v".getBytes(UTF_8)));

            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
            assertTrue(e.getStatus().getDescription().contains("client_id"),
                    e.getStatus().getDescription());
        }
    }

    @Test
    void scanReportsThatRaftModeCannotServeIt(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            NodeClient leader = cluster.awaitLeaderClient();

            StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                    () -> leader.scan("a", "z", 10));

            assertEquals(Status.Code.UNIMPLEMENTED, e.getStatus().getCode(),
                    "an empty stream would read as 'no keys in range', which is a different claim");
        }
    }

    @Test
    void theClusterClientFindsTheLeaderFromAnyStartingPoint(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 3)) {
            cluster.awaitLeaderClient(); // let the group settle before pointing a client at it
            try (LedgerKvClusterClient client = LedgerKvClusterClient.connect(
                    cluster.endpoints, "app-1", Duration.ofSeconds(10))) {

                assertTrue(client.put("k", "v".getBytes(UTF_8)) > 0);
                assertArrayEquals("v".getBytes(UTF_8), client.get("k").orElseThrow(AssertionError::new));
                assertTrue(client.delete("k"));
                assertFalse(client.get("k").isPresent());

                assertEquals(cluster.leaderEndpoint(), client.preferredEndpoint(),
                        "the client should have settled on the leader, not kept sweeping");
            }
        }
    }

    @Test
    void theClusterClientKeepsWritingAcrossALeaderLoss(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 5)) {
            cluster.awaitLeaderClient();
            try (LedgerKvClusterClient client = LedgerKvClusterClient.connect(
                    cluster.endpoints, "app-1", Duration.ofSeconds(20))) {

                client.put("before", "1".getBytes(UTF_8));
                cluster.stopLeader();

                // The surviving majority elects a new leader; the client has to find it by itself.
                client.put("after", "2".getBytes(UTF_8));

                assertArrayEquals("1".getBytes(UTF_8),
                        client.get("before").orElseThrow(AssertionError::new));
                assertArrayEquals("2".getBytes(UTF_8),
                        client.get("after").orElseThrow(AssertionError::new));
            }
        }
    }

    @Test
    void aMinorityRefusesRatherThanServingStaleData(@TempDir Path root) throws Exception {
        try (Cluster cluster = build(root, 5)) {
            cluster.awaitLeaderClient();
            try (LedgerKvClusterClient client = LedgerKvClusterClient.connect(
                    cluster.endpoints, "app-1", Duration.ofSeconds(10))) {
                client.put("k", "committed".getBytes(UTF_8));
            }

            // Strand the leader itself with one follower: two of five, below the three needed to
            // commit. Keeping the leader is the point. It has the committed value in memory and
            // still believes it leads, so it is the node most likely to answer when it must not.
            NodeClient stranded = cluster.stripToMinorityAroundLeader();

            NotLeaderException read = assertThrows(NotLeaderException.class,
                    () -> stranded.getSiblings("k"),
                    "no majority can confirm leadership, so the read must be refused");
            assertNull(read.leaderEndpoint(),
                    "and it must not name itself: it cannot know it is still the leader");

            // Not a redirect, and deliberately so. The entry is in this leader's log and could
            // still commit if the majority came back, so the outcome is unknown rather than
            // misdirected. UNAVAILABLE says "retry"; a NotLeader hint would claim knowledge the
            // node does not have.
            StatusRuntimeException write = assertThrows(StatusRuntimeException.class,
                    () -> stranded.put("k", "minority".getBytes(UTF_8), MutationId.of("c9", 1)),
                    "a minority cannot commit, so it must not acknowledge the write");
            assertEquals(Status.Code.UNAVAILABLE, write.getStatus().getCode());

            // Whatever happened to that proposal, it must not be visible as a committed value.
            assertThrows(NotLeaderException.class, () -> stranded.getSiblings("k"));

            // Readiness has to follow, or the node keeps collecting traffic it cannot serve.
            RaftRuntime strandedLeader = cluster.runtimeFor(cluster.awaitSettledLeader());
            assertFalse(strandedLeader.ready(),
                    "a leader without majority support must drop out of the client Service");

            // The surviving follower is a known gap, pinned here rather than left to drift. It is
            // still being heartbeated by the stranded leader, so it reports ready and will redirect
            // there; the leader then refuses. That costs a wasted hop, never a stale answer, and
            // closing it would mean a leadership round trip on every readiness probe.
            for (RaftRuntime runtime : cluster.runningRuntimes()) {
                if (runtime != strandedLeader) {
                    assertTrue(runtime.ready(),
                            "a follower cannot tell that the leader heartbeating it is stranded");
                }
            }
        }
    }

    @Test
    void acknowledgedWritesSurviveAFullRestart(@TempDir Path root) throws Exception {
        // Ports are reused across both lifetimes so the restarted group is the same cluster.
        List<Integer> clientPorts = new ArrayList<>();
        List<Integer> raftPorts = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            clientPorts.add(freePort());
            raftPorts.add(freePort());
        }

        try (Cluster first = new Cluster(root, clientPorts, raftPorts)) {
            NodeClient leader = first.awaitLeaderClient();
            leader.put("durable", "value".getBytes(UTF_8), MutationId.of("c1", 1));
        }

        try (Cluster second = new Cluster(root, clientPorts, raftPorts)) {
            NodeClient leader = second.awaitLeaderClient();
            List<StoredValue> read = leader.getSiblings("durable");
            assertEquals(1, read.size(), "the write was acknowledged, so it must be here");
            assertArrayEquals("value".getBytes(UTF_8), read.get(0).value());
        }
    }
}
