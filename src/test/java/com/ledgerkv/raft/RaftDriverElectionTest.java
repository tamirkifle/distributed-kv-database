package com.ledgerkv.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.raft.kv.KvCommand;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * A three-node gRPC group driven only by {@link RaftReplicationDriver} — no test thread pumps
 * {@code tick()}, so the driver has to elect a leader, replicate, and confirm reads on its own
 * threads the way a deployed node does.
 *
 * <p>What these tests are really checking is that peer I/O no longer runs under the node monitor.
 * Before the split, an election and a ReadIndex round both held the lock across every peer RPC in
 * turn; with a peer down, inbound RPCs on this node queued behind the dead peer's timeout.
 */
class RaftDriverElectionTest {

    private static final Duration TICK = Duration.ofMillis(20);

    private static final class Group implements AutoCloseable {
        final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        final Map<String, RaftKvStateMachine> states = new ConcurrentHashMap<>();
        final List<RaftServer> servers = new ArrayList<>();
        final List<RaftClient> clients = new ArrayList<>();
        final List<RaftReplicationDriver> drivers = new ArrayList<>();

        final Map<String, RaftServer> serversById = new ConcurrentHashMap<>();

        Group(List<String> ids) throws IOException {
            Map<String, Integer> ports = new ConcurrentHashMap<>();
            for (String id : ids) {
                List<String> peers = new ArrayList<>(ids);
                peers.remove(id);
                RaftKvStateMachine state = new RaftKvStateMachine();
                states.put(id, state);
                // Randomized in ticks, as a real deployment draws it: 8-16 ticks of 20ms.
                RaftNode node = new RaftNode(id, peers, state,
                        () -> ThreadLocalRandom.current().nextInt(8, 17));
                nodes.put(id, node);
                RaftServer server = RaftServer.create(0, node).start();
                servers.add(server);
                serversById.put(id, server);
                ports.put(id, server.port());
            }
            for (String id : ids) {
                List<RaftPeer> peers = new ArrayList<>();
                for (String other : ids) {
                    if (!other.equals(id)) {
                        RaftClient client = RaftClient.connect(
                                "localhost", ports.get(other), Duration.ofMillis(200));
                        clients.add(client);
                        GrpcRaftPeer peer = new GrpcRaftPeer(other, client);
                        nodes.get(id).registerPeer(peer);
                        peers.add(peer);
                    }
                }
                drivers.add(new RaftReplicationDriver(nodes.get(id), peers, TICK).start());
            }
        }

        RaftNode awaitLeader(Duration limit) throws InterruptedException {
            long deadline = System.nanoTime() + limit.toNanos();
            while (System.nanoTime() < deadline) {
                for (RaftNode node : nodes.values()) {
                    if (node.isLeader()) {
                        return node;
                    }
                }
                Thread.sleep(10);
            }
            return null;
        }

        List<RaftServer> serversExcept(String id) {
            List<RaftServer> rest = new ArrayList<>();
            serversById.forEach((owner, server) -> {
                if (!owner.equals(id)) {
                    rest.add(server);
                }
            });
            return rest;
        }

        RaftReplicationDriver driverFor(RaftNode node) {
            for (RaftReplicationDriver driver : drivers) {
                if (driver.nodeId().equals(node.nodeId())) {
                    return driver;
                }
            }
            throw new AssertionError("no driver for " + node.nodeId());
        }

        @Override
        public void close() {
            drivers.forEach(RaftReplicationDriver::close);
            for (RaftClient client : clients) {
                try { client.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            for (RaftServer server : servers) {
                try { server.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }

    @Test
    void driverElectsALeaderWithoutAnyExternalTick() throws Exception {
        try (Group group = new Group(List.of("n0", "n1", "n2"))) {
            RaftNode leader = group.awaitLeader(Duration.ofSeconds(10));
            assertNotNull(leader, "the driver's ticker must start and win an election on its own");

            // Every member converges on the same leader, which is what client routing depends on.
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline
                    && !group.nodes.values().stream()
                            .allMatch(n -> leader.nodeId().equals(n.leaderId()))) {
                Thread.sleep(10);
            }
            for (RaftNode node : group.nodes.values()) {
                assertEquals(leader.nodeId(), node.leaderId(),
                        node.nodeId() + " must be able to name the leader");
            }
        }
    }

    @Test
    void proposalCommitsAndReadIndexServesIt() throws Exception {
        try (Group group = new Group(List.of("n0", "n1", "n2"))) {
            RaftNode leader = group.awaitLeader(Duration.ofSeconds(10));
            assertNotNull(leader);
            RaftReplicationDriver driver = group.driverFor(leader);

            driver.propose(KvCommand.put("c1", 1, "k", "v".getBytes(UTF_8)).encode(),
                    Duration.ofSeconds(5));

            long readIndex = driver.readIndex(Duration.ofSeconds(5));
            assertTrue(readIndex > 0, "a healthy majority must confirm the leader");
            assertArrayEqualsUtf8("v", group.states.get(leader.nodeId()).get("k"));
        }
    }

    @Test
    void isolatedLeaderRefusesReadsOnceItCannotReachAMajority() throws Exception {
        try (Group group = new Group(List.of("n0", "n1", "n2"))) {
            RaftNode leader = group.awaitLeader(Duration.ofSeconds(10));
            assertNotNull(leader);
            RaftReplicationDriver driver = group.driverFor(leader);
            assertTrue(driver.readIndex(Duration.ofSeconds(5)) >= 0, "healthy read must succeed");

            // Take both followers away. The leader does not know it has been superseded, and that
            // is exactly the case ReadIndex step 3 exists to catch.
            for (RaftServer server : group.serversExcept(leader.nodeId())) {
                server.close();
            }

            assertEquals(-1, driver.readIndex(Duration.ofSeconds(2)),
                    "no majority can acknowledge, so the read must be refused rather than served");
        }
    }

    private static void assertArrayEqualsUtf8(String expected, byte[] actual) {
        assertNotNull(actual, "expected " + expected + " but the key was absent");
        assertEquals(expected, new String(actual, UTF_8));
    }
}
