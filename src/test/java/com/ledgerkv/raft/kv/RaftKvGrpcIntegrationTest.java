package com.ledgerkv.raft.kv;

import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.raft.GrpcRaftPeer;
import com.ledgerkv.raft.RaftClient;
import com.ledgerkv.raft.RaftNode;
import com.ledgerkv.raft.RaftServer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RaftKvGrpcIntegrationTest {

    private static final class Group implements AutoCloseable {
        final Map<String, RaftNode> nodes = new HashMap<>();
        final Map<String, RaftServer> servers = new HashMap<>();
        final Map<String, RaftKvStateMachine> sms = new HashMap<>();
        final List<RaftClient> clients = new ArrayList<>();

        Group(List<String> ids, Map<String, Integer> timeouts) throws Exception {
            for (String id : ids) {
                List<String> peers = new ArrayList<>(ids);
                peers.remove(id);
                RaftKvStateMachine sm = new RaftKvStateMachine();
                sms.put(id, sm);
                RaftNode node = new RaftNode(id, peers, sm, () -> timeouts.get(id));
                nodes.put(id, node);
                servers.put(id, RaftServer.create(0, node).start());
            }
            for (String id : ids) {
                for (String other : ids) {
                    if (!other.equals(id)) {
                        RaftClient c = RaftClient.connect("localhost", servers.get(other).port());
                        clients.add(c);
                        nodes.get(id).registerPeer(new GrpcRaftPeer(other, c));
                    }
                }
            }
        }

        String awaitLeader(String candidate, int maxTicks) {
            for (int i = 0; i < maxTicks; i++) {
                nodes.get(candidate).tick();
                if (nodes.get(candidate).isLeader()) {
                    return candidate;
                }
            }
            throw new AssertionError(candidate + " not leader within " + maxTicks + " ticks");
        }

        @Override
        public void close() {
            for (RaftClient c : clients) {
                try { c.close(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
            for (RaftServer s : servers.values()) {
                try { s.close(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static Map<String, Integer> timeouts() {
        Map<String, Integer> t = new HashMap<>();
        t.put("n0", 2);
        t.put("n1", 8);
        t.put("n2", 12);
        return t;
    }

    @Test
    void linearizablePutGetDeleteOverGrpc() throws Exception {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        try (Group g = new Group(ids, timeouts())) {
            assertEquals("n0", g.awaitLeader("n0", 5));
            RaftNode leader = g.nodes.get("n0");
            Runnable drive = () -> {
                for (int i = 0; i < 3; i++) {
                    leader.tick();
                }
            };
            RaftKvClient client = new RaftKvClient("c1", leader, g.sms.get("n0"), drive);

            client.put("k", "v".getBytes());
            assertArrayEquals("v".getBytes(), client.get("k"));

            assertArrayEquals("v".getBytes(), client.delete("k"));
            assertNull(client.get("k"));
        }
    }

    @Test
    void atMostOnceAcrossTheWire() throws Exception {
        List<String> ids = Arrays.asList("n0", "n1", "n2");
        try (Group g = new Group(ids, timeouts())) {
            assertEquals("n0", g.awaitLeader("n0", 5));
            RaftNode leader = g.nodes.get("n0");
            Runnable drive = () -> {
                for (int i = 0; i < 3; i++) {
                    leader.tick();
                }
            };
            RaftKvClient c1 = new RaftKvClient("c1", leader, g.sms.get("n0"), drive);
            RaftKvClient c2 = new RaftKvClient("c2", leader, g.sms.get("n0"), drive);

            c1.putWithSequence(1, "k", "v1".getBytes());
            c2.putWithSequence(1, "k", "v2".getBytes());
            c1.putWithSequence(1, "k", "v1".getBytes()); // retry -> deduped
            assertArrayEquals("v2".getBytes(), c1.get("k"));
        }
    }
}
