package com.ledgerkv.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In-JVM gRPC Raft group test harness: one RaftServer/RaftNode per id, wired over real loopback. */
final class RaftCluster implements AutoCloseable {

    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final Map<String, RaftServer> servers = new HashMap<>();
    private final Map<String, List<RaftClient>> clients = new HashMap<>();
    private final Map<String, Recorder> recorders = new HashMap<>();

    static final class Recorder implements StateMachine {
        final List<String> applied = new ArrayList<>();
        @Override public synchronized byte[] apply(byte[] command) {
            applied.add(new String(command));
            return command;
        }
    }

    RaftCluster(List<String> ids, Map<String, Integer> timeouts) throws IOException {
        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids);
            peers.remove(id);
            Recorder rec = new Recorder();
            recorders.put(id, rec);
            RaftNode node = new RaftNode(id, peers, rec, () -> timeouts.get(id));
            nodes.put(id, node);
            servers.put(id, RaftServer.create(0, node).start());
            clients.put(id, new ArrayList<>());
        }
        for (String id : ids) {
            for (String other : ids) {
                if (!other.equals(id)) {
                    RaftClient c = RaftClient.connect("localhost", servers.get(other).port());
                    clients.get(id).add(c);
                    nodes.get(id).registerPeer(new GrpcRaftPeer(other, c));
                }
            }
        }
    }

    RaftNode node(String id) {
        return nodes.get(id);
    }

    List<String> appliedAt(String id) {
        return recorders.get(id).applied;
    }

    String awaitLeader(String candidate, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            nodes.get(candidate).tick();
            if (nodes.get(candidate).isLeader()) {
                return candidate;
            }
        }
        throw new AssertionError(candidate + " did not become leader within " + maxTicks + " ticks");
    }

    void tickLeader(String leaderId, int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.get(leaderId).tick();
        }
    }

    void crash(String id) throws InterruptedException {
        servers.get(id).close();
    }

    @Override
    public void close() {
        for (List<RaftClient> cs : clients.values()) {
            for (RaftClient c : cs) {
                try { c.close(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
        }
        for (RaftServer s : servers.values()) {
            try { s.close(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }
}
