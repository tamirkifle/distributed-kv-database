package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.transport.NodeClient;
import com.ledgerkv.transport.NodeServer;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link GrpcReplicaClient} round-trips a {@link VersionedValue} — value + version + vector clock —
 * through the internal replica RPCs against a real loopback {@link NodeServer}.
 */
class GrpcReplicaClientTest {

    @TempDir
    Path dataDir;

    private NodeServer server;
    private NodeClient client;
    private GrpcReplicaClient replica;

    @BeforeEach
    void setUp() throws Exception {
        server = NodeServer.builder(0).dataDir(dataDir).build();
        server.start();
        client = NodeClient.connect("localhost", server.port());
        replica = new GrpcReplicaClient("itest-node-0", client);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void putThenGetRoundTripsOverGrpc() {
        VersionMetadata clock = VersionMetadata.initial("nA").increment("nB");
        replica.put("k", new VersionedValue("v", 4, clock));

        Optional<VersionedValue> read = replica.get("k");
        assertTrue(read.isPresent());
        assertEquals("v", read.get().getValue());
        assertEquals(4, read.get().getVersion());
        assertEquals(clock.getVectorClock(), read.get().getVersionMetadata().getVectorClock());
        assertEquals("itest-node-0", replica.nodeId());
    }

    @Test
    void deliverHintRoundTripsOverGrpc() {
        VersionMetadata clock = VersionMetadata.initial("nA");
        replica.deliverHint("h", new VersionedValue("hv", 1, clock));

        Optional<VersionedValue> read = replica.get("h");
        assertTrue(read.isPresent());
        assertEquals("hv", read.get().getValue());
        assertEquals(clock.getVectorClock(), read.get().getVersionMetadata().getVectorClock());
    }
}
