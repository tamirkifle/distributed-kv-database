package com.ledgerkv.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.consistency.VersionMetadata;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives the internal replica RPCs ({@code ReplicaGet}/{@code ReplicaPut}/{@code DeliverHint})
 * over a real gRPC loopback channel, proving the coordinator-supplied {@link StoredValue} — value,
 * version, tombstone, and the full vector clock — round-trips client -> server -> client verbatim.
 */
class ReplicaRpcLoopbackTest {

    @TempDir
    Path dataDir;

    private NodeServer server;
    private NodeClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = NodeServer.builder(0).dataDir(dataDir).build();
        server.start();
        client = NodeClient.connect("localhost", server.port());
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
    void replicaPutThenReplicaGetRoundTripsFullVersionedValue() {
        VersionMetadata clock = VersionMetadata.initial("nodeA").increment("nodeB");
        client.replicaPut("k", new StoredValue("v".getBytes(UTF_8), 5, false, clock));

        List<StoredValue> read = client.replicaGet("k");
        assertEquals(1, read.size());
        StoredValue value = read.get(0);
        assertArrayEquals("v".getBytes(UTF_8), value.value());
        assertEquals(5, value.version());
        assertFalse(value.tombstone());
        assertEquals(clock.getVectorClock(), value.metadata().getVectorClock());
    }

    @Test
    void deliverHintStoresVerbatim() {
        VersionMetadata clock = VersionMetadata.initial("nodeA");
        client.deliverHint("itest-node-1", "h", new StoredValue("hv".getBytes(UTF_8), 2, false, clock));

        List<StoredValue> read = client.replicaGet("h");
        assertEquals(1, read.size());
        assertArrayEquals("hv".getBytes(UTF_8), read.get(0).value());
        assertEquals(2, read.get(0).version());
        assertEquals(clock.getVectorClock(), read.get(0).metadata().getVectorClock());
    }

    @Test
    void replicaGetAbsentReturnsEmpty() {
        assertTrue(client.replicaGet("missing").isEmpty());
    }
}
