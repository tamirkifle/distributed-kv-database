package com.ledgerkv.transport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.transport.proto.ScanEntry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end loopback test that drives a real gRPC {@link NodeServer} over the wire
 * via {@link NodeClient}. Proves the transport substrate round-trips get/put/delete/scan
 * against the temporary in-memory store (replaced by the LSM engine in 2b).
 */
class NodeServerLoopbackTest {

    private NodeServer server;
    private NodeClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = NodeServer.builder(0).build();
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
    void roundTripsGetPutDeleteScan() {
        // Absent key reads empty.
        assertFalse(client.get("absent").isPresent());

        // Put returns a version, get reads the value back.
        long v1 = client.put("k1", bytes("v1"));
        assertTrue(v1 >= 1, "put version should be >= 1 but was " + v1);
        Optional<byte[]> read = client.get("k1");
        assertTrue(read.isPresent());
        assertArrayEquals(bytes("v1"), read.get());

        // A second key, then scan returns both in ascending key order.
        client.put("k2", bytes("v2"));
        List<ScanEntry> scanned = client.scan("k1", "k3", 10);
        List<String> keys = scanned.stream().map(ScanEntry::getKey).collect(Collectors.toList());
        assertEquals(List.of("k1", "k2"), keys);

        // Delete removes the key.
        assertTrue(client.delete("k1"));
        assertFalse(client.get("k1").isPresent());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
