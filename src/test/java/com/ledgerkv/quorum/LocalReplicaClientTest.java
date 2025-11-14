package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.VersionedValue;
import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.storage.lsm.LsmEngine;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link LocalReplicaClient} stores a {@link VersionedValue} verbatim through a per-node
 * {@link LsmEngine} (the honest local-node storage), preserving value, version, and vector clock.
 */
class LocalReplicaClientTest {

    @TempDir
    Path dir;

    private LsmEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        engine = LsmEngine.open(dir);
    }

    @AfterEach
    void tearDown() throws Exception {
        engine.close();
    }

    @Test
    void putThenGetRoundTripsVersionedValue() {
        LocalReplicaClient client = new LocalReplicaClient("n0", engine);
        VersionMetadata clock = VersionMetadata.initial("n0");
        client.put("k", new VersionedValue("v", 3, clock));

        Optional<VersionedValue> read = client.get("k");
        assertTrue(read.isPresent());
        assertEquals("v", read.get().getValue());
        assertEquals(3, read.get().getVersion());
        assertEquals(clock.getVectorClock(), read.get().getVersionMetadata().getVectorClock());
        assertFalse(client.get("absent").isPresent());
    }

    @Test
    void deliverHintStoresVerbatim() {
        LocalReplicaClient client = new LocalReplicaClient("n0", engine);
        VersionMetadata clock = VersionMetadata.initial("nA").increment("nB");
        client.deliverHint("k2", new VersionedValue("v2", 5, clock));

        Optional<VersionedValue> read = client.get("k2");
        assertTrue(read.isPresent());
        assertEquals("v2", read.get().getValue());
        assertEquals(clock.getVectorClock(), read.get().getVersionMetadata().getVectorClock());
    }
}
