package com.ledgerkv.raft.kv;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class KvCommandTest {

    @Test
    void putRoundTrips() {
        KvCommand c = KvCommand.put("client-A", 7, "k1", "v1".getBytes());
        KvCommand d = KvCommand.decode(c.encode());
        assertEquals(KvCommand.Op.PUT, d.op());
        assertEquals("client-A", d.clientId());
        assertEquals(7, d.sequenceNumber());
        assertEquals("k1", d.key());
        assertArrayEquals("v1".getBytes(), d.value());
    }

    @Test
    void deleteRoundTripsWithNullValue() {
        KvCommand c = KvCommand.delete("client-B", 3, "k2");
        KvCommand d = KvCommand.decode(c.encode());
        assertEquals(KvCommand.Op.DELETE, d.op());
        assertEquals("client-B", d.clientId());
        assertEquals(3, d.sequenceNumber());
        assertEquals("k2", d.key());
        assertNull(d.value());
    }

    @Test
    void encodingIsDeterministic() {
        KvCommand a = KvCommand.put("c", 1, "k", "v".getBytes());
        KvCommand b = KvCommand.put("c", 1, "k", "v".getBytes());
        assertTrue(Arrays.equals(a.encode(), b.encode()));
    }

    @Test
    void supportsBinaryValue() {
        byte[] raw = new byte[] {0, -1, 13, 127, -128};
        KvCommand c = KvCommand.put("c", 1, "k", raw);
        assertArrayEquals(raw, KvCommand.decode(c.encode()).value());
    }
}
