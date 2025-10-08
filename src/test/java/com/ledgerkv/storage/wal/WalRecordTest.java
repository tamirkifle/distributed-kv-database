package com.ledgerkv.storage.wal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WalRecordTest {

    @Test
    void putRecordRoundTrips() {
        WalRecord r = WalRecord.put("alpha", "v1".getBytes(UTF_8));
        WalRecord decoded = WalRecord.decode(r.encode());

        assertEquals(RecordType.PUT, decoded.type());
        assertEquals("alpha", decoded.key());
        assertArrayEquals("v1".getBytes(UTF_8), decoded.value());
        assertEquals(r, decoded);
    }

    @Test
    void deleteRecordRoundTrips() {
        WalRecord r = WalRecord.delete("alpha");
        WalRecord decoded = WalRecord.decode(r.encode());

        assertEquals(RecordType.DELETE, decoded.type());
        assertEquals("alpha", decoded.key());
        assertNull(decoded.value());
        assertEquals(r, decoded);
    }

    @Test
    void emptyValueIsDistinctFromDelete() {
        WalRecord put = WalRecord.put("k", new byte[0]);
        WalRecord del = WalRecord.delete("k");

        assertNotEquals(put, del);
        assertArrayEquals(new byte[0], WalRecord.decode(put.encode()).value());
        assertNull(WalRecord.decode(del.encode()).value());
    }
}
