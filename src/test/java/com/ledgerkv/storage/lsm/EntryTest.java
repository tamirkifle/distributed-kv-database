package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class EntryTest {

    @Test
    void putEntryHoldsValueAndIsNotTombstone() {
        Entry e = Entry.put("k", "v".getBytes(UTF_8), 7L);
        assertEquals("k", e.key());
        assertArrayEquals("v".getBytes(UTF_8), e.value());
        assertEquals(7L, e.sequence());
        assertFalse(e.isTombstone());
    }

    @Test
    void tombstoneHasNullValueAndIsTombstone() {
        Entry e = Entry.tombstone("k", 9L);
        assertNull(e.value());
        assertTrue(e.isTombstone());
        assertEquals(9L, e.sequence());
    }

    @Test
    void emptyValueIsDistinctFromTombstone() {
        Entry put = Entry.put("k", new byte[0], 1L);
        Entry tomb = Entry.tombstone("k", 1L);
        assertFalse(put.isTombstone());
        assertTrue(tomb.isTombstone());
        assertNotEquals(put, tomb);
    }

    @Test
    void equalsIsValueBasedAndSequenceSensitive() {
        assertEquals(Entry.put("k", "v".getBytes(UTF_8), 1L),
                Entry.put("k", "v".getBytes(UTF_8), 1L));
        assertNotEquals(Entry.put("k", "v".getBytes(UTF_8), 1L),
                Entry.put("k", "v".getBytes(UTF_8), 2L));
    }
}
