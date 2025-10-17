package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class MemTableTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    @Test
    void entriesAreReturnedInSortedKeyOrder() {
        MemTable mt = new MemTable();
        mt.put("banana", b("1"), 1L);
        mt.put("apple", b("2"), 2L);
        mt.put("cherry", b("3"), 3L);

        List<String> keys = new ArrayList<>();
        for (Entry e : mt.entries()) {
            keys.add(e.key());
        }
        assertEquals(Arrays.asList("apple", "banana", "cherry"), keys);
    }

    @Test
    void latestWriteForAKeyWins() {
        MemTable mt = new MemTable();
        mt.put("k", b("old"), 1L);
        mt.put("k", b("new"), 2L);
        assertArrayEquals(b("new"), mt.get("k").value());
        assertEquals(2L, mt.get("k").sequence());
    }

    @Test
    void deleteStoresTombstone() {
        MemTable mt = new MemTable();
        mt.put("k", b("v"), 1L);
        mt.delete("k", 2L);
        assertTrue(mt.get("k").isTombstone());
    }

    @Test
    void getOfAbsentKeyReturnsNull() {
        assertNull(new MemTable().get("nope"));
    }

    @Test
    void approximateSizeGrowsWithWritesAndOverwriteDoesNotDoubleCount() {
        MemTable mt = new MemTable();
        assertTrue(mt.isEmpty());
        mt.put("k", b("vvvvv"), 1L);
        long afterFirst = mt.approximateSizeBytes();
        assertTrue(afterFirst > 0);
        assertFalse(mt.isEmpty());

        mt.put("k", b("vvvvv"), 2L); // overwrite same key, same value size
        assertEquals(afterFirst, mt.approximateSizeBytes(), "overwrite must not double-count");

        mt.put("k2", b("vvvvv"), 3L);
        assertTrue(mt.approximateSizeBytes() > afterFirst);
    }

    @Test
    void sealedMemTableRejectsWrites() {
        MemTable mt = new MemTable();
        mt.put("k", b("v"), 1L);
        mt.seal();
        assertTrue(mt.isSealed());
        assertThrows(IllegalStateException.class, () -> mt.put("k2", b("v"), 2L));
        assertThrows(IllegalStateException.class, () -> mt.delete("k", 3L));
    }
}
