package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.ledgerkv.storage.wal.DurabilityMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LsmEngineScanTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static LsmEngineConfig noFlush() {
        return new LsmEngineConfig(DurabilityMode.SYNC, Long.MAX_VALUE, null, 1L << 30, 50L);
    }

    private static LsmEngineConfig smallFlush() {
        return new LsmEngineConfig(DurabilityMode.SYNC, 64L, null, 1L << 30, 50L);
    }

    private static List<String> keys(Iterator<Entry> it) {
        List<String> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next().key());
        }
        return out;
    }

    @Test
    void scanReturnsSortedLiveEntriesInRange(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlush())) {
            engine.put("d", b("4"));
            engine.put("a", b("1"));
            engine.put("c", b("3"));
            engine.put("b", b("2"));
            engine.put("e", b("5"));
            assertEquals(List.of("b", "c", "d"), keys(engine.scan("b", "e")));
        }
    }

    @Test
    void scanExcludesTombstones(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, noFlush())) {
            engine.put("a", b("1"));
            engine.put("b", b("2"));
            engine.put("c", b("3"));
            engine.delete("b");
            assertEquals(List.of("a", "c"), keys(engine.scan(null, null)));
        }
    }

    @Test
    void scanMergesMemtableAndSstableInOrder(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("a", b("1"));
            engine.put("b", b("2"));
            engine.flush();                 // a,b in an SSTable
            engine.put("c", b("3"));        // c,d in the active MemTable
            engine.put("d", b("4"));
            assertEquals(List.of("a", "b", "c", "d"), keys(engine.scan(null, null)));
        }
    }

    @Test
    void scanNewestWinsAcrossSources(@TempDir Path dir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(dir, smallFlush())) {
            engine.put("k", b("old"));
            engine.flush();
            engine.put("k", b("new"));
            Iterator<Entry> it = engine.scan("k", null);
            assertTrue(it.hasNext());
            Entry e = it.next();
            assertEquals("k", e.key());
            assertArrayEquals(b("new"), e.value());
            assertFalse(it.hasNext());
        }
    }
}
