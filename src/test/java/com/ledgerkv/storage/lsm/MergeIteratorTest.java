package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class MergeIteratorTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static List<Entry> drain(MergeIterator it) {
        List<Entry> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    @Test
    void mergesTwoRunsInKeyOrder() {
        Iterator<Entry> a = Arrays.asList(
                Entry.put("a", b("1"), 1),
                Entry.put("c", b("3"), 3)).iterator();
        Iterator<Entry> c = Arrays.asList(
                Entry.put("b", b("2"), 2),
                Entry.put("d", b("4"), 4)).iterator();

        List<Entry> merged = drain(new MergeIterator(Arrays.asList(a, c), false));

        assertEquals(Arrays.asList("a", "b", "c", "d"),
                merged.stream().map(Entry::key).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void highestSequenceWinsRegardlessOfRunOrder() {
        // The newer value (seq 5) is in the SECOND run; it must still win.
        Iterator<Entry> older = Arrays.asList(Entry.put("k", b("old"), 2)).iterator();
        Iterator<Entry> newer = Arrays.asList(Entry.put("k", b("new"), 5)).iterator();

        List<Entry> merged = drain(new MergeIterator(Arrays.asList(older, newer), false));

        assertEquals(1, merged.size());
        assertArrayEquals(b("new"), merged.get(0).value());
        assertEquals(5, merged.get(0).sequence());
    }

    @Test
    void keepsTombstoneWhenNotDropping() {
        Iterator<Entry> older = Arrays.asList(Entry.put("k", b("v"), 1)).iterator();
        Iterator<Entry> newer = Arrays.asList(Entry.tombstone("k", 3)).iterator();

        List<Entry> merged = drain(new MergeIterator(Arrays.asList(older, newer), false));

        assertEquals(1, merged.size());
        assertTrue(merged.get(0).isTombstone());
    }

    @Test
    void dropsWinningTombstoneWhenDropping() {
        Iterator<Entry> older = Arrays.asList(Entry.put("k", b("v"), 1)).iterator();
        Iterator<Entry> newer = Arrays.asList(Entry.tombstone("k", 3)).iterator();

        List<Entry> merged = drain(new MergeIterator(Arrays.asList(older, newer), true));

        assertTrue(merged.isEmpty(), "shadowed value and its tombstone both gone at bottom level");
    }

    @Test
    void liveValueShadowedByOlderTombstoneSurvives() {
        // Newer PUT (seq 4) beats older tombstone (seq 2): the key is live.
        Iterator<Entry> tomb = Arrays.asList(Entry.tombstone("k", 2)).iterator();
        Iterator<Entry> live = Arrays.asList(Entry.put("k", b("v"), 4)).iterator();

        List<Entry> merged = drain(new MergeIterator(Arrays.asList(tomb, live), true));

        assertEquals(1, merged.size());
        assertArrayEquals(b("v"), merged.get(0).value());
    }

    @Test
    void handlesEmptyAndSingleRuns() {
        assertTrue(drain(new MergeIterator(java.util.Collections.emptyList(), false)).isEmpty());
        Iterator<Entry> only = Arrays.asList(Entry.put("x", b("1"), 1)).iterator();
        assertEquals(1, drain(new MergeIterator(Arrays.asList(only), false)).size());
    }
}
