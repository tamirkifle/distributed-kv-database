package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableHandleRefCountTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    // The channel lifecycle (open until refCount hits 0) is SSTableHandle's invariant, asserted
    // directly via SSTable.isChannelOpen(). A get() is no longer a reliable channel-liveness probe
    // because the per-table block cache can serve an already-decoded block without touching disk.
    private static SSTableHandle write(Path dir) throws Exception {
        Path path = dir.resolve("t.db");
        try (SSTableWriter w = new SSTableWriter(path, 2)) {
            w.add(Entry.put("a", b("1"), 1));
            w.add(Entry.put("b", b("2"), 2));
            w.finish();
        }
        return SSTableHandle.open(path, 0);
    }

    @Test
    void pinKeepsChannelOpenUntilLastUnpin(@TempDir Path dir) throws Exception {
        SSTableHandle h = write(dir);          // refCount = 1 (engine ref)
        h.pin();                                // refCount = 2 (one reader)
        h.unpin();                              // refCount = 1 -> still open
        assertTrue(h.table().get("a").isPresent(), "channel must still be readable while pinned");
        assertTrue(h.table().isChannelOpen(), "channel must stay open while a reference remains");
        h.unpin();                              // refCount = 0 -> closed
        assertFalse(h.table().isChannelOpen(), "channel must be closed after the last unpin");
    }

    @Test
    void closeWhilePinnedDoesNotRipChannelFromReader(@TempDir Path dir) throws Exception {
        SSTableHandle h = write(dir);          // refCount = 1 (engine ref)
        h.pin();                                // refCount = 2 (one in-flight reader)
        h.close();                              // releases the ENGINE ref: refCount = 1, NOT closed
        assertTrue(h.table().get("a").isPresent(),
                "close() must not rip the channel out from under a pinned reader");
        assertTrue(h.table().isChannelOpen(), "channel must stay open for the pinned reader");
        h.unpin();                              // reader done: refCount = 0 -> closed
        assertFalse(h.table().isChannelOpen(),
                "channel closes only when the last reference is released");
    }

    @Test
    void unpinToZeroClosesExactlyOnce(@TempDir Path dir) throws Exception {
        SSTableHandle h = write(dir);          // refCount = 1
        assertEquals("a", h.firstKey());        // metadata still works
        h.unpin();                              // refCount = 0 -> closed
        // A redundant hard close must not blow up (FileChannel.close is idempotent).
        h.close();
    }
}
