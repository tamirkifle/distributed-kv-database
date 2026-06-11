package com.ledgerkv.storage.lsm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableHandleRefCountTest {

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

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
        h.unpin();                              // refCount = 0 -> closed
        assertThrows(UncheckedIOException.class, () -> h.table().get("a"),
                "channel must be closed after the last unpin");
    }

    @Test
    void closeWhilePinnedDoesNotRipChannelFromReader(@TempDir Path dir) throws Exception {
        SSTableHandle h = write(dir);          // refCount = 1 (engine ref)
        h.pin();                                // refCount = 2 (one in-flight reader)
        h.close();                              // releases the ENGINE ref: refCount = 1, NOT closed
        assertTrue(h.table().get("a").isPresent(),
                "close() must not rip the channel out from under a pinned reader");
        h.unpin();                              // reader done: refCount = 0 -> closed
        assertThrows(UncheckedIOException.class, () -> h.table().get("a"),
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
