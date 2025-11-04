package com.ledgerkv.raft;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LogEntryTest {

    @Test
    void carriesTermIndexAndCommand() {
        LogEntry e = LogEntry.of(3, 7, new byte[] {1, 2, 3});
        assertEquals(3, e.term());
        assertEquals(7, e.index());
        assertArrayEquals(new byte[] {1, 2, 3}, e.command());
    }

    @Test
    void defensivelyCopiesCommandOnReadAndWrite() {
        byte[] in = {9};
        LogEntry e = LogEntry.of(1, 1, in);
        in[0] = 0; // mutate caller's array
        assertArrayEquals(new byte[] {9}, e.command(), "write must copy");
        byte[] out = e.command();
        out[0] = 0; // mutate returned array
        assertArrayEquals(new byte[] {9}, e.command(), "read must copy");
    }

    @Test
    void valueEquality() {
        assertEquals(LogEntry.of(2, 5, new byte[] {7}), LogEntry.of(2, 5, new byte[] {7}));
        assertEquals(LogEntry.of(2, 5, new byte[] {7}).hashCode(),
                LogEntry.of(2, 5, new byte[] {7}).hashCode());
        assertNotEquals(LogEntry.of(2, 5, new byte[] {7}), LogEntry.of(2, 5, new byte[] {8}));
        assertNotEquals(LogEntry.of(2, 5, new byte[] {7}), LogEntry.of(3, 5, new byte[] {7}));
    }

    @Test
    void rejectsNullCommand() {
        assertThrows(NullPointerException.class, () -> LogEntry.of(1, 1, null));
    }

    @Test
    void rejectsNegativeIndex() {
        assertThrows(IllegalArgumentException.class, () -> LogEntry.of(1, -1, new byte[0]));
    }
}
