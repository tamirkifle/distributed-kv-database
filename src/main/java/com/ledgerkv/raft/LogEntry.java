package com.ledgerkv.raft;

import java.util.Arrays;
import java.util.Objects;

/** An immutable Raft replicated-log record: a term, a 1-based log index, and an opaque command. */
public final class LogEntry {

    private final long term;
    private final long index;
    private final byte[] command;

    private LogEntry(long term, long index, byte[] command) {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0");
        }
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        this.term = term;
        this.index = index;
        this.command = Objects.requireNonNull(command, "command").clone();
    }

    public static LogEntry of(long term, long index, byte[] command) {
        return new LogEntry(term, index, command);
    }

    public long term() {
        return term;
    }

    public long index() {
        return index;
    }

    public byte[] command() {
        return command.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogEntry)) {
            return false;
        }
        LogEntry other = (LogEntry) o;
        return term == other.term && index == other.index && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        return (Objects.hash(term, index) * 31) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", index=" + index + ", commandBytes=" + command.length + '}';
    }
}
