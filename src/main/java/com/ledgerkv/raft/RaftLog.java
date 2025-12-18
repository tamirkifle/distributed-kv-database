package com.ledgerkv.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The in-memory Raft replicated log. Indices are 1-based; index 0 is the sentinel meaning
 * "before the first entry" (its term is 0), matching the Raft paper's prevLogIndex/prevLogTerm
 * conventions. Durability (WAL backing) is added in sub-plan 3b.
 */
public final class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    public boolean hasEntryAt(long index) {
        return index >= 1 && index <= lastIndex();
    }

    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index < 0 || index > lastIndex()) {
            throw new IndexOutOfBoundsException("no entry at index " + index);
        }
        return entries.get((int) (index - 1)).term();
    }

    public LogEntry entryAt(long index) {
        if (!hasEntryAt(index)) {
            throw new IndexOutOfBoundsException("no entry at index " + index);
        }
        return entries.get((int) (index - 1));
    }

    /** Bulk-replace the log with a dense, 1-based-indexed list (recovery path). */
    public void replace(List<LogEntry> recovered) {
        entries.clear();
        entries.addAll(recovered);
    }

    public void append(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalArgumentException(
                    "append must be dense: expected index " + (lastIndex() + 1) + " got " + entry.index());
        }
        entries.add(entry);
    }

    public List<LogEntry> from(long index) {
        List<LogEntry> suffix = new ArrayList<>();
        for (long i = Math.max(index, 1); i <= lastIndex(); i++) {
            suffix.add(entryAt(i));
        }
        return suffix;
    }

    /** The AppendEntries consistency check: does the local log contain prevLogIndex with prevLogTerm? */
    public boolean matches(long prevLogIndex, long prevLogTerm) {
        if (prevLogIndex == 0) {
            return true;
        }
        return hasEntryAt(prevLogIndex) && termAt(prevLogIndex) == prevLogTerm;
    }

    /**
     * Merge entries from a leader, assuming {@link #matches(long, long)} already passed for prevLogIndex.
     * Truncates any divergent suffix (different term at a shared index) and appends the rest;
     * identical entries are skipped (idempotent re-delivery).
     */
    public void appendAll(long prevLogIndex, List<LogEntry> incoming) {
        Objects.requireNonNull(incoming, "incoming");
        long index = prevLogIndex + 1;
        for (LogEntry entry : incoming) {
            if (hasEntryAt(index)) {
                if (termAt(index) == entry.term()) {
                    index++;
                    continue; // identical prefix, leave untouched
                }
                truncateFrom(index); // divergent suffix: discard from here
            }
            entries.add(entry);
            index++;
        }
    }

    private void truncateFrom(long index) {
        while (lastIndex() >= index) {
            entries.remove(entries.size() - 1);
        }
    }
}
