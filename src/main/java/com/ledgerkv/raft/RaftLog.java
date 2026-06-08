package com.ledgerkv.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The in-memory Raft replicated log. Indices are 1-based and absolute; index 0 is the sentinel
 * meaning "before the first entry" (term 0). After log compaction (Raft paper §7) the log keeps a
 * <b>snapshot base</b> {@code (lastIncludedIndex, lastIncludedTerm)}: physical entries start after
 * the base, but external indices stay absolute, so the base generalizes the index-0 sentinel.
 * Durability (WAL backing) lives in {@link RaftPersistence}; snapshotting is sub-plan 5c.
 */
public final class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long lastIncludedIndex = 0;
    private long lastIncludedTerm = 0;

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long lastIncludedTerm() {
        return lastIncludedTerm;
    }

    /** Number of physical (uncompacted) entries currently held. */
    public int size() {
        return entries.size();
    }

    public long lastIndex() {
        return lastIncludedIndex + entries.size();
    }

    public long lastTerm() {
        if (entries.isEmpty()) {
            return lastIncludedTerm;
        }
        return entries.get(entries.size() - 1).term();
    }

    public boolean hasEntryAt(long index) {
        return index > lastIncludedIndex && index <= lastIndex();
    }

    public long termAt(long index) {
        if (index == lastIncludedIndex) {
            return lastIncludedTerm; // generalized sentinel (index 0 when never compacted)
        }
        if (!hasEntryAt(index)) {
            throw new IndexOutOfBoundsException("no entry at index " + index
                    + " (base=" + lastIncludedIndex + ", last=" + lastIndex() + ")");
        }
        return entries.get(slot(index)).term();
    }

    public LogEntry entryAt(long index) {
        if (!hasEntryAt(index)) {
            throw new IndexOutOfBoundsException("no entry at index " + index
                    + " (base=" + lastIncludedIndex + ", last=" + lastIndex() + ")");
        }
        return entries.get(slot(index));
    }

    /** Bulk-replace the physical log with a dense list whose first entry sits just after the base. */
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
        for (long i = Math.max(index, lastIncludedIndex + 1); i <= lastIndex(); i++) {
            suffix.add(entryAt(i));
        }
        return suffix;
    }

    /** The AppendEntries consistency check: does the local log contain prevLogIndex with prevLogTerm? */
    public boolean matches(long prevLogIndex, long prevLogTerm) {
        if (prevLogIndex == lastIncludedIndex) {
            return prevLogTerm == lastIncludedTerm; // base (or index-0 sentinel) match
        }
        return hasEntryAt(prevLogIndex) && termAt(prevLogIndex) == prevLogTerm;
    }

    /**
     * Merge entries from a leader, assuming {@link #matches(long, long)} already passed for prevLogIndex.
     * Truncates any divergent suffix (different term at a shared index) and appends the rest;
     * identical entries are skipped (idempotent re-delivery). Entries at or below the base are
     * already covered by the snapshot and are skipped.
     */
    public void appendAll(long prevLogIndex, List<LogEntry> incoming) {
        Objects.requireNonNull(incoming, "incoming");
        long index = prevLogIndex + 1;
        for (LogEntry entry : incoming) {
            if (index <= lastIncludedIndex) {
                index++;
                continue; // covered by the snapshot already
            }
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

    /**
     * Discard the log prefix through {@code index} (inclusive), recording it as the new snapshot
     * base with term {@code term}. No-op if {@code index <= lastIncludedIndex}.
     */
    public void compactThrough(long index, long term) {
        if (index <= lastIncludedIndex) {
            return;
        }
        if (index > lastIndex()) {
            throw new IllegalArgumentException(
                    "cannot compact through " + index + " beyond lastIndex " + lastIndex());
        }
        int drop = (int) (index - lastIncludedIndex);
        entries.subList(0, drop).clear();
        lastIncludedIndex = index;
        lastIncludedTerm = term;
    }

    /** Discard ALL physical entries and set the base (a follower installing a fresh snapshot). */
    public void resetToSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        entries.clear();
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
    }

    private int slot(long index) {
        return (int) (index - lastIncludedIndex - 1);
    }

    private void truncateFrom(long index) {
        while (lastIndex() >= index) {
            entries.remove(entries.size() - 1);
        }
    }
}
