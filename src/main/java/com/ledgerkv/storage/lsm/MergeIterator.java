package com.ledgerkv.storage.lsm;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A k-way merge over sorted runs of {@link Entry} (each run ascending by key, keys unique within
 * a run). For any key, the entry with the highest {@code sequence} across all runs wins
 * (newest-wins); older duplicates are discarded. When {@code dropTombstones} is true, a winning
 * tombstone is omitted from the output — set only when the merge output lands at the bottom of the
 * tree, where no lower table can hold a value the tombstone still needs to shadow.
 *
 * <p>Because the winner is chosen by sequence, the order of {@code runs} does not matter.
 */
public final class MergeIterator implements Iterator<Entry> {

    /** A run plus its current head entry (null once exhausted). */
    private static final class Cursor {
        private final Iterator<Entry> it;
        private final int runIndex;
        private Entry head;

        Cursor(Iterator<Entry> it, int runIndex) {
            this.it = it;
            this.runIndex = runIndex;
            this.head = it.hasNext() ? it.next() : null;
        }

        boolean advance() {
            head = it.hasNext() ? it.next() : null;
            return head != null;
        }
    }

    // Order by key ascending; for equal keys, highest sequence first so the winner pops first;
    // runIndex is a final tie-break for determinism (equal key+sequence should not occur in practice).
    private static final Comparator<Cursor> ORDER = (a, b) -> {
        int c = a.head.key().compareTo(b.head.key());
        if (c != 0) {
            return c;
        }
        c = Long.compare(b.head.sequence(), a.head.sequence());
        if (c != 0) {
            return c;
        }
        return Integer.compare(a.runIndex, b.runIndex);
    };

    private final PriorityQueue<Cursor> heap = new PriorityQueue<>(ORDER);
    private final boolean dropTombstones;
    private Entry next;

    public MergeIterator(List<Iterator<Entry>> runs, boolean dropTombstones) {
        this.dropTombstones = dropTombstones;
        int i = 0;
        for (Iterator<Entry> run : runs) {
            Cursor cursor = new Cursor(run, i++);
            if (cursor.head != null) {
                heap.add(cursor);
            }
        }
        advance();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Entry next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        Entry result = next;
        advance();
        return result;
    }

    private void advance() {
        next = null;
        while (!heap.isEmpty()) {
            Cursor top = heap.poll();
            Entry winner = top.head; // highest sequence for this key
            if (top.advance()) {
                heap.add(top);
            }
            // Discard every older version of the same key.
            while (!heap.isEmpty() && heap.peek().head.key().equals(winner.key())) {
                Cursor dup = heap.poll();
                if (dup.advance()) {
                    heap.add(dup);
                }
            }
            if (dropTombstones && winner.isTombstone()) {
                continue; // skip; advance to the next distinct key
            }
            next = winner;
            return;
        }
    }
}
