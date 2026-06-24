package com.ledgerkv.transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Causal merge of the concurrent values a replica holds for one key — the operation Riak performs
 * as {@code riak_object:merge} on its vnode, and the reason a replica can accept an out-of-order
 * arrival without losing data.
 *
 * <p>Merging {@code incoming} into {@code existing} keeps exactly the values that nothing else
 * dominates:
 *
 * <ul>
 *   <li>A value whose vector clock <em>happens before</em> another is dropped: it is a strictly
 *       older ancestor and carries no information the survivor lacks.
 *   <li>An identical value (same clock and same bytes) is folded into one, so redelivering a write
 *       or replaying a stale hint is idempotent.
 *   <li>Genuinely concurrent values — neither clock dominating the other — are all retained as
 *       siblings, to be resolved by a reader with the causal context rather than discarded by
 *       arrival order.
 * </ul>
 *
 * <p>This is what makes a delayed hinted handoff safe. Replaying a hint carrying {@code v1} against
 * a replica that has since accepted {@code v2} is a no-op, because {@code v1}'s clock happens
 * before {@code v2}'s. Unconditional storage is what let that hint roll the replica backwards.
 */
public final class Siblings {

    private Siblings() {
    }

    /** Merges one incoming value into the values already held, dropping anything dominated. */
    public static List<StoredValue> merge(List<StoredValue> existing, StoredValue incoming) {
        return merge(existing, java.util.Collections.singletonList(incoming));
    }

    /** Merges two sibling sets, keeping only values that nothing else causally dominates. */
    public static List<StoredValue> merge(List<StoredValue> existing, List<StoredValue> incoming) {
        Objects.requireNonNull(existing, "existing");
        Objects.requireNonNull(incoming, "incoming");
        List<StoredValue> candidates = new ArrayList<>(existing);
        candidates.addAll(incoming);
        return prune(candidates);
    }

    /**
     * Reduces a candidate list to its causal frontier: every value that no other value in the list
     * happens after, with exact duplicates collapsed.
     */
    public static List<StoredValue> prune(List<StoredValue> candidates) {
        List<StoredValue> survivors = new ArrayList<>();
        for (StoredValue candidate : candidates) {
            boolean dominated = false;
            for (StoredValue other : candidates) {
                if (candidate == other) {
                    continue;
                }
                if (candidate.metadata().happensBefore(other.metadata())) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated && !containsEquivalent(survivors, candidate)) {
                survivors.add(candidate);
            }
        }
        return survivors;
    }

    /**
     * True when {@code values} already holds a value with the same causal clock and the same
     * contents. Two values sharing a clock but differing in bytes are kept apart deliberately: that
     * is a genuine conflict, not a duplicate, and collapsing it would silently pick a winner.
     */
    private static boolean containsEquivalent(List<StoredValue> values, StoredValue candidate) {
        for (StoredValue value : values) {
            if (value.metadata().equals(candidate.metadata())
                    && value.tombstone() == candidate.tombstone()
                    && Arrays.equals(value.value(), candidate.value())) {
                return true;
            }
        }
        return false;
    }
}
