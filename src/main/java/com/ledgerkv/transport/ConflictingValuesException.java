package com.ledgerkv.transport;

import java.util.Collections;
import java.util.List;

/**
 * Raised when a scalar read finds a key holding concurrent siblings. The key <em>exists</em>; the
 * cluster simply cannot name a winner, because no vector clock dominates the others.
 *
 * <p>This is deliberately not an empty result. Reporting a conflicted key as absent is
 * indistinguishable from a delete to the caller, and hides the one situation where the caller is
 * the only party that can decide. The siblings travel with the
 * exception so a caller can resolve and write back.
 */
public final class ConflictingValuesException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient List<StoredValue> siblings;

    public ConflictingValuesException(String key, List<StoredValue> siblings) {
        super("key " + key + " has " + siblings.size() + " conflicting values");
        this.siblings = Collections.unmodifiableList(siblings);
    }

    /** The concurrent values, for a caller that intends to reconcile them. */
    public List<StoredValue> siblings() {
        return siblings;
    }
}
