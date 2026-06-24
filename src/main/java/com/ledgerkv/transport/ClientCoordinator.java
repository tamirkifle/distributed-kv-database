package com.ledgerkv.transport;

import java.util.List;

/**
 * The abstraction a {@link NodeServer} routes its <em>public</em> client {@code Get}/{@code Put}
 * through when it acts as a quorum coordinator. Implementations fan a request out across the key's
 * replica set and reconcile the result.
 *
 * <p>Lives in {@code transport} so {@code NodeServer} need not depend on the {@code cluster} package;
 * the concrete quorum adapter ({@code com.ledgerkv.node.QuorumClientCoordinator}) bridges the two.
 */
public interface ClientCoordinator {

    /**
     * Coordinates a quorum read. Returns every value the read observed: empty when the key is
     * absent, one value in the ordinary case, and more than one when the replicas hold an
     * unresolved conflict. Throws if the read quorum is not met.
     *
     * <p>Returning a list is what lets the public API distinguish "no such key" from "this key has
     * two concurrent values". Collapsing the conflicted case to an empty Optional reported a live
     * key as missing.
     */
    List<StoredValue> get(String key);

    /** Coordinates a quorum write and returns the stored value; throws if the write quorum is not met. */
    StoredValue put(String key, byte[] value);
}
