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

    /**
     * Coordinates a write and returns the stored value; throws if it could not be made durable.
     * {@code id} carries the caller's at-most-once identity: the Raft path requires it and refuses
     * a mutation without one, the quorum path ignores it.
     */
    StoredValue put(String key, byte[] value, MutationId id);

    /**
     * Coordinates a delete and reports whether a live value existed beforehand. Throws if it could
     * not be made durable. {@code id} is used as in {@link #put}.
     */
    boolean delete(String key, MutationId id);

    /**
     * Whether this coordinator can serve a range scan. The Raft path cannot: its state machine is
     * an unordered map with no range iterator, and a scan served off one member's applied state
     * would not be linearizable with the writes around it. {@code NodeServer} turns a false here
     * into {@code UNIMPLEMENTED} rather than an empty stream, which would read as "no such range".
     */
    default boolean supportsScan() {
        return true;
    }
}
