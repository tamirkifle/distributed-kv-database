package com.ledgerkv.transport;

import java.util.Optional;

/**
 * The abstraction a {@link NodeServer} routes its <em>public</em> client {@code Get}/{@code Put}
 * through when it acts as a quorum coordinator. Implementations fan a request out across the key's
 * replica set and reconcile the result.
 *
 * <p>Lives in {@code transport} so {@code NodeServer} need not depend on the {@code cluster} package;
 * the concrete quorum adapter ({@code com.ledgerkv.node.QuorumClientCoordinator}) bridges the two.
 */
public interface ClientCoordinator {

    /** Coordinates a quorum read. Empty if no live value; throws if the read quorum is not met. */
    Optional<StoredValue> get(String key);

    /** Coordinates a quorum write and returns the stored value; throws if the write quorum is not met. */
    StoredValue put(String key, byte[] value);
}
