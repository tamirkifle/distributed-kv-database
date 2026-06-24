package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import java.util.List;

/**
 * The transport seam the quorum coordinator fans out to. A {@code ReplicaClient} addresses a single
 * replica's storage in the cluster's currency — a {@link VersionedValue} (value + version +
 * vector-clock metadata). The same coordinator logic runs over any implementation: a same-JVM
 * {@link LocalReplicaClient} backed by an {@code LsmEngine}, or a {@link GrpcReplicaClient} talking
 * to a remote {@code NodeServer}.
 *
 * <p>A write is merged into the replica's existing values by causal clock rather than overwriting
 * them, so a delayed or reordered arrival cannot roll a replica backwards and two concurrent writes
 * are both retained as siblings. A read therefore returns a <em>list</em>: empty when the key is
 * absent, one value in the ordinary case, and more than one when that replica holds an unresolved
 * conflict.
 *
 * <p>A replica signals unavailability by throwing a {@link RuntimeException}; the coordinator
 * treats that as a failed acknowledgment (it does not count toward quorum).
 */
public interface ReplicaClient {

    /** The cluster node id this client targets. */
    String nodeId();

    /** The values held for {@code key}: empty if absent, more than one if conflicted. */
    List<VersionedValue> get(String key);

    /** Merges {@code value} into whatever this replica holds for {@code key}. */
    void put(String key, VersionedValue value);

    /**
     * Delivers a hinted-handoff write. Merged exactly like {@link #put}, which is what makes a
     * late-replayed hint safe: if the replica has since accepted a causally newer value, the hint's
     * older clock loses and the delivery is a no-op.
     */
    void deliverHint(String key, VersionedValue value);
}
