package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import java.util.Optional;

/**
 * The transport seam the quorum coordinator fans out to. A {@code ReplicaClient} addresses a single
 * replica's storage in the cluster's currency — a {@link VersionedValue} (value + version +
 * vector-clock metadata). The same coordinator logic runs over any implementation: a same-JVM
 * {@link LocalReplicaClient} backed by an {@code LsmEngine}, or a {@link GrpcReplicaClient} talking
 * to a remote {@code NodeServer}.
 *
 * <p>Writes store the supplied value <em>verbatim</em> (the coordinator owns versioning). A replica
 * signals unavailability by throwing a {@link RuntimeException}; the coordinator treats that as a
 * failed acknowledgment (it does not count toward quorum).
 */
public interface ReplicaClient {

    /** The cluster node id this client targets. */
    String nodeId();

    /** Reads the stored versioned value for {@code key}, or empty if absent. */
    Optional<VersionedValue> get(String key);

    /** Stores {@code value} under {@code key} verbatim. */
    void put(String key, VersionedValue value);

    /** Delivers a hinted-handoff write under {@code key}, stored verbatim. */
    void deliverHint(String key, VersionedValue value);
}
