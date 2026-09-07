package com.ledgerkv.transport;

import java.util.Objects;

/**
 * The at-most-once identity a client attaches to one mutation: a stable client id plus a sequence
 * number that client assigns. Both must survive retries and leader changes unchanged, because they
 * are the only thing that lets a replicated state machine tell a retry from a second write.
 *
 * <p>Quorum mode ignores it and reconciles by vector clock instead, so a coordinator may receive
 * {@link #absent()}.
 */
public final class MutationId {

    private static final MutationId ABSENT = new MutationId("", 0);

    private final String clientId;
    private final long sequence;

    private MutationId(String clientId, long sequence) {
        this.clientId = clientId;
        this.sequence = sequence;
    }

    public static MutationId of(String clientId, long sequence) {
        Objects.requireNonNull(clientId, "clientId");
        if (clientId.isEmpty() || sequence < 1) {
            throw new IllegalArgumentException(
                    "client id must be non-empty and sequence >= 1, got '" + clientId + "'/" + sequence);
        }
        return new MutationId(clientId, sequence);
    }

    /** A request that carried no identity. Usable in quorum mode; refused by the Raft path. */
    public static MutationId absent() {
        return ABSENT;
    }

    public boolean isPresent() {
        return !clientId.isEmpty();
    }

    public String clientId() {
        return clientId;
    }

    public long sequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return isPresent() ? clientId + "#" + sequence : "<absent>";
    }
}
