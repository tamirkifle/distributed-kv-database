package com.ledgerkv.node;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Which replication path a deployment runs. One mode per deployment: the two have different
 * durability, different storage, and different failure behaviour, so a node serves one or the
 * other, never both at once.
 */
public enum NodeMode {

    /** Dynamo-style leaderless quorums over the LSM engine. The default, and what ships today. */
    QUORUM,

    /** A fixed Raft group replicating the whole keyspace through its own log and snapshots. */
    RAFT;

    /** Parses {@code LEDGERKV_MODE}, case-insensitively. Throws on anything unrecognized. */
    public static NodeMode parse(String raw) {
        for (NodeMode mode : values()) {
            if (mode.name().equalsIgnoreCase(raw.trim())) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown LEDGERKV_MODE '" + raw + "', expected one of "
                + Arrays.stream(values())
                        .map(m -> m.name().toLowerCase(Locale.ROOT))
                        .collect(Collectors.joining("|")));
    }

    /** The lowercase spelling used in env vars, the identity file, and metrics labels. */
    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
