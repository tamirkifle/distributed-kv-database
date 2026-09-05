package com.ledgerkv.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * A marker file in the data directory recording which cluster, node, and mode last owned it.
 *
 * <p>The failure this prevents is quiet and expensive: point a Raft node at a volume holding a
 * quorum node's LSM data (or reuse one node's volume for another ordinal) and nothing complains.
 * The node starts, finds no state it recognizes, and serves an empty keyspace as if that were the
 * truth. Checking identity at startup turns that into a refusal to boot.
 */
public final class ClusterIdentity {

    private static final String FILE = "cluster-identity.properties";

    private final String clusterId;
    private final String nodeId;
    private final NodeMode mode;

    public ClusterIdentity(String clusterId, String nodeId, NodeMode mode) {
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.mode = mode;
    }

    public String clusterId() {
        return clusterId;
    }

    public String nodeId() {
        return nodeId;
    }

    public NodeMode mode() {
        return mode;
    }

    /**
     * Claims {@code dir} for this identity. Adopts a directory that carries no marker yet, which is
     * both a fresh volume and an existing quorum deployment upgrading into this check. Throws when
     * the directory already belongs to a different cluster, node, or mode.
     */
    public static ClusterIdentity claim(Path dir, ClusterIdentity wanted) throws IOException {
        Files.createDirectories(dir);
        Path marker = dir.resolve(FILE);
        if (Files.exists(marker)) {
            ClusterIdentity found = read(marker);
            if (!found.matches(wanted)) {
                throw new IllegalStateException("data directory " + dir + " holds " + found
                        + " but this node started as " + wanted
                        + "; point it at its own volume or clear the directory");
            }
            return found;
        }
        write(marker, wanted);
        return wanted;
    }

    private boolean matches(ClusterIdentity other) {
        return clusterId.equals(other.clusterId)
                && nodeId.equals(other.nodeId)
                && mode == other.mode;
    }

    private static ClusterIdentity read(Path marker) throws IOException {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(marker)) {
            props.load(in);
        }
        String mode = props.getProperty("mode", "");
        return new ClusterIdentity(
                props.getProperty("clusterId", ""),
                props.getProperty("nodeId", ""),
                NodeMode.parse(mode));
    }

    private static void write(Path marker, ClusterIdentity identity) throws IOException {
        Properties props = new Properties();
        props.setProperty("clusterId", identity.clusterId);
        props.setProperty("nodeId", identity.nodeId);
        props.setProperty("mode", identity.mode.label());
        // Write beside the target and move into place, so a crash mid-write cannot leave a
        // half-written marker that fails to parse on the next boot.
        Path temp = marker.resolveSibling(FILE + ".tmp");
        try (OutputStream out = Files.newOutputStream(temp)) {
            props.store(out, "LedgerKV data directory owner. Delete only to reuse this volume.");
        }
        Files.move(temp, marker, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public String toString() {
        return "cluster=" + clusterId + " node=" + nodeId + " mode=" + mode.label();
    }
}
