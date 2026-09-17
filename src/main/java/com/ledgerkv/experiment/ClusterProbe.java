package com.ledgerkv.experiment;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Raft state off each member's {@code /metrics} endpoint.
 *
 * <p>Experiments need the leader's identity, and guessing it from a node index is how a failover
 * measurement quietly stops measuring failover. Asking the cluster costs one HTTP call per member
 * and is correct across every election.
 *
 * <p>Which member answered is taken from the {@code node} label in the response, never from which
 * URL was dialled. Those disagree in practice: detaching and reattaching a container to the
 * Compose network can leave the host's published ports crossed, so port 8180 starts answering for
 * a different member than it did at startup. A harness that trusted the port would then read the
 * leader's role off one container and kill another, and every failover number it produced would
 * be timing the loss of a follower.
 */
public final class ClusterProbe {

    private static final Pattern NODE_LABEL = Pattern.compile("node=\"[^\"]*-node-(\\d+)\"");

    /** Health-port base URLs; which member each answers for is read from the response. */
    private final List<String> healthUrls;
    private final List<String> services;

    public ClusterProbe(List<String> healthUrls, List<String> services) {
        this.healthUrls = new ArrayList<>(healthUrls);
        this.services = new ArrayList<>(services);
    }

    /** The Compose service currently leading, or empty if no member claims the role. */
    public Optional<String> leaderService() {
        for (String healthUrl : healthUrls) {
            String metrics = fetch(healthUrl + "/metrics");
            if (metrics != null && metrics.contains("role=\"leader\"} 1")) {
                return serviceOf(metrics);
            }
        }
        return Optional.empty();
    }

    /**
     * The Compose service behind a metrics response, read from its {@code node} label. Member ids
     * are {@code <cluster>-node-<ordinal>} and the ordinal indexes the service list.
     */
    private Optional<String> serviceOf(String metrics) {
        Matcher matcher = NODE_LABEL.matcher(metrics);
        if (!matcher.find()) {
            return Optional.empty();
        }
        int ordinal = Integer.parseInt(matcher.group(1));
        return ordinal < services.size() ? Optional.of(services.get(ordinal)) : Optional.empty();
    }

    /** Blocks until some member reports itself leader, or the deadline passes. */
    public Optional<String> awaitLeader(Duration limit) throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<String> leader = leaderService();
            if (leader.isPresent()) {
                return leader;
            }
            Thread.sleep(50);
        }
        return Optional.empty();
    }

    /** Every member's {@code applied} index, keyed by service, skipping unreachable members. */
    public Map<String, Long> appliedIndexes() {
        Map<String, Long> applied = new LinkedHashMap<>();
        for (String healthUrl : healthUrls) {
            String metrics = fetch(healthUrl + "/metrics");
            if (metrics == null) {
                continue;
            }
            Optional<String> service = serviceOf(metrics);
            if (service.isEmpty()) {
                continue;
            }
            for (String line : metrics.split("\n")) {
                if (line.startsWith("ledgerkv_raft_applied_index")) {
                    applied.put(service.get(),
                            Long.parseLong(line.substring(line.lastIndexOf(' ') + 1).trim()));
                }
            }
        }
        return applied;
    }

    /**
     * Blocks until every reachable member has applied at least {@code index}, returning how long
     * that took. Empty if the deadline passed first.
     */
    public Optional<Duration> awaitCaughtUp(long index, int expectedMembers, Duration limit)
            throws InterruptedException {
        Instant start = Instant.now();
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            Map<String, Long> applied = appliedIndexes();
            if (applied.size() >= expectedMembers
                    && applied.values().stream().allMatch(v -> v >= index)) {
                return Optional.of(Duration.between(start, Instant.now()));
            }
            Thread.sleep(50);
        }
        return Optional.empty();
    }

    private static String fetch(String url) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setConnectTimeout(500);
            connection.setReadTimeout(1000);
            try (InputStream in = connection.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException unreachable) {
            return null; // a killed or partitioned member simply does not answer
        }
    }
}
