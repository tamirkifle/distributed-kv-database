package com.ledgerkv.experiment;

import com.ledgerkv.transport.LedgerKvClusterClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * exp-02: kills the Raft leader repeatedly and measures how long writes stop working, and whether
 * any acknowledged write is lost.
 *
 * <p>Two things make the number honest. The outage is measured from the instant the SIGKILL was
 * issued — taken by {@link ClusterFaults} outside the cluster — to the first write that
 * subsequently succeeds, so it includes detection, election, and the client's own rediscovery of
 * the leader. And every acknowledged key is kept in an external ledger, checked after each cycle,
 * because "no writes lost" is only a claim if something outside the cluster remembers what was
 * promised.
 *
 * <p>A write whose outcome was never learned is <em>not</em> in the ledger. It may have committed,
 * so its absence afterwards is not loss; counting it either way would be a guess.
 *
 * <pre>
 * java -cp target/ledgerkv.jar com.ledgerkv.experiment.FailoverExperiment --cycles 30
 * </pre>
 */
public final class FailoverExperiment {

    private static final Duration CLIENT_DEADLINE = Duration.ofSeconds(5);
    private static final Duration RECOVERY_LIMIT = Duration.ofSeconds(60);

    private FailoverExperiment() {
    }

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.parse(args);
        ClusterProbe probe = new ClusterProbe(settings.healthUrls, settings.services);
        ClusterFaults faults = new ClusterFaults(settings.composeFile, settings.services);

        List<Long> outagesMillis = new ArrayList<>();
        List<Long> catchUpMillis = new ArrayList<>();
        Set<String> acknowledged = new LinkedHashSet<>();
        Set<String> unknown = new LinkedHashSet<>();
        int lost = 0;

        System.out.println("exp-02 cycles=" + settings.cycles);
        try (LedgerKvClusterClient client = LedgerKvClusterClient.connect(
                settings.endpoints, "exp02-writer", CLIENT_DEADLINE)) {

            for (int cycle = 0; cycle < settings.cycles; cycle++) {
                if (probe.awaitLeader(Duration.ofSeconds(30)).isEmpty()) {
                    throw new IllegalStateException("no leader before cycle " + cycle);
                }
                // A write before the kill, so each cycle has something that must survive it.
                write(client, "pre-" + cycle, acknowledged, unknown);

                String leader = probe.leaderService()
                        .orElseThrow(() -> new IllegalStateException("leader vanished"));
                Instant killedAt = faults.kill(leader);

                // Time to the first write that succeeds after the fault. Retries are the client's
                // own; this measures what an application would actually experience.
                Instant recovered = firstSuccessfulWrite(client, "post-" + cycle, acknowledged,
                        unknown, killedAt);
                long outage = Duration.between(killedAt, recovered).toMillis();
                outagesMillis.add(outage);

                faults.start(leader);
                Optional<Duration> caughtUp = probe.awaitCaughtUp(
                        1, settings.services.size(), RECOVERY_LIMIT);
                caughtUp.ifPresent(d -> catchUpMillis.add(d.toMillis()));

                int missing = countMissing(client, acknowledged);
                lost += missing;
                System.out.printf("  cycle %-3d killed=%-6s outage=%5d ms catchUp=%s lost=%d%n",
                        cycle, leader, outage,
                        caughtUp.map(d -> d.toMillis() + " ms").orElse("timeout"), missing);
            }

            System.out.println();
            System.out.println("acknowledged=" + acknowledged.size()
                    + " unknownOutcome=" + unknown.size() + " lost=" + lost);
            report("write outage", outagesMillis);
            report("follower catch-up", catchUpMillis);
            if (!unknown.isEmpty()) {
                System.out.println("note: " + unknown.size() + " writes had an unknown outcome and"
                        + " are excluded from the loss count; their absence is not loss.");
            }
        } finally {
            faults.healAll();
        }
    }

    /** Writes, recording the key as acknowledged only if the cluster actually said so. */
    private static void write(LedgerKvClusterClient client, String key,
            Set<String> acknowledged, Set<String> unknown) {
        try {
            client.put(key, key.getBytes(StandardCharsets.UTF_8));
            acknowledged.add(key);
        } catch (RuntimeException e) {
            if (HistoryRecorder.classify(e) == com.ledgerkv.checker.OperationResult.UNKNOWN) {
                unknown.add(key);
            }
        }
    }

    /** Retries until a write succeeds, so the measured outage ends at real restored service. */
    private static Instant firstSuccessfulWrite(LedgerKvClusterClient client, String key,
            Set<String> acknowledged, Set<String> unknown, Instant killedAt)
            throws InterruptedException {
        long deadline = System.nanoTime() + RECOVERY_LIMIT.toNanos();
        int attempt = 0;
        while (System.nanoTime() < deadline) {
            String attemptKey = key + "-" + attempt++;
            write(client, attemptKey, acknowledged, unknown);
            if (acknowledged.contains(attemptKey)) {
                return Instant.now();
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException(
                "no write succeeded within " + RECOVERY_LIMIT + " of the kill at " + killedAt);
    }

    /** How many acknowledged keys the cluster can no longer produce. */
    private static int countMissing(LedgerKvClusterClient client, Set<String> acknowledged) {
        int missing = 0;
        for (String key : acknowledged) {
            try {
                if (client.get(key).isEmpty()) {
                    missing++;
                }
            } catch (RuntimeException unreadable) {
                // Could not check this key right now; do not score it either way.
            }
        }
        return missing;
    }

    private static void report(String label, List<Long> samples) {
        if (samples.isEmpty()) {
            System.out.println(label + ": no samples");
            return;
        }
        List<Long> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        System.out.printf("%s: n=%d min=%d median=%d p95=%d max=%d (ms)%n",
                label, sorted.size(), sorted.get(0), percentile(sorted, 50),
                percentile(sorted, 95), sorted.get(sorted.size() - 1));
        System.out.println("  raw: " + samples);
    }

    /** Nearest-rank percentile. Reported alongside the raw list, since n here is small. */
    private static long percentile(List<Long> sorted, int p) {
        int rank = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, rank - 1)));
    }

    static final class Settings {
        final List<String> services =
                Arrays.asList("node0", "node1", "node2", "node3", "node4");
        final List<String> endpoints = Arrays.asList("localhost:9190", "localhost:9191",
                "localhost:9192", "localhost:9193", "localhost:9194");
        final List<String> healthUrls = Arrays.asList("http://localhost:8180",
                "http://localhost:8181", "http://localhost:8182", "http://localhost:8183",
                "http://localhost:8184");
        String composeFile = "docker-compose.raft.yml";
        int cycles = 30;

        static Settings parse(String[] args) {
            Settings settings = new Settings();
            for (int i = 0; i + 1 < args.length; i += 2) {
                switch (args[i]) {
                    case "--cycles":
                        settings.cycles = Integer.parseInt(args[i + 1]);
                        break;
                    case "--compose":
                        settings.composeFile = args[i + 1];
                        break;
                    default:
                        throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            return settings;
        }
    }
}
