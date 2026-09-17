package com.ledgerkv.experiment;

import com.ledgerkv.checker.LinearizabilityCheckException;
import com.ledgerkv.checker.LinearizabilityChecker;
import com.ledgerkv.checker.LinearizabilityResult;
import com.ledgerkv.transport.LedgerKvClusterClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * exp-01: runs seeded, bounded concurrent histories against a deployed Raft cluster, under a
 * scenario's faults, and checks each one for linearizability.
 *
 * <p>Each history is independent and stays under the checker's per-key operation bound, so a
 * violation is a counterexample the checker can actually search rather than a budget overrun.
 * Three verdicts are possible and all three are reported: linearizable, violating, and
 * inconclusive — the last meaning the search exceeded its budget, which is not evidence of
 * correctness and must not be counted as such.
 *
 * <p>Run it against a cluster that is already up:
 * <pre>
 * java -cp target/ledgerkv.jar com.ledgerkv.experiment.CorrectnessExperiment \
 *     --histories 100 --seed 1 --scenario healthy
 * </pre>
 */
public final class CorrectnessExperiment {

    /** Operations per history. Well under the checker's 64-per-key default. */
    private static final int OPS_PER_HISTORY = 24;
    private static final int CLIENTS = 4;
    private static final Duration CLIENT_DEADLINE = Duration.ofSeconds(3);

    /**
     * Think time between a client's operations. Without it a history of 24 tiny requests finishes
     * in a few milliseconds, the fault lands after every client has already gone home, and the
     * scenario silently measures a healthy cluster.
     */
    private static final int MIN_THINK_MS = 20;
    private static final int MAX_THINK_MS = 60;

    /**
     * Inject once this fraction of the history has been recorded, rather than after a fixed delay.
     * Progress is the only trigger that keeps the fault inside the history on a machine whose
     * speed the harness does not know.
     */
    private static final double FAULT_AT_FRACTION = 0.3;

    /** What is done to the cluster while a history is being recorded. */
    public enum Scenario { HEALTHY, LEADER_LOSS, PARTITION_3_2, RESTART }

    /**
     * Distinguishes this process's keys from every earlier run's.
     *
     * <p>Keys must be fresh, not merely unique within a run. A cluster is not wiped between
     * scenarios, so a key named only after its seed is re-read in the next scenario with the
     * previous scenario's value still in it. Every history then opens with reads of a value no
     * write in that history explains, and the checker correctly calls all of them violations —
     * which looks exactly like a database that loses its mind under fault injection.
     */
    private static final String RUN_ID =
            Long.toUnsignedString(System.currentTimeMillis(), 36);

    private CorrectnessExperiment() {
    }

    /** One history's verdict. */
    public static final class Outcome {
        public final long seed;
        public final boolean linearizable;
        public final boolean inconclusive;
        public final String detail;
        public final HistoryRecorder.Counts counts;
        public final long slowestMillis;
        public final List<String> faultEvents;

        Outcome(long seed, boolean linearizable, boolean inconclusive, String detail,
                HistoryRecorder.Counts counts, long slowestMillis, List<String> faultEvents) {
            this.seed = seed;
            this.linearizable = linearizable;
            this.inconclusive = inconclusive;
            this.detail = detail;
            this.counts = counts;
            this.slowestMillis = slowestMillis;
            this.faultEvents = faultEvents;
        }
    }

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.parse(args);
        ClusterProbe probe = new ClusterProbe(settings.healthUrls, settings.services);
        ClusterFaults faults = new ClusterFaults(settings.composeFile, settings.services);

        int linearizable = 0;
        int violating = 0;
        int inconclusive = 0;
        int operations = 0;
        List<Outcome> counterexamples = new ArrayList<>();

        System.out.println("exp-01 scenario=" + settings.scenario + " histories="
                + settings.histories + " baseSeed=" + settings.seed + " runId=" + RUN_ID);
        // Warm the network lookup while every container is still attached; see ClusterFaults.
        faults.resolveNetwork();
        try {
            for (int i = 0; i < settings.histories; i++) {
                long seed = settings.seed + i;
                Outcome outcome = runOne(seed, settings, probe, faults);
                operations += outcome.counts.total();
                if (outcome.inconclusive) {
                    inconclusive++;
                } else if (outcome.linearizable) {
                    linearizable++;
                } else {
                    violating++;
                    counterexamples.add(outcome);
                    System.out.println("    counterexample: " + outcome.detail);
                }
                System.out.printf("  seed=%-6d %-14s %s slowestOp=%dms faults=%s%n", seed,
                        outcome.inconclusive ? "INCONCLUSIVE"
                                : outcome.linearizable ? "linearizable" : "VIOLATION",
                        outcome.counts, outcome.slowestMillis, outcome.faultEvents);
            }
        } finally {
            faults.healAll();
        }

        System.out.println();
        System.out.println("linearizable=" + linearizable + " violating=" + violating
                + " inconclusive=" + inconclusive + " operationsChecked=" + operations);
        for (Outcome counterexample : counterexamples) {
            System.out.println("counterexample seed=" + counterexample.seed + ": "
                    + counterexample.detail);
        }
        if (inconclusive > 0) {
            System.out.println("note: inconclusive histories exceeded the checker's search budget."
                    + " They are not evidence of correctness.");
        }
        System.exit(violating == 0 ? 0 : 1);
    }

    private static Outcome runOne(long seed, Settings settings, ClusterProbe probe,
            ClusterFaults faults) throws Exception {
        Optional<String> leader = probe.awaitLeader(Duration.ofSeconds(60));
        if (leader.isEmpty()) {
            throw new IllegalStateException("no leader within 60s before history seed=" + seed
                    + "; the cluster did not re-form after the previous history's faults, so"
                    + " every later verdict would be measuring a broken deployment");
        }

        faults.clearEvents(); // each history reports only the faults injected during it
        // One key per history keeps every recorded operation inside one register's search space.
        String key = "exp01-" + RUN_ID + "-" + settings.scenario.name().toLowerCase(
                java.util.Locale.ROOT) + "-" + seed;
        HistoryRecorder recorder = new HistoryRecorder();
        Random random = new Random(seed);

        ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
        List<LedgerKvClusterClient> handles = new ArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int c = 0; c < CLIENTS; c++) {
                // A distinct client id per history, so a retry from a previous history can never
                // be deduplicated against this one's sequence numbers.
                LedgerKvClusterClient client = LedgerKvClusterClient.connect(
                        settings.endpoints, "exp01-" + seed + "-" + c, CLIENT_DEADLINE);
                handles.add(client);
                final long clientSeed = seed * 31 + c;
                clients.submit(() -> {
                    Random rng = new Random(clientSeed);
                    await(start);
                    for (int op = 0; op < OPS_PER_HISTORY / CLIENTS; op++) {
                        if (rng.nextInt(100) < 40) {
                            String value = "v" + rng.nextInt(1000);
                            recorder.recordWrite(key, value,
                                    () -> client.put(key, value.getBytes(StandardCharsets.UTF_8)));
                        } else {
                            recorder.recordRead(key, readOf(client, key));
                        }
                        Thread.sleep(MIN_THINK_MS + rng.nextInt(MAX_THINK_MS - MIN_THINK_MS));
                    }
                    return null;
                });
            }

            Thread fault = faultThread(
                    settings.scenario, faults, recorder, leader.get(), random, settings.services);
            start.countDown();
            if (fault != null) {
                fault.start();
                fault.join();
            }
            clients.shutdown();
            if (!clients.awaitTermination(2, TimeUnit.MINUTES)) {
                throw new IllegalStateException("history seed=" + seed + " did not finish");
            }
        } finally {
            clients.shutdownNow();
            for (LedgerKvClusterClient client : handles) {
                client.close();
            }
            faults.healAll();
        }

        List<String> injected = new ArrayList<>();
        for (ClusterFaults.Event event : faults.events()) {
            injected.add(event.what);
        }
        try {
            LinearizabilityResult result = new LinearizabilityChecker().check(recorder.history());
            return new Outcome(seed, result.isLinearizable(), false, result.describeWitness(),
                    recorder.counts(), recorder.slowestMillis(), injected);
        } catch (LinearizabilityCheckException overBudget) {
            return new Outcome(seed, false, true, overBudget.getMessage(),
                    recorder.counts(), recorder.slowestMillis(), injected);
        }
    }

    /** Reads the key, mapping "absent" to null so the checker sees an unwritten register. */
    private static Callable<String> readOf(LedgerKvClusterClient client, String key) {
        return () -> client.get(key)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .orElse(null);
    }

    private static Thread faultThread(Scenario scenario, ClusterFaults faults,
            HistoryRecorder recorder, String leader, Random random, List<String> services) {
        if (scenario == Scenario.HEALTHY) {
            return null;
        }
        return new Thread(() -> {
            try {
                awaitProgress(recorder, (int) (OPS_PER_HISTORY * FAULT_AT_FRACTION));
                switch (scenario) {
                    case LEADER_LOSS:
                        faults.kill(leader);
                        break;
                    case PARTITION_3_2:
                        // The leader plus one peer, cut off from the other three.
                        faults.partition(leader);
                        faults.partition(otherThan(services, leader, random));
                        break;
                    case RESTART:
                        faults.restartAll();
                        break;
                    default:
                        break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("fault injection failed: " + e.getMessage());
            }
        }, "exp01-fault");
    }

    /** Waits until the clients have recorded {@code target} operations, so the fault overlaps them. */
    private static void awaitProgress(HistoryRecorder recorder, int target)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (recorder.size() < target && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }

    private static String otherThan(List<String> services, String leader, Random random) {
        List<String> candidates = new ArrayList<>(services);
        candidates.remove(leader);
        return candidates.get(random.nextInt(candidates.size()));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Command-line settings, with defaults matching {@code docker-compose.raft.yml}. */
    static final class Settings {
        final List<String> services =
                Arrays.asList("node0", "node1", "node2", "node3", "node4");
        final List<String> endpoints = Arrays.asList("localhost:9190", "localhost:9191",
                "localhost:9192", "localhost:9193", "localhost:9194");
        final List<String> healthUrls = Arrays.asList("http://localhost:8180",
                "http://localhost:8181", "http://localhost:8182", "http://localhost:8183",
                "http://localhost:8184");
        String composeFile = "docker-compose.raft.yml";
        int histories = 20;
        long seed = 1;
        Scenario scenario = Scenario.HEALTHY;

        static Settings parse(String[] args) {
            Settings settings = new Settings();
            for (int i = 0; i + 1 < args.length; i += 2) {
                switch (args[i]) {
                    case "--histories":
                        settings.histories = Integer.parseInt(args[i + 1]);
                        break;
                    case "--seed":
                        settings.seed = Long.parseLong(args[i + 1]);
                        break;
                    case "--scenario":
                        settings.scenario =
                                Scenario.valueOf(args[i + 1].toUpperCase(java.util.Locale.ROOT));
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
