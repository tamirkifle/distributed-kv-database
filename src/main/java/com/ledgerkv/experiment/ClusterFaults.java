package com.ledgerkv.experiment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Injects and reverses faults in a Docker Compose deployment, and timestamps each one.
 *
 * <p>Every fault is recorded in an {@link #events() event log} with the instant it was applied.
 * That log is the only thing that makes a recovery measurement meaningful: without a fault
 * timestamp taken outside the cluster, "time to recover" is measured from whenever the harness
 * happened to notice, which flatters the result by however long the harness was asleep.
 *
 * <p>Faults are reversed by {@link #healAll()}, which runs even after a failed experiment. A
 * partition left in place silently poisons every run after it.
 */
public final class ClusterFaults implements AutoCloseable {

    private final String composeFile;
    private final List<String> services;
    private final List<String> killed = new ArrayList<>();
    private final List<String> partitioned = new ArrayList<>();
    private final List<Event> events = new ArrayList<>();
    private String networkName;

    public ClusterFaults(String composeFile, List<String> services) {
        this.composeFile = Objects.requireNonNull(composeFile, "composeFile");
        this.services = new ArrayList<>(Objects.requireNonNull(services, "services"));
    }

    /** One fault or repair, with the instant it took effect. */
    public static final class Event {
        public final Instant at;
        public final String what;

        Event(Instant at, String what) {
            this.at = at;
            this.what = what;
        }

        @Override
        public String toString() {
            return at + " " + what;
        }
    }

    public synchronized List<Event> events() {
        return new ArrayList<>(events);
    }

    /** Drops the event log, so a per-iteration caller reports only its own faults. */
    public synchronized void clearEvents() {
        events.clear();
    }

    /** Resolves and caches the network name up front, while nothing is disconnected yet. */
    public void resolveNetwork() throws IOException, InterruptedException {
        network();
    }

    /** SIGKILL, not stop: a graceful shutdown is a different fault with a different recovery. */
    public synchronized Instant kill(String service) throws IOException, InterruptedException {
        run("kill", "-s", "SIGKILL", service);
        Instant at = Instant.now();
        killed.add(service);
        events.add(new Event(at, "kill " + service));
        return at;
    }

    public synchronized void start(String service) throws IOException, InterruptedException {
        run("start", service);
        killed.remove(service);
        events.add(new Event(Instant.now(), "start " + service));
    }

    /**
     * Cuts {@code service} off the Compose network. Unlike a kill, the process keeps running and
     * keeps believing whatever it believed, which is what a partition has to reproduce.
     */
    public synchronized Instant partition(String service) throws IOException, InterruptedException {
        String container = containerId(service);
        exec(Arrays.asList("docker", "network", "disconnect", network(), container));
        Instant at = Instant.now();
        partitioned.add(service);
        events.add(new Event(at, "partition " + service));
        return at;
    }

    public synchronized void heal(String service) throws IOException, InterruptedException {
        exec(Arrays.asList("docker", "network", "connect", network(), containerId(service)));
        partitioned.remove(service);
        events.add(new Event(Instant.now(), "heal " + service));
    }

    /** Reverses every fault still in place. Safe to call twice. */
    public synchronized void healAll() {
        for (String service : new ArrayList<>(partitioned)) {
            try {
                heal(service);
            } catch (IOException | InterruptedException e) {
                System.err.println("could not heal " + service + ": " + e.getMessage());
            }
        }
        for (String service : new ArrayList<>(killed)) {
            try {
                start(service);
            } catch (IOException | InterruptedException e) {
                System.err.println("could not restart " + service + ": " + e.getMessage());
            }
        }
    }

    /** Stops and restarts every member, keeping volumes, for the full-restart durability case. */
    public synchronized void restartAll() throws IOException, InterruptedException {
        run("stop");
        events.add(new Event(Instant.now(), "stop all"));
        run("start");
        events.add(new Event(Instant.now(), "start all"));
    }

    /**
     * The Compose network, resolved once and cached.
     *
     * <p>Caching is not an optimization. A disconnected container reports no networks, so looking
     * this up while a partition is in place can fail on the very container being healed — and a
     * partition that cannot be reversed silently corrupts every run after it.
     */
    private synchronized String network() throws IOException, InterruptedException {
        if (networkName != null) {
            return networkName;
        }
        for (String service : services) {
            String name;
            try {
                name = exec(Arrays.asList("docker", "inspect", "-f",
                        "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
                        containerId(service))).trim();
            } catch (IOException notRunning) {
                continue;
            }
            if (!name.isEmpty()) {
                networkName = name;
                return networkName;
            }
        }
        throw new IOException("could not determine the compose network from " + services);
    }

    private String containerId(String service) throws IOException, InterruptedException {
        String id = run("ps", "-q", service).trim();
        if (id.isEmpty()) {
            throw new IOException("no container for service " + service);
        }
        return id.lines().findFirst().orElse(id);
    }

    private String run(String... composeArgs) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(Arrays.asList("docker", "compose", "-f", composeFile));
        command.addAll(Arrays.asList(composeArgs));
        return exec(command);
    }

    private static String exec(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append('\n');
            }
        }
        if (!process.waitFor(Duration.ofMinutes(2).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            throw new IOException("timed out running " + command);
        }
        if (process.exitValue() != 0) {
            throw new IOException(command + " exited " + process.exitValue() + ": " + output);
        }
        return output.toString();
    }

    @Override
    public void close() {
        healAll();
    }
}
