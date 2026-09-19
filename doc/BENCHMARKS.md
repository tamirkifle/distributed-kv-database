# Benchmarks

This document details the linearizability verification, leader failover, and partition recovery benchmarks run on LedgerKV, along with instructions for reproducing the results.

All measurements were collected on 2026-09-19 against commit `206fb2d` (`humanized-history`).

## Test Environment

* **Host:** macOS 24.6.0, arm64 (8 CPU, 13.6 GB RAM)
* **Runtime:** Docker 29.6.1, Java 11 (Eclipse Temurin)
* **Cluster:** 5-node containerized Raft deployment (`docker-compose.raft.yml`)
* **Harness:** In-tree concurrent history recorder and Wing & Gong linearizability checker (`src/main/java/com/ledgerkv/checker/`)

> [!NOTE]
> Every number below reflects single samples taken on hardware shared between the cluster and the test harness, not repeated trials across independent physical hosts. See [`LIMITATIONS.md`](LIMITATIONS.md).

---

## Linearizability Under Faults (exp-01)

Histories were generated using 4 concurrent clients executing 24 operations per history (40% writes, 20–60 ms client think time, one fresh key per history). Faults were injected at 30% progress to ensure they overlapped active operations. Linearizability was verified using the Wing & Gong search checker:

| Scenario | Histories | Linearizable | Violations | Inconclusive | Ops checked | Unknown-outcome ops |
|---|---|---|---|---|---|---|
| Healthy (control) | 25 | 25 | **0** | 0 | 600 | 0 |
| Leader SIGKILL | 25 | 25 | **0** | 0 | 600 | 0 |
| 3/2 Network partition | 15 of 25 | 15 | **0** | 0 | 360 | 53 |

### Observations

* **Latency under faults:** Slowest operation per history was 29–36 ms in healthy runs, rising to 1.8–2.9 s during leader loss, and up to 3.5 s under network partitions.
* **Unknown outcomes:** Under leader SIGKILL, zero operations resulted in unknown outcomes because the 3 s client retry deadline exceeded the sub-2 s failover window. Under network partitions, 53 operations timed out in-flight and were tracked as unknown outcomes; the checker validated all possible completion/drop interleavings without finding violations.
* **Bounded scope:** 1,560 operations across 65 histories yielded zero violations. This establishes absence of counterexamples within the tested concurrency and fault boundaries, not a formal proof for all interleavings.

---

## Leader Failover and Recovery (exp-02)

Failover was measured across 15 leader SIGKILL cycles on a 5-node cluster with persistent volumes retained. Outage duration is measured from the moment `SIGKILL` is sent to the leader container until the first subsequent write succeeds on the new leader (encompassing failure detection, election, and client redirection):

| Measure | n | Min | Median | p95 | Max |
|---|---|---|---|---|---|
| **Write outage** | 15 | 618 ms | **792 ms** | 1,087 ms | 1,087 ms |
| **Follower catch-up** | 15 | 801 ms | **1,562 ms** | 7,631 ms | 7,631 ms |

Across a second validation run of 15 cycles:
* **Write outage:** Median remained **790 ms** (p95 rose to 2,325 ms).
* **Data retention:** **30 of 30 acknowledged writes survived (0 lost)**. Every key was verified readable after cluster recovery.
* **Follower catch-up:** Median was **2.4 s** (log replay catch-up after container restart).

Tail percentiles (p95) varied between runs due to small sample size ($n=15$), where a single delayed election moves the 95th percentile. The median write outage (~790 ms) remained consistent across runs.

---

## Network Partition Dynamics

The 3/2 network partition experiment completed 15 of 25 planned histories before the cluster failed to reform within the 60 s timeout.

Container logs revealed 42 instances of `UnknownHostException` among peers. This was caused by the fault injector using `docker network disconnect` and `connect`: Docker assigns a new container IP upon reconnection, causing DNS resolution lag across gRPC channels during rapid churn. In contrast, real network partitions drop traffic without altering network addresses.

---

## Reproducing Measurements

Run the automated experiment suite:

```bash
./scripts/run-experiments.sh
```

Each run outputs timestamps, git commit hash, seeds, and raw execution logs to `results/<timestamp>/`.

### Storage Engine Microbenchmarks

To run the local JMH storage engine microbenchmarks and compaction amplification profiling:

```bash
mvn -Pbench -DskipTests clean package
java -jar target/benchmarks.jar -l                                        # List benchmarks
java -jar target/benchmarks.jar EngineBenchmarks                          # Engine micro-benchmarks
java -cp target/benchmarks.jar com.ledgerkv.bench.jmh.AmplificationMain   # Compaction amplification
```
