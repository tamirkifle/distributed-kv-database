# LedgerKV

[![CI](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml/badge.svg)](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml)
[![Java 11](https://img.shields.io/badge/Java-11-blue.svg)](https://adoptium.net/temurin/releases/?version=11)
[![Build](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)
[![Tests](https://img.shields.io/badge/tests-334%20green-success.svg)](#build--test)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A distributed key-value storage engine built from primitives — a write-ahead log, an
LSM-tree storage engine, quorum replication, consistent hashing, Raft consensus, and a
Jepsen-style linearizability checker — with real YCSB benchmarks, deterministic failure
injection, and full Docker / Kubernetes deployment. Written from scratch in Java 11.

> A study engine for how storage, replication, consensus, and failure interact at the
> correctness layer. Not production infrastructure, and it makes no production-readiness
> claims — but every subsystem is the real thing, built up from first principles and
> tested. See the [architecture diagram](docs/architecture-diagram.md).

## The two-minute tour

LedgerKV implements **two consistency models over one storage engine**, side by side:

- **Leaderless quorum (AP path)** — Dynamo-style. Any node coordinates; reads/writes use a
  configurable `N`/`R`/`W` quorum over a consistent-hash preference list, with vector-clock
  versioning, read repair, and hinted handoff. With `W+R<=N` it can serve stale reads.
- **Raft (CP path)** — leader election + log replication backed by the same on-disk WAL
  framing, driving a linearizable register state machine with at-most-once client dedup.

A hand-written **linearizability checker** runs both paths under deterministic partition
injection in CI and proves the contrast: the Raft path linearizes; the `W+R<=N` quorum path
does not (with a concrete stale-read witness).

Underneath both sits a from-scratch **LSM storage engine**: a CRC32 write-ahead log with
group-commit fsync and crash recovery; a sorted MemTable that flushes to immutable SSTables
(sparse index + per-table Bloom filter + bounded range scan); and background size-tiered /
leveled compaction. Measured YCSB throughput and a size-tiered-vs-leveled amplification
report are in [Benchmarks](#benchmarks).

## Why I built each component

Each subsystem maps to a real problem solved in a production system. This is the interview
surface — what the component is for, and the production analogue it mirrors.

| Component | The production problem it solves | Production analogue |
|---|---|---|
| **Write-ahead log** (`storage/wal`) | Durability without paying a random-write fsync per update: append sequentially, fsync in groups, replay the tail on restart, detect torn writes via CRC32. | The commit log in **Apache Cassandra**; the WAL in **PostgreSQL** / **RocksDB**. |
| **LSM tree** (`storage/lsm`) | Turn random writes into sequential ones: buffer in a sorted MemTable, flush to immutable SSTables, merge on read (newest-wins). Optimizes write-heavy workloads vs a B-tree. | **RocksDB** / **LevelDB**; Cassandra's storage engine; **Bitcask**'s append-only lineage. |
| **Bloom filter** (`storage/bloom`) | Skip the disk read for keys absent from an SSTable, cutting read amplification on misses. | Per-SSTable Bloom filters in **RocksDB** and **Cassandra**. |
| **Compaction** (`storage/compaction`) | Reclaim space and bound read amplification by merging SSTables — with the explicit size-tiered vs leveled write/read/space amplification trade-off measured in this repo. | **Cassandra**'s STCS / LCS; **RocksDB**'s leveled compaction. |
| **Quorum replication** (`cluster`, root) | Stay available under node loss with tunable consistency: `N`/`R`/`W` with `W+R>N` overlap, vector clocks, read repair, hinted handoff. | **Amazon Dynamo** / **Cassandra**'s tunable consistency. |
| **Consistent hashing** (`quorum/HashRing`) | Place keys on nodes so adding/removing a node moves only ~1/N of the data; virtual nodes (K=150) smooth the distribution. | The partitioner ring in **Dynamo** / **Cassandra**. |
| **gRPC transport** (`transport`) | A real wire protocol with a versioned schema and streaming range scans, instead of in-process calls. | gRPC/protobuf service definitions across modern infra (**etcd**, **CockroachDB**). |
| **Raft consensus** (`consensus`) | Linearizable replication via a single elected leader and a replicated log — strong consistency where the quorum path only offers tunable consistency. | **etcd** / **Consul** / **TiKV**; the Raft paper. |
| **Linearizability checker** (`checker`) | Empirically find consistency violations under partition (a bounded bug-finder, not a soundness proof) — the difference between "I think it's correct" and "I tested it." | **Jepsen** / **Knossos** / **Porcupine**. |
| **Prometheus + Grafana** (`metrics`, `monitoring/`) | Operate the cluster: per-node operation rates, p99 latency, and quorum-failure counters, scraped and dashboarded — including p99 under a `kill -9`. | The Prometheus/Grafana standard across cloud-native infra. |

## Architecture

The full diagram is in [`docs/architecture-diagram.md`](docs/architecture-diagram.md)
(renders on GitHub). Package map:

| Package | Responsibility |
|---|---|
| `storage/wal` | Write-ahead log: CRC32 framing, group-commit fsync, torn-tail recovery, checkpoint truncation |
| `storage/lsm` | MemTable, SSTable (sparse index + range scan), `LsmEngine`, k-way merge iterator |
| `storage/bloom` | Hand-written double-hashing Bloom filter |
| `storage/compaction` | Size-tiered + leveled compaction; background compactor with pin/unpin read-safety |
| root + `cluster` | Quorum core (`VersionedKVStore`, `QuorumKVStore*`), leaderless cluster, hinted handoff, `HashRing` |
| `consistency` | Vector-clock version metadata + conflict resolution |
| `consensus` (+ `raft/kv`) | Raft node, log, durable WAL persistence, gRPC transport, linearizable KV state machine |
| `transport` | gRPC `NodeServer`/`NodeClient`, replica RPCs, `ReplicaClient` seam |
| `node` | Container entrypoint (`NodeMain`), config, `/health` + `/metrics` server |
| `failure` | Deterministic partition / drop / latency injection |
| `metrics` | Operation metrics, latency summaries, hand-written Prometheus exporter |
| `checker` | Session-level consistency checker + Wing-&-Gong/Porcupine-style linearizability checker |

See [`docs/architecture.md`](docs/architecture.md) for the layered walkthrough and
[`docs/operations.md`](docs/operations.md) for running and operating a cluster.

## Build & test

```bash
mvn clean compile     # build
mvn test              # full test suite (the correctness surface) — 334 tests
mvn clean package     # build a runnable jar (target/ledgerkv.jar)
```

Targeted checks:

```bash
mvn test -Dtest=LinearizabilityCheckerTest          # the linearizability checker
mvn test -Dtest=CorrectnessHarnessTest              # both paths under partition (the headline)
mvn test -Dtest=HashRingTest                        # consistent-hash rebalance property
mvn test -Dtest=LsmEngineCompactionRaceTest         # read-vs-compaction race regression
```

## Running a cluster (Docker)

LedgerKV runs as a 5-node, quorum-replicated cluster (N=3, W=R=2) via Docker Compose:

```bash
docker compose up -d --build   # 5 nodes, each a quorum coordinator
curl -fsS http://localhost:8080/health   # -> OK
```

A write through any node is replicated across the key's preference list and survives the
loss of a node. See [`docs/operations.md`](docs/operations.md) for configuration, host
ports, Kubernetes (`StatefulSet`), and the `kill -9` / network-partition demo scripts.

## Observability

Each node exposes a **hand-written Prometheus exporter** at `/metrics` on its health port —
no Prometheus Java client dependency; the text is rendered straight from the node's live
quorum-metrics snapshot and served off the same JDK `HttpServer` as `/health`. Exported
series (labeled by `node`) cover operation counts, success/failure and quorum-failure
counters, p50/p95/**p99** latency, **hedged-request count** (`ledgerkv_hedged_requests_total`),
and repair activity.

**Tail-latency hardening (request hedging).** The quorum fan-out is concurrent and
deadline-bounded (`LEDGERKV_REQUEST_DEADLINE_MS`); on top of that, if the primary replicas
have not met quorum after a tunable **hedging delay** (`LEDGERKV_HEDGING_DELAY_MS`, default
50ms, sized to ~p95), the coordinator fires a single **backup request** to the next distinct
preference-list replica and takes whichever returns first — the canonical "Tail at Scale"
(Dean &amp; Barroso) technique that tames the slowest 1–5% of requests for a small (~2%) load
increase. Every hedge is counted into `ledgerkv_hedged_requests_total`, so the tactic is
observable on the dashboard.

A `monitoring/` overlay adds Prometheus (scraping all 5 nodes) and an auto-provisioned
Grafana dashboard (ops rate, **p99 latency**, quorum-failure rate, repairs):

```bash
docker compose -f docker-compose.yml -f monitoring/docker-compose.monitoring.yml up -d
# Grafana: http://localhost:3000   Prometheus: http://localhost:9099
```

**p99 under failure:** drive load, run `scripts/demo-kill.sh` to SIGKILL a replica, and
watch the p99-latency, hedged-request, and quorum-failure panels register the blip and
recover — with hedging on, the slow replica's would-be tail latency is absorbed by the
backup request rather than head-of-line-blocking the response.

![Grafana p99 under failure](docs/images/grafana-p99-under-failure.png)

> The screenshot above is a **manual capture step** (requires a live cluster + Grafana) — it
> is intentionally not committed/fabricated. To produce it: run the monitoring overlay, drive
> load, slow/kill a replica, compare p99 with and without hedging
> (`LEDGERKV_HEDGING_DELAY_MS`), and save the p99 panel to
> `docs/images/grafana-p99-under-failure.png`.

## Known limitations / hardening

Things I'd harden before this were anything other than a portfolio project:

- **Monotonic-clock latency measurement.** Quorum read/write latency is timed with
  `System.nanoTime()` (monotonic), not wall-clock. An earlier `System.currentTimeMillis()`
  implementation could record a *negative* duration when a wall-clock correction (NTP step or
  laptop sleep/resume) landed mid-operation — which, because the metrics types reject negative
  samples, made every `/metrics` scrape throw and the node read as DOWN in Prometheus while still
  healthy in Docker. The collector also defensively clamps durations at zero, so a stray sample
  can never poison the endpoint. Production systems measure elapsed time with monotonic clocks for
  exactly this reason.
- **Per-request deadlines + hedging on the quorum fan-out (implemented).** `LeaderlessKVCluster`
  now fans out to replicas **concurrently** on an injected executor, each call bounded by a
  per-request deadline (`LEDGERKV_REQUEST_DEADLINE_MS`), and fires a single **hedge** to the next
  replica after the hedging delay (`LEDGERKV_HEDGING_DELAY_MS`) — so a dead/slow replica no longer
  head-of-line-blocks its ring neighbors. (Earlier versions fanned out sequentially with no
  timeout.) Further hardening would tune the hedging delay adaptively off the live p95.
- **No snapshotting on the Raft log.** The Raft WAL grows unbounded (acknowledged in the Phase 3
  design); log compaction / snapshot install is the natural next step (Phase 5c).

## Benchmarks

All benchmarks run on demand via the `bench` Maven profile and are excluded from `mvn test`
(which stays fast and JMH-free). Build and run them with:

```bash
mvn -Pbench -DskipTests clean package          # builds target/benchmarks.jar
java -jar target/benchmarks.jar -l             # list benchmarks
java -jar target/benchmarks.jar EngineBenchmarks   # run the engine micro-benchmarks
java -cp target/benchmarks.jar com.ledgerkv.bench.jmh.AmplificationMain   # amplification report
```

The workload generator is an in-repo, deterministic YCSB-shaped driver (`com.ledgerkv.bench`):
workloads A–F under Zipfian and uniform key distributions, no external YCSB process.

> Numbers below are illustrative single-machine results (Apple Silicon / JDK 17), measured with
> reduced JMH iterations and a reduced (20k-record) dataset for a quick pass; they characterize
> relative behavior, not absolute peak throughput, and are not a production-readiness claim.

### Engine micro-benchmarks (JMH, throughput; 20k-record dataset)

| Operation | Throughput |
|---|---|
| `put` (SYNC WAL, fsync per write) | 0.22 ops/ms |
| `get` (hit) | 1790 ops/ms |
| `get` (miss, Bloom-filtered) | 7386 ops/ms |
| `scan` (100 entries) | 7.1 ops/ms |
| recovery / open (20k records) | 33.8 ms/op |

The `put` path fsyncs the WAL on every write (SYNC durability), so its throughput is fsync-bound;
the read-mostly YCSB numbers below use ASYNC durability and are far higher. Bloom filters make a
miss ~4× faster than a hit (no block read).

### YCSB workloads (JMH, throughput, ops/ms; ASYNC durability, 50k records)

| Workload | Zipfian | Uniform |
|---|---|---|
| A (50% read / 50% update) | 329 | 301 |
| C (100% read) | 333 | 322 |

### Compaction amplification: size-tiered vs leveled

Write amplification = SSTable bytes written ÷ user logical bytes (WAL excluded — a constant ~1×
common to both strategies). Space amplification = live on-disk bytes ÷ logical live bytes. Read
amplification = average number of live SSTables whose key range covers a sampled key (a structural
proxy; true read amplification also depends on Bloom false-positives and block reads).

## Compaction amplification (workload A, Zipfian, 50k records, 200k ops)

| Strategy | Write Amp | Read Amp | Space Amp | Live Tables | On-disk MB |
|---|---|---|---|---|---|
| size-tiered | 2.02× | 5.97 | 1.82× | 7 | 9.87 |
| leveled | 2.74× | 2.00 | 1.61× | 2 | 8.74 |

Size-tiered trades higher read/space amplification for lower write amplification; leveled inverts
that trade-off — the table above quantifies it for a fixed workload.

## For a reviewer / hiring manager

A 10-minute guided tour to assess depth, in order of "most signal per file":

1. **Linearizability under partition (the headline).**
   `src/test/java/com/ledgerkv/CorrectnessHarnessTest.java` points the checker at BOTH
   consistency models under deterministic partition injection and asserts the contrast —
   Raft linearizes, the `W+R<=N` quorum path does not (with a witness). The checker itself:
   `src/main/java/com/ledgerkv/checker/LinearizabilityChecker.java`
   (Wing-&-Gong/Porcupine-style search; partition-by-key + memoization + bounded guardrail).
2. **Raft.** `src/main/java/com/ledgerkv/raft/RaftNode.java` (election + log
   replication, deterministic logical-`tick()` timeouts), `RaftPersistence.java` (durable
   log over the Phase-1 WAL framing), and `raft/kv/RaftKvStateMachine.java`
   (linearizable register set with at-most-once dedup). Crash recovery:
   `src/test/java/com/ledgerkv/raft/RaftCrashRecoveryTest.java` (forks a JVM, halts it,
   proves term/vote/log survive).
3. **LSM storage engine.** `src/main/java/com/ledgerkv/storage/lsm/LsmEngine.java` (WAL +
   MemTable + SSTables + compactor), `storage/wal/WriteAheadLog.java` (CRC32 + group-commit
   fsync), `storage/lsm/SSTable.java` (sparse index + Bloom + range scan). The
   read-vs-compaction race fix (pin/unpin) is in
   `src/test/java/com/ledgerkv/storage/lsm/LsmEngineCompactionRaceTest.java`.
4. **Consistent hashing.** `src/main/java/com/ledgerkv/quorum/HashRing.java` (K=150 vnodes,
   FNV-1a + SplitMix64 hash) with the rebalance property test in
   `src/test/java/com/ledgerkv/quorum/HashRingTest.java` (add a node -> ~1/N keys move, all
   onto the new node).
5. **Distribution over gRPC.** `src/main/proto/ledgerkv.proto` (service + schema),
   `src/main/java/com/ledgerkv/quorum/LeaderlessKVCluster.java` (quorum coordinator), and
   `LeaderlessQuorumGrpcIntegrationTest.java` (3 real loopback nodes,
   write/read/read-repair/hinted-handoff/partitioned-read).
6. **Benchmarks.** `src/bench/java/com/ledgerkv/bench/` (JMH + a deterministic YCSB-shaped
   workload generator + the amplification harness). Numbers in [Benchmarks](#benchmarks).
7. **Ops.** `Dockerfile`, `docker-compose.yml`, `k8s/`, `monitoring/`, and
   [`docs/operations.md`](docs/operations.md) (kill -9 / partition demos, Prometheus series,
   p99-under-failure).

What to ask me about: group-commit fsync vs per-write durability; LSM vs B-tree write
amplification (and the STCS-vs-LCS table in this repo); why `W+R>N` is necessary but not
sufficient for linearizability; Raft leader step-down under partition; and what a bounded
linearizability checker can and cannot prove.

Build + test surface: `mvn test` (334 green), CI runs it on every push (badge at top).

## Roadmap

See [docs/roadmap.md](docs/roadmap.md). All five phases are complete: the WAL + LSM storage
engine with JMH/YCSB benchmarks (Phase 1); a gRPC-networked, containerized, Kubernetes-
deployed quorum cluster with Prometheus/Grafana (Phase 2); Raft consensus, a Jepsen-style
linearizability checker, and SSTable range scans (Phase 3); and this presentation layer
(Phase 4).
