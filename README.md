# LedgerKV

[![CI](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml/badge.svg)](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml)

A distributed key-value storage engine built from primitives — quorum replication,
versioned values, read repair, anti-entropy, and hinted handoff — with deterministic
failure injection and a consistency-history checker.

> Status: in-process distributed prototype. Not production infrastructure. This is a
> from-scratch engine for studying how replication, failure, and repair affect
> correctness. See [docs/roadmap.md](docs/roadmap.md) for the build-out (storage engine,
> gRPC cluster, Kubernetes, Raft, linearizability testing).

## What it does

- Versioned key-value storage with per-key monotonic versions and conflict metadata.
- Leaderless quorum reads/writes with configurable `N`, `R`, `W` (W+R>N overlap).
- Replica convergence via read repair, anti-entropy, and hinted-handoff replay.
- Deterministic injection of node failures, dropped messages, latency, and partitions.
- Operation metrics: read/write/repair counts and p50/p95/p99 latency summaries.
- A consistency-history checker for stale-read, read-your-writes, and monotonic-read violations.

## Architecture

| Package        | Responsibility |
|----------------|----------------|
| root           | Quorum core: `VersionedKVStore`, `QuorumKVStore`, `QuorumKVStoreWithRepair`, `QuorumConfig`, `QuorumResponse`, `ReadRepairStrategy` |
| `cluster`      | Leaderless cluster, membership, nodes, hinted handoff |
| `consistency`  | Version metadata and conflict resolution |
| `failure`      | Deterministic failure / partition injection |
| `metrics`      | Operation metrics and latency summaries |
| `checker`      | Consistency-history violation checker |

See [docs/architecture.md](docs/architecture.md) for detail.

## Build & test

```bash
mvn clean compile     # build
mvn test              # full test suite (the correctness surface)
mvn clean package     # build a jar
```

Targeted checks:

```bash
mvn test -Dtest=QuorumKVStoreTest
mvn test -Dtest=ReadRepairTest#testBasicReadRepair
mvn test -Dtest=AntiEntropyRepairTest
```

## Running a cluster (Docker)

LedgerKV runs as a 5-node, quorum-replicated cluster (N=3, W=R=2) via Docker
Compose:

```bash
docker compose up -d --build   # 5 nodes, each a quorum coordinator
curl -fsS http://localhost:8080/health   # -> OK
```

A write through any node is replicated across the key's preference list and
survives the loss of a node. See [`docs/operations.md`](docs/operations.md) for
configuration, host ports, and the `kill -9` / network-partition demo scripts.

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

## Roadmap

See [docs/roadmap.md](docs/roadmap.md). The write-ahead log + LSM-tree storage engine (with the
JMH/YCSB benchmark numbers above) is complete; next up is a gRPC-networked, containerized,
Kubernetes-deployed cluster, then Raft consensus and Jepsen-style linearizability testing.
