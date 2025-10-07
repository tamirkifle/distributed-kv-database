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

## Roadmap

See [docs/roadmap.md](docs/roadmap.md). Next up: a write-ahead log + LSM-tree storage
engine with real (JMH/YCSB) benchmark numbers, then a gRPC-networked, containerized,
Kubernetes-deployed cluster, then Raft consensus and Jepsen-style linearizability testing.
