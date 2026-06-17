# LedgerKV

[![CI](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml/badge.svg)](https://github.com/tamirkifle/distributed-kv-database/actions/workflows/ci.yml)
[![Java 11](https://img.shields.io/badge/Java-11-blue.svg)](https://adoptium.net/temurin/releases/?version=11)
[![Build](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)
[![Tests](https://img.shields.io/badge/tests-353%20green-success.svg)](#build--test)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

LedgerKV is a distributed key-value store built on an LSM-tree storage engine, with two
selectable consistency models: a Dynamo-style leaderless quorum for high availability, and
Raft for linearizable strong consistency. It ships as a gRPC-networked cluster with
consistent-hash partitioning, Prometheus/Grafana observability, and Docker / Kubernetes
deployment out of the box.

## Features

- **LSM-tree storage engine** — write-ahead log with group-commit fsync and crash recovery,
  MemTable + SSTables with sparse indexes, Bloom filters, and background compaction
  (size-tiered or leveled).
- **Two consistency models, your choice per use case:**
  - **Leaderless quorum (AP)** — any node coordinates; tunable `N`/`R`/`W` quorum with
    vector-clock versioning, read repair, and hinted handoff for availability under node loss.
  - **Raft (CP)** — leader election and log replication for linearizable reads/writes, with
    log compaction and snapshotting so the log doesn't grow unbounded.
- **Consistent hashing** with virtual nodes for even key distribution and minimal data
  movement on cluster resize.
- **gRPC transport** with a versioned protobuf schema and streaming range scans.
- **Tail-latency controls** — deadline-bounded concurrent replica fan-out and request
  hedging to absorb slow or dead replicas.
- **Built-in observability** — a Prometheus exporter on every node and a ready-to-run
  Grafana dashboard (throughput, latency percentiles, quorum failures, hedged requests).
- **Docker Compose and Kubernetes manifests** for a multi-node cluster, plus scripts to
  demo node failure and network partitions.

## Quick start

Run a 5-node cluster locally with Docker Compose:

```bash
docker compose up -d --build
curl -fsS http://localhost:8080/health   # -> OK
```

Any node accepts reads and writes and coordinates replication across the key's preference
list — a write survives the loss of a node. See [`docs/operations.md`](docs/operations.md)
for configuration, ports, Kubernetes deployment, and failure-injection demo scripts.

## Build & test

Requires Java 11 and Maven.

```bash
mvn clean compile     # build
mvn test              # full test suite — 353 tests
mvn clean package     # build a runnable jar (target/ledgerkv.jar)
```

## Architecture

See [`docs/architecture-diagram.md`](docs/architecture-diagram.md) for the full system
diagram and [`docs/architecture.md`](docs/architecture.md) for a layer-by-layer walkthrough.

| Package | Responsibility |
|---|---|
| `storage/wal` | Write-ahead log: framing, group-commit fsync, crash recovery |
| `storage/lsm` | MemTable, SSTable, engine, merge iterator |
| `storage/bloom` | Bloom filter |
| `storage/compaction` | Size-tiered and leveled compaction |
| `cluster` | Quorum coordination, hinted handoff, consistent-hash ring |
| `consensus` (+ `raft/kv`) | Raft node, log, durable persistence, gRPC transport, KV state machine |
| `transport` | gRPC client/server, replica RPCs |
| `node` | Container entrypoint, config, health/metrics endpoints |
| `metrics` | Metrics collection and Prometheus exporter |
| `checker` | Consistency and linearizability checkers |

## Observability

Every node exposes Prometheus metrics at `/metrics` on its health port — operation counts,
success/failure and quorum-failure counters, p50/p95/p99 latency, hedged-request count, and
repair activity. Bring up Prometheus and a pre-provisioned Grafana dashboard alongside the
cluster:

```bash
docker compose -f docker-compose.yml -f monitoring/docker-compose.monitoring.yml up -d
# Grafana: http://localhost:3000   Prometheus: http://localhost:9099
```

## Benchmarks

An in-repo, deterministic YCSB-shaped workload generator drives JMH benchmarks and a
compaction-amplification report, run on demand via the `bench` Maven profile (excluded from
`mvn test`):

```bash
mvn -Pbench -DskipTests clean package
java -jar target/benchmarks.jar -l                                        # list benchmarks
java -jar target/benchmarks.jar EngineBenchmarks                          # engine micro-benchmarks
java -cp target/benchmarks.jar com.ledgerkv.bench.jmh.AmplificationMain   # amplification report
```

## Documentation

- [Architecture diagram](docs/architecture-diagram.md)
- [Architecture walkthrough](docs/architecture.md)
- [Operations guide](docs/operations.md) — running, configuring, and operating a cluster
- [Roadmap](docs/roadmap.md)

## License

[MIT](LICENSE)
