# Limitations

Every performance and fault-tolerance measurement in this repository came off a single laptop: macOS 24.6.0, arm64, 8 CPU, 13.6 GB RAM, Docker 29.6.1. This document details what those experiments do not establish, and the architectural constraints of both consistency modes.

## Measurement and Experimental Scope

* **Single-host container environment:** Five containers on one machine share a single CPU, kernel, and physical NVMe drive. This does not model physical network latency, asymmetric packet loss, packet reordering, or independent machine failures.
* **Small sample sizes:** The reported failover figures come from two runs of 15 leader SIGKILL cycles. While medians reproduced closely (790–792 ms write outage), tail percentiles varied significantly (p95 of 1,087 ms vs. 2,325 ms). The data is not averaged over enough trials to rigorously characterize tail distributions.
* **Throughput and latency under sustained load are unmeasured:** No sustained saturation or load-stepping experiments (e.g., YCSB throughput sweeps over a live gRPC cluster) have been run. No operations/second or p99 request-latency claims are made for cluster deployments.
* **Linearizability checks are bounded:** 1,560 operations across 65 histories (24 operations per history, 4 concurrent clients) found zero violations under the Wing & Gong search. This establishes that no bugs were found within these histories, but finite histories are a bug-finding tool, not a mathematical proof of correctness across all possible interleavings.
* **Partition injection mechanism:** The test harness uses `docker network disconnect` and `docker network connect`, which detaches containers and assigns new IP addresses upon reconnection. This introduces DNS re-resolution lag in gRPC channels that is harsher than real-world packet drops.

## Raft Consensus Constraints (CP Mode)

* **Static group membership:** Membership is fixed at startup (3 or 5 members). Raft joint consensus and dynamic membership changes (§4.3) are not implemented; resizing the cluster requires a redeployment.
* **Single-shard Raft:** A single Raft consensus group manages the entire keyspace. Write throughput is bounded by the capacity of a single leader node; multi-Raft sharding is not implemented.
* **In-memory state machine:** The KV state machine is maintained as an in-memory hash map, backed durably by the Raft WAL and periodic log snapshots. The dataset must fit entirely in JVM heap. The custom LSM-tree storage engine is used only in quorum mode, not in Raft mode.
* **No range scans (`SCAN`):** The state machine uses an unordered map, and range scans over applied state would not be linearizable against concurrent overlapping mutations. Raft nodes reject `SCAN` requests with `UNIMPLEMENTED` rather than returning non-linearizable or partial ranges.
* **Bounded client sessions with undetectable eviction:** At-most-once retry semantics rely on a per-client session table bounded at 4,096 entries (evicted LRU, deterministically across replicas). There is no explicit `RegisterClient` handshake (Ongaro §6.3); a request without an existing session creates one. If an active client's session is evicted under heavy client churn, a subsequent retried mutation could be applied twice.
* **Single outstanding mutation per client:** `LedgerKvClusterClient` serializes mutations per client. Concurrent in-flight mutations per client session (requiring a window of sequence/response pairs) are not supported.
* **Minority follower readiness reporting:** A follower isolated in a minority partition alongside its former leader continues receiving local heartbeats. Its `/readyz` health check remains 200 and it continues returning redirects to the isolated leader. The leader itself fails quorum checks and returns 503 (dropping out of Kubernetes Service endpoints), so clients experience a single redirection hop, never stale reads.

## Leaderless Quorum Constraints (AP Mode)

* **Tombstone accumulation:** Deletions replicate as versioned tombstones that remain in LSM SSTables indefinitely. There is currently no `gc_grace_seconds` compaction reaper to safely purge tombstones after all replicas have converged.
* **Read-before-write coordination overhead:** To preserve causal consistency and track siblings across concurrent updates, the coordinating node must perform a read of the key's existing causal context (vector clocks) from replicas before stamping and replicating a new write version.
