# VerdictDB

VerdictDB is a failure-tested distributed KV store and benchmark harness for studying how consistency choices affect AI evaluation correctness.

Modern AI products depend on eval traces, prompt versions, retriever versions, model outputs, quality scores, latency, cost, and freshness metadata. VerdictDB is not positioned as a production database replacement. It is a distributed-systems lab that shows when stale or inconsistent eval state can corrupt model deployment decisions.

The primary proof path is the stale-eval-decision demo: a baseline-vs-candidate model comparison that looks safe under weak, stale reads, then flips after partition healing and read repair expose the fresher candidate trace.

## What VerdictDB Does

- Stores versioned key-value data across replicated nodes.
- Supports leader/follower replication and leaderless quorum reads/writes.
- Exposes configurable `N`, `R`, and `W` consistency settings.
- Simulates node failures, message drops, latency, and partitions.
- Detects stale reads, conflicts, quorum failures, and session consistency violations.
- Repairs divergent replicas through read repair, anti-entropy repair, and hinted handoff replay.
- Tracks operational signals such as stale reads, repair counts, quorum behavior, conflicts, and latency.
- Provides an AI eval trace workload, decision audit report, and consistency matrix.
- Demonstrates how weak reads can produce a wrong model comparison under failure.

## Demo

Run the compact wrong-decision scenario:

```bash
mvn -q exec:java -Dexec.mainClass=com.distributed.kv.cli.VerdictDbCli -Dexec.args="demo wrong-decision --nodes 5 --weak-consistency R1W1 --repair read-repair"
```

Run the same scenario with the Markdown audit report:

```bash
mvn -q exec:java -Dexec.mainClass=com.distributed.kv.cli.VerdictDbCli -Dexec.args="demo wrong-decision --nodes 5 --weak-consistency R1W1 --repair read-repair --report"
```

Run the scenario matrix across weak, quorum, and all-replica consistency:

```bash
mvn -q exec:java -Dexec.mainClass=com.distributed.kv.cli.VerdictDbCli -Dexec.args="demo wrong-decision --nodes 5 --weak-consistency R1W1 --repair read-repair --matrix"
```

The demo starts a 5-node in-process cluster, writes baseline and candidate eval traces, isolates replicas with a deterministic partition, and shows a weak `R=1/W=1` read that incorrectly favors the candidate. It then heals the partition, repairs the candidate trace, and prints the corrected comparison where the candidate is no longer better than baseline.

## Use Case: AI Eval Trace Storage

VerdictDB is designed around eval and observability records such as:

```text
run_id
prompt
retrieved_docs
response
model
latency_ms
score
version
```

The product question is not only "can we store traces?" It is:

- What consistency level is enough for eval decisions?
- How stale can quality or latency metadata become before a team makes a bad deployment call?
- How do partitions, retries, and read repair affect observed model quality, cost, and freshness?
- Can baseline and candidate model runs be compared reproducibly under failure?

## Project Structure

```text
verdictdb/
├── src/main/java/com/distributed/kv/  # Core implementation
├── src/test/java/com/distributed/kv/  # Behavior tests
└── docs/                              # Active guide, roadmap, reports, and archive
```

## Commands

```bash
mvn clean compile
mvn test
mvn clean package
```

Useful targeted checks:

```bash
mvn test -Dtest=EvalDecisionMatrixTest
mvn test -Dtest=EvalConsistencyRiskTest,EvalDecisionAuditReportTest
mvn test -Dtest=QuorumKVStoreTest
mvn test -Dtest=ReadRepairTest#testBasicReadRepair
```

## Documentation

- [Project guide](docs/project-guide.md): product direction, architecture, operating tradeoffs, engineering rules, and gotchas.
- [Roadmap](docs/roadmap.md): current V1 state, next work options, future version arc, and gates.
- [Current focus](docs/current-focus.md): short handoff for the next implementation session.
- [Reports](docs/reports/): committed sample benchmark, audit, matrix, and demo outputs.
- [Archive](docs/archive/): historical specs and superseded planning docs.

## Project Status

VerdictDB is currently an in-process distributed KV prototype and benchmark harness with tests for leaderless quorum behavior, deterministic failure injection, partitions, conflict handling, stale reads, read repair, hinted handoff, eval trace storage, decision audit reporting, workload matrices, and consistency history checking.

Real users should not rely on it as infrastructure yet. The current surface is runnable evidence: a wrong-decision demo, audit report, matrix, benchmark/report primitives, and correctness checks that connect distributed consistency behavior to AI eval decisions.

## Roadmap Snapshot

- V0 MVP foundation: complete.
- V1 eval correctness failure report: audit report and decision matrix implemented; next work should improve report clarity, CSV/export shape, or evaluator-facing docs.
- V2 trace API and workload realism: deferred until V1 report surface is polished.
- V3 correctness and failure testing depth: deeper histories, repair convergence, and counterexample summaries.
- Later: process runtime, observability exports, durability/recovery, and final research artifact.
