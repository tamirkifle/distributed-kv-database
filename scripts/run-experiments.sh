#!/usr/bin/env bash
# Runs the exp-01 correctness histories and the exp-02 failover cycles against a Raft cluster
# started from docker-compose.raft.yml, and writes every artifact under results/<timestamp>/.
#
# What gets saved is the point. A number without its seed, its commit, and the machine it ran on
# cannot be reproduced or defended, so this records all three next to the raw output before it
# records any summary.
#
# Usage: scripts/run-experiments.sh [histories] [cycles]
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

HISTORIES="${1:-20}"
CYCLES="${2:-10}"
COMPOSE="docker-compose.raft.yml"
JAR="target/ledgerkv.jar"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="results/$STAMP"

command -v docker >/dev/null || { echo "docker is required"; exit 1; }
[[ -f "$JAR" ]] || { echo "build first: mvn -DskipTests clean package"; exit 1; }

mkdir -p "$OUT"

# Provenance first, so an interrupted run still says what it was.
{
  echo "timestamp:   $STAMP"
  echo "commit:      $(git rev-parse HEAD)"
  echo "branch:      $(git rev-parse --abbrev-ref HEAD)"
  echo "dirty:       $(git status --porcelain | wc -l | tr -d ' ') modified files"
  echo "uname:       $(uname -a)"
  echo "java:        $(java -version 2>&1 | head -1)"
  echo "docker:      $(docker info --format '{{.ServerVersion}} {{.NCPU}}cpu {{.MemTotal}}bytes' 2>/dev/null)"
  echo "histories:   $HISTORIES"
  echo "cycles:      $CYCLES"
} > "$OUT/provenance.txt"
git diff HEAD > "$OUT/working-tree.patch"

echo "==> artifacts: $OUT"

# Each experiment gets a brand-new cluster, volumes included.
#
# Not tidiness. Repeated partitions leave members that have been detached and reattached to the
# Compose network, and a cluster in that state takes far longer to re-form than a fresh one --
# long enough that the next experiment measures the previous experiment's damage. Starting clean
# also guarantees an empty keyspace, so no history can read a value an earlier run wrote.
fresh_cluster() {
  docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
  docker compose -f "$COMPOSE" up -d >>"$OUT/compose-up.log" 2>&1
  for _ in $(seq 1 90); do
    for port in 8180 8181 8182 8183 8184; do
      if curl -fsS --max-time 2 "http://localhost:$port/metrics" 2>/dev/null \
          | grep -q 'role="leader"} 1'; then
        return 0
      fi
    done
    sleep 2
  done
  echo "no leader after 180s" >&2
  return 1
}

echo "==> building the image"
docker compose -f "$COMPOSE" build >"$OUT/compose-up.log" 2>&1

status=0
for scenario in healthy leader_loss partition_3_2; do
  echo "==> exp-01 $scenario ($HISTORIES histories)"
  fresh_cluster || { status=1; continue; }
  java -cp "$JAR" com.ledgerkv.experiment.CorrectnessExperiment \
    --histories "$HISTORIES" --seed 1 --scenario "$scenario" --compose "$COMPOSE" \
    >"$OUT/exp01-$scenario.txt" 2>&1 || status=1
  tail -4 "$OUT/exp01-$scenario.txt"
  docker compose -f "$COMPOSE" logs --no-color >"$OUT/cluster-$scenario.log" 2>&1 || true
done

echo "==> exp-02 failover ($CYCLES cycles)"
fresh_cluster || status=1
java -cp "$JAR" com.ledgerkv.experiment.FailoverExperiment \
  --cycles "$CYCLES" --compose "$COMPOSE" >"$OUT/exp02-failover.txt" 2>&1 || status=1
tail -8 "$OUT/exp02-failover.txt"
docker compose -f "$COMPOSE" logs --no-color >"$OUT/cluster-exp02.log" 2>&1 || true

echo "==> tearing the cluster down"
docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true

echo "==> done; artifacts in $OUT"
exit "$status"
