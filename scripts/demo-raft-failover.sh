#!/usr/bin/env bash
# Kills the Raft leader and shows that the surviving majority keeps serving, that the write
# acknowledged before the kill is still there afterwards, and how long the gap lasted.
#
# Unlike the quorum demos this one has to find the leader first, because only the leader serves.
# It reads the role straight off each node's /metrics rather than guessing from the node index.
#
# Requires: docker compose, grpcurl (https://github.com/fullstorydev/grpcurl).
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

COMPOSE="docker-compose.raft.yml"
PROTO="src/main/proto/ledgerkv.proto"
SVC="ledgerkv.LedgerKvNode"
CLIENT_ID="demo-raft"
# grpcurl encodes proto `bytes` as base64 JSON; "aGVsbG8=" decodes to "hello".
VALUE_B64="aGVsbG8="

command -v grpcurl >/dev/null || { echo "install grpcurl first"; exit 1; }

# Host port pairs published by docker-compose.raft.yml: <grpc>:<health>.
NODES=(9190:8180 9191:8181 9192:8182 9193:8183 9194:8184)

kv() { # kv <grpc-port> <method> <json>
  grpcurl -plaintext -import-path . -proto "$PROTO" -d "$3" "localhost:$1" "$SVC/$2"
}

leader_port() {
  for pair in "${NODES[@]}"; do
    local grpc="${pair%%:*}" health="${pair##*:}"
    if curl -fsS --max-time 2 "http://localhost:${health}/metrics" 2>/dev/null \
        | grep -q '^ledgerkv_raft_role{.*role="leader"} 1$'; then
      echo "$grpc"
      return 0
    fi
  done
  return 1
}

await_leader() { # await_leader <timeout-seconds>; echoes the leader's gRPC port
  local deadline=$(( SECONDS + $1 ))
  while (( SECONDS < deadline )); do
    if port="$(leader_port)"; then
      echo "$port"
      return 0
    fi
    sleep 1
  done
  echo "no leader within $1s" >&2
  return 1
}

service_for_port() { # map a published gRPC port back to its compose service
  for i in "${!NODES[@]}"; do
    [[ "${NODES[$i]%%:*}" == "$1" ]] && { echo "node$i"; return 0; }
  done
  return 1
}

echo "==> Bringing the 5-node Raft cluster up"
docker compose -f "$COMPOSE" up -d --build
echo "==> Waiting for the first election"
LEADER="$(await_leader 90)"
echo "    leader is $(service_for_port "$LEADER") (localhost:$LEADER)"

echo "==> PUT before-kill via the leader"
kv "$LEADER" Put "{\"key\":\"before-kill\",\"value\":\"$VALUE_B64\",\"client_id\":\"$CLIENT_ID\",\"sequence\":1}"

echo "==> A follower should redirect rather than serve"
for pair in "${NODES[@]}"; do
  FOLLOWER="${pair%%:*}"
  [[ "$FOLLOWER" == "$LEADER" ]] && continue
  kv "$FOLLOWER" Get '{"key":"before-kill"}'
  break
done

echo "==> SIGKILL the leader $(service_for_port "$LEADER")"
KILLED="$(service_for_port "$LEADER")"
KILL_AT=$SECONDS
docker compose -f "$COMPOSE" kill -s SIGKILL "$KILLED"

echo "==> Waiting for the surviving majority to elect a replacement"
NEW_LEADER="$(await_leader 60)"
echo "    new leader is $(service_for_port "$NEW_LEADER") after $(( SECONDS - KILL_AT ))s"

echo "==> PUT after-kill via the new leader (a write must be possible again)"
kv "$NEW_LEADER" Put "{\"key\":\"after-kill\",\"value\":\"$VALUE_B64\",\"client_id\":\"$CLIENT_ID\",\"sequence\":2}"

echo "==> The write acknowledged before the kill must have survived it"
BEFORE="$(kv "$NEW_LEADER" Get '{"key":"before-kill"}')"
echo "$BEFORE"
grep -q '"found": true' <<<"$BEFORE" || {
  echo "FAIL: an acknowledged write was lost across the leader kill" >&2
  exit 1
}

echo "==> Restoring $KILLED"
docker compose -f "$COMPOSE" start "$KILLED"

echo "==> Demo complete. 'docker compose -f $COMPOSE down -v' to clean up."
