#!/usr/bin/env bash
# Demonstrates read availability while a node is network-partitioned from the cluster.
# Requires: docker compose, grpcurl.
set -euo pipefail

PROTO="src/main/proto/ledgerkv.proto"
SVC="ledgerkv.LedgerKvNode"
KEY="partition-key"
VALUE_B64="d29ybGQ="  # "world"
NET="$(docker compose ps --format '{{.Name}}' node0 | sed 's/-node0.*//')_default"

command -v grpcurl >/dev/null || { echo "install grpcurl first"; exit 1; }

echo "==> Bringing the cluster up"
docker compose up -d --build
echo "==> Waiting for healthy nodes"; sleep 25

echo "==> PUT $KEY via node0"
grpcurl -plaintext -import-path . -proto "$PROTO" \
  -d "{\"key\":\"$KEY\",\"value\":\"$VALUE_B64\"}" localhost:9090 "$SVC/Put"

echo "==> Disconnect node3 from the cluster network (partition)"
docker network disconnect "$NET" "$(docker compose ps -q node3)"

echo "==> GET $KEY via node0 while node3 is partitioned -> still served by the surviving quorum"
grpcurl -plaintext -import-path . -proto "$PROTO" \
  -d "{\"key\":\"$KEY\"}" localhost:9090 "$SVC/Get"

echo "==> Reconnect node3"
docker network connect "$NET" "$(docker compose ps -q node3)"
echo "==> Demo complete. 'docker compose down' to clean up."
