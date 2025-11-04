#!/usr/bin/env bash
# Demonstrates that a value written through the quorum survives a hard (SIGKILL) node loss.
# Requires: docker compose, grpcurl (https://github.com/fullstorydev/grpcurl).
set -euo pipefail

PROTO="src/main/proto/ledgerkv.proto"
SVC="ledgerkv.LedgerKvNode"
KEY="demo-key"
# grpcurl encodes proto `bytes` as base64 JSON; "aGVsbG8=" decodes to "hello".
VALUE_B64="aGVsbG8="

command -v grpcurl >/dev/null || { echo "install grpcurl first"; exit 1; }

echo "==> Bringing the 5-node cluster up"
docker compose up -d --build
echo "==> Waiting for healthy nodes"; sleep 25

echo "==> PUT $KEY via node0 (localhost:9090)"
grpcurl -plaintext -import-path . -proto "$PROTO" \
  -d "{\"key\":\"$KEY\",\"value\":\"$VALUE_B64\"}" \
  localhost:9090 "$SVC/Put"

echo "==> GET $KEY via node1 (localhost:9091) before failure"
grpcurl -plaintext -import-path . -proto "$PROTO" \
  -d "{\"key\":\"$KEY\"}" localhost:9091 "$SVC/Get"

echo "==> SIGKILL node2 (a replica)"
docker compose kill -s SIGKILL node2

echo "==> GET $KEY via node1 AFTER node2 is killed -> value must still be present"
grpcurl -plaintext -import-path . -proto "$PROTO" \
  -d "{\"key\":\"$KEY\"}" localhost:9091 "$SVC/Get"

echo "==> Demo complete. 'docker compose down' to clean up."
