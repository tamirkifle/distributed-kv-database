#!/usr/bin/env bash
# Validate the LedgerKV Kubernetes manifests in k8s/ (quorum mode) and k8s/raft/ (Raft mode).
#
# The two sets are separate directories, not separate files in one, because a deployment runs one
# mode. `kubectl apply -f k8s/` is not recursive, so it brings up the quorum stack alone; the Raft
# stack needs `-f k8s/raft/` explicitly.
#
# Prefers `kubeval` (offline schema check, no cluster needed). Otherwise, if `kubectl`
# can reach a cluster, runs a client-side `kubectl apply --dry-run=client`. If neither
# is usable (no kubeval, and no reachable cluster), it prints a skip notice and exits 0
# so CI without a Kubernetes toolchain stays green — the structural contract is already
# covered by K8sManifestContractTest (mvn test).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if command -v kubeval >/dev/null 2>&1; then
  echo "validating with kubeval..."
  kubeval --strict k8s/*.yaml k8s/raft/*.yaml
  echo "OK (kubeval)"
elif command -v kubectl >/dev/null 2>&1 && kubectl cluster-info >/dev/null 2>&1; then
  echo "validating with kubectl --dry-run=client..."
  kubectl apply --dry-run=client -f k8s/
  kubectl apply --dry-run=client -f k8s/raft/
  echo "OK (kubectl dry-run)"
else
  echo "skip: no kubeval, and no reachable cluster for kubectl --dry-run=client;"
  echo "      structural contract is covered by K8sManifestContractTest (mvn test)."
  echo "      Install kubeval, or point kubectl at a cluster, to schema-check k8s/."
fi
