#!/usr/bin/env bash
# Usage: scripts/cluster-up.sh - create (or reuse) the kind cluster "slotkv": 1 control plane + 3 zoned workers.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need docker kind kubectl

docker info >/dev/null 2>&1 || die "docker is not running (start Docker Desktop or the docker daemon)"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "cluster $CLUSTER exists; refreshing kubeconfig"
  kind export kubeconfig --name "$CLUSTER" # the API server's host port changes when docker restarts
else
  kind create cluster --config deploy/kind/cluster.yaml --wait 120s
fi

k wait --for=condition=Ready node --all --timeout=180s
for w in worker:zone-a worker2:zone-b worker3:zone-c; do
  got=$(k get node "$CLUSTER-${w%%:*}" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
  [[ $got == "${w#*:}" ]] || die "node $CLUSTER-${w%%:*} has zone '$got', want ${w#*:}"
done
k get storageclass standard >/dev/null || die "storage class 'standard' is missing"
k get nodes -L topology.kubernetes.io/zone
k version
