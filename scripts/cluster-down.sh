#!/usr/bin/env bash
# Usage: scripts/cluster-down.sh - delete the kind cluster "slotkv" and everything in it.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need kind

kind delete cluster --name "$CLUSTER"
