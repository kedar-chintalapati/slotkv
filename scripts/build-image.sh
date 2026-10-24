#!/usr/bin/env bash
# Usage: [IMAGE=slotkv:dev] scripts/build-image.sh - build the kvnode/kvbench image and load it into the kind cluster.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need docker kind

export DOCKER_BUILDKIT=1 # the Dockerfile uses cache mounts
# Attestation manifests break `kind load` when docker uses the containerd image store (Docker Desktop).
noattest=
if docker build --help 2>/dev/null | grep -q -- --provenance; then noattest="--provenance=false --sbom=false"; fi
# shellcheck disable=SC2086
docker build $noattest -t "$IMAGE" .
kind load docker-image "$IMAGE" --name "$CLUSTER"
