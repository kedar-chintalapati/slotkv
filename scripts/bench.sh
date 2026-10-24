#!/usr/bin/env bash
# Usage: scripts/bench.sh NAME -- KVBENCH_ARGS... - run kvbench as a Job; saves results/raw/NAME.log and NAME.json.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need kubectl

[[ $# -ge 2 && $2 == -- ]] || die "usage: bench.sh NAME -- KVBENCH_ARGS..."
name=$1
shift 2
bench_start "$name" "$@"
bench_wait "$name"
