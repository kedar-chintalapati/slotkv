#!/usr/bin/env bash
# Usage: [GROUPS_LIST="1 3 6 12"] [TP_WORKERS=4096] [TP_CPU=1] [TP_ROUNDS=3] scripts/exp-throughput.sh - write throughput vs. Raft group count to results/throughput.md.
# Each server pod gets TP_CPU cores, standing in for a machine of that size, so on
# one host the servers, not the shared disk or the load generator, are the limit.
# Every configuration runs TP_ROUNDS times, interleaved; the report shows each one's median run.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need kubectl go

workers=${TP_WORKERS:-4096}
cpu=${TP_CPU:-1}
rounds=${TP_ROUNDS:-3}
configs=() # NAME:GROUPS:LOG_STORE:LABEL
for g in ${GROUPS_LIST:-1 3 6 12}; do configs+=("throughput-g$g:$g:shared:groups=$g"); done
# Ablation: the same 12 groups, but each with its own log and fsyncs.
configs+=("throughput-g12-per-group:12:per-group:groups=12, log per group")

for ((r = 1; r <= rounds; r++)); do
  for c in "${configs[@]}"; do
    IFS=: read -r name g store label <<<"$c"
    LOG_STORE=$store bash scripts/deploy.sh "$g" graceful caught-up "$cpu"
    bash scripts/bench.sh "$name-r$r" -- --mode throughput --workers "$workers" --deadline 5s --duration 30s \
      --warmup 5s --value-size 128 --keys 100000 --label "$label"
    for p in kv-0 kv-1 kv-2; do k logs "$p" >"results/raw/$name-r$r-$p.log" 2>&1 || true; done
  done
done

ops_s() { sed -n 's/.*"throughput_ops_s":\([0-9.]*\).*/\1/p' "$1" | head -n 1; }
medians=()
for c in "${configs[@]}"; do
  name=${c%%:*}
  medians+=("$(for ((r = 1; r <= rounds; r++)); do echo "$(ops_s "results/raw/$name-r$r.json") results/raw/$name-r$r.json"; done |
    sort -n | sed -n "$(((rounds + 1) / 2))p" | cut -d' ' -f2)")
done
write_report throughput results/throughput.md "${medians[@]}"
