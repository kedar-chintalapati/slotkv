#!/usr/bin/env bash
# Usage: ROLLOUT_RATE=2000 ROLLOUT_DURATION=150s ROLLOUT_TAG=500 scripts/exp-rollout.sh
# Rolling restart under steady load. A nonempty tag separates raw files, Jobs, and report.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

rate=${ROLLOUT_RATE:-2000}
duration=${ROLLOUT_DURATION:-150s}
tag=${ROLLOUT_TAG:-}
[[ $rate =~ ^[1-9][0-9]*$ ]] || die "ROLLOUT_RATE must be a positive integer, got '$rate'"
if [[ $duration =~ ^([1-9][0-9]{0,8})(ms|s|m|h)$ ]]; then
  duration_value=${BASH_REMATCH[1]}
  duration_unit=${BASH_REMATCH[2]}
else
  die "ROLLOUT_DURATION must be an integer duration (ms, s, m, or h), got '$duration'"
fi
case $duration_unit in
  ms) (( duration_value >= 30000 )) || die "ROLLOUT_DURATION must be at least 30s so the rollout occurs under load" ;;
  s)  (( duration_value >= 30 )) || die "ROLLOUT_DURATION must be at least 30s so the rollout occurs under load" ;;
esac
if [[ -n $tag ]]; then
  # bench_start permits names up to 42 characters; this suffix fits both rollout job names.
  [[ $tag =~ ^[a-z0-9]([-a-z0-9]{0,23}[a-z0-9])?$ ]] || die "ROLLOUT_TAG must be a DNS-safe suffix of at most 25 lowercase letters, digits, or hyphens, got '$tag'"
  suffix="-$tag"
else
  suffix=
fi
report="results/rollout${suffix}.md"
need kubectl go

log_stream_pids=()
stop_log_streams() {
  local pid i
  for pid in "${log_stream_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${log_stream_pids[@]}"; do
    # Give kubectl a short grace period, then force-stop and reap any stragglers.
    for ((i = 0; i < 50; i++)); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  log_stream_pids=()
}
trap stop_log_streams EXIT

files=()
for cfg in baseline:naive:serving slotkv:graceful:caught-up; do
  IFS=: read -r name shutdown readiness <<<"$cfg"
  job="rollout-$name$suffix"
  label=$name
  if [[ -n $tag ]]; then label="$name rate=${rate}/s"; fi
  bash scripts/deploy.sh 12 "$shutdown" "$readiness"
  bench_start "$job" --mode steady --rate "$rate" --deadline 1s --duration "$duration" --warmup 0s \
    --read-ratio 0.2 --keys 100000 --label "$label"
  for pod in kv-0 kv-1 kv-2; do
    kubectl --context "kind-$CLUSTER" -n "$NS" logs --timestamps --follow "$pod" \
      >"results/raw/$job-$pod.log" 2>&1 &
    log_stream_pids+=("$!")
  done
  T0=$SECONDS
  sleep_until 20
  start=$(now_ms)
  k rollout restart statefulset/kv
  k rollout status statefulset/kv --timeout=300s
  end=$(now_ms)
  stop_log_streams
  printf 'start_unix_ms=%s\nend_unix_ms=%s\n' "$start" "$end" >"results/raw/$job.times"
  log "$name: rollout took $(( (end - start) / 1000 ))s"
  bench_wait "$job"
  files+=("results/raw/$job.json")
done
write_report rollout "$report" "${files[@]}"
