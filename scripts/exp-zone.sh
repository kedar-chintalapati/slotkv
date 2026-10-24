#!/usr/bin/env bash
# Usage: [ZONE_RATE=200] [ZONE_KEYS=256] [ZONE_DURATION=90s] ZONE_TAG=history scripts/exp-zone.sh
# Linearizability check while zone-b is paused and kv-0 is force-deleted.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

rate=${ZONE_RATE:-200}
tag=${ZONE_TAG:-}
[[ $rate =~ ^[1-9][0-9]*$ ]] || die "ZONE_RATE must be a positive integer, got '$rate'"
if [[ -n $tag ]]; then
  [[ $tag =~ ^[a-z0-9]([-a-z0-9]{0,23}[a-z0-9])?$ ]] || die "ZONE_TAG must be a DNS-safe suffix of at most 25 lowercase letters, digits, or hyphens, got '$tag'"
  suffix="-$tag"
else
  suffix=
fi
job="zone$suffix"
label=zone
if [[ -n $tag ]]; then label="zone rate=${rate}/s"; fi
report="results/$job.md"
need docker kubectl go base64

bash scripts/deploy.sh 12 graceful caught-up

# The kind node container of zone-b (slotkv-worker2) is the one to pause.
node=$(k get nodes -l topology.kubernetes.io/zone=zone-b -o jsonpath='{.items[0].metadata.name}')
[[ -n $node ]] || die "no node labeled zone-b"
trap 'docker unpause "$node" >/dev/null 2>&1 || true' EXIT # never leave the node frozen

log "pod placement:"
k get pods -l app=kv -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' |
  while read -r pod n; do
    echo "  $pod -> $n ($(k get node "$n" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'))" >&2
  done

bench_start "$job" --mode check --rate "$rate" --keys "${ZONE_KEYS:-256}" --read-ratio 0.5 --duration "${ZONE_DURATION:-90s}" --warmup 0s --label "$label" --history=-
T0=$SECONDS
sleep_until 20
log "+20s: pausing $node (zone-b)"
docker pause "$node" >/dev/null
sleep_until 50
log "+50s: unpausing $node"
docker unpause "$node" >/dev/null
sleep_until 75
log "+75s: force-deleting pod kv-0"
k delete pod kv-0 --grace-period=0 --force
bench_wait "$job"
sed -n 's/^HISTORY //p' "results/raw/$job.log" | tr -d '\n' | base64 --decode > "results/raw/$job.history.json"
[[ -s results/raw/$job.history.json ]] || die "zone history missing from results/raw/$job.log"
write_report zone "$report" "results/raw/$job.json"
