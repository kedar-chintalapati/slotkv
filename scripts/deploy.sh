#!/usr/bin/env bash
# Usage: [LOG_STORE=shared|per-group] scripts/deploy.sh GROUPS SHUTDOWN READINESS [CPU_LIMIT=2] - fresh slotkv (graceful|naive, caught-up|serving); waits for 3/3 ready.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
need kubectl

(( $# >= 3 && $# <= 4 )) || die "usage: deploy.sh GROUPS SHUTDOWN READINESS [CPU_LIMIT=2]"
ngroups=$1 shutdown=$2 readiness=$3 cpu=${4:-2}
log_store=${LOG_STORE:-shared}
int='^[1-9][0-9]*$'
[[ $ngroups =~ $int ]] || die "GROUPS must be a positive integer, got '$ngroups'"
[[ $shutdown == graceful || $shutdown == naive ]] || die "SHUTDOWN must be graceful or naive, got '$shutdown'"
[[ $readiness == caught-up || $readiness == serving ]] || die "READINESS must be caught-up or serving, got '$readiness'"
[[ $log_store == shared || $log_store == per-group ]] || die "LOG_STORE must be shared or per-group, got '$log_store'"

# Fresh state: drop old bench jobs, the StatefulSet with its pods, and the PVCs (which outlive it).
log "removing the previous deployment"
k delete job -l app=kvbench --ignore-not-found --cascade=foreground --timeout=120s >/dev/null
k delete statefulset kv --ignore-not-found --cascade=foreground --timeout=180s >/dev/null
k delete pod -l app=kv --ignore-not-found --timeout=180s >/dev/null
# StatefulSet volumeClaimTemplates do not inherit pod labels. The generated
# claim names are data-kv-0 through data-kv-2, so select them explicitly to
# ensure every experiment starts with empty Raft state.
k delete pvc data-kv-0 data-kv-1 data-kv-2 --ignore-not-found --timeout=180s >/dev/null

log "deploying groups=$ngroups shutdown=$shutdown readiness=$readiness log_store=$log_store cpu_limit=$cpu image=$IMAGE"
render deploy/k8s/slotkv.yaml.tmpl IMAGE="$IMAGE" GROUPS="$ngroups" SHUTDOWN="$shutdown" \
  READINESS="$readiness" LOG_STORE="$log_store" CPU_LIMIT="$cpu" | k apply -f - >/dev/null
k rollout status statefulset/kv --timeout=300s
k get pods -l app=kv -o wide
