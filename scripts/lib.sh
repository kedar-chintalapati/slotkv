# Shared helpers for the slotkv scripts. Source this file; do not run it.
# Everything runs from the repo root with relative paths, which native Windows
# tools (kubectl, kind, docker) need under Git Bash.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
mkdir -p results/raw # raw/ is gitignored, so it is missing on a fresh clone
export MSYS_NO_PATHCONV=1 # Git Bash: leave /container/paths in arguments alone

for d in "$HOME/sdk/go1.27.3/go/bin" "$HOME/go/bin" /usr/local/go/bin; do
  if [[ -d $d ]]; then PATH="$d:$PATH"; fi
done

CLUSTER=slotkv
NS=slotkv
IMAGE=${IMAGE:-slotkv:dev}
SEEDS=kv-0.kv.slotkv.svc.cluster.local:7000,kv-1.kv.slotkv.svc.cluster.local:7000,kv-2.kv.slotkv.svc.cluster.local:7000

log() { printf '[%s] %s\n' "$(date +%T)" "$*" >&2; }
die() { log "error: $*"; exit 1; }
need() { local c; for c; do command -v "$c" >/dev/null || die "$c not found on PATH"; done; }

# k: kubectl pinned to the slotkv cluster and namespace.
k() { kubectl --context "kind-$CLUSTER" -n "$NS" "$@"; }

# now_ms: unix time in milliseconds (whole seconds where date lacks %N, as on macOS).
now_ms() {
  local t
  t=$(date +%s%3N)
  [[ $t =~ ^[0-9]{13}$ ]] || t=$(date +%s)000
  echo "$t"
}

# esc VALUE: escape VALUE for the replacement side of sed 's|..|..|' (\ & | and newlines).
esc() { printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g' -e '$!s/$/\\/'; }

# render TEMPLATE KEY=VALUE...: print TEMPLATE with every __KEY__ replaced by VALUE.
render() {
  local tmpl=$1 kv out sedargs
  shift
  sedargs=()
  for kv in "$@"; do sedargs+=(-e "s|__${kv%%=*}__|$(esc "${kv#*=}")|g"); done
  out=$(sed "${sedargs[@]}" "$tmpl")
  [[ $out != *__[A-Z]*__* ]] || die "$tmpl: unfilled placeholder"
  printf '%s\n' "$out"
}

# yaml_list ARG...: ARGs as single-quoted YAML list items, indented as in bench-job.yaml.tmpl.
yaml_list() {
  local a
  for a in "$@"; do printf "        - '%s'\n" "$(printf '%s' "$a" | sed "s/'/''/g")"; done
}

# bench_start NAME KVBENCH_ARGS...: (re)create Job kvbench-NAME, with --seeds prepended,
# and return once its pod is running, so callers can time fault injection from there.
bench_start() {
  local name=$1 job=kvbench-$1 re='^[a-z0-9]([-a-z0-9]{0,40}[a-z0-9])?$' phase i
  shift
  [[ $name =~ $re ]] || die "bad bench name '$name': use lowercase letters, digits and '-'"
  k delete job "$job" --ignore-not-found --cascade=foreground --timeout=120s >/dev/null
  render deploy/k8s/bench-job.yaml.tmpl IMAGE="$IMAGE" NAME="$name" \
    ARGS="$(yaml_list "--seeds=$SEEDS" "$@")" | k apply -f - >/dev/null
  log "started $job: $*"
  for ((i = 0; i < 180; i++)); do
    phase=$(k get pods -l "job-name=$job" -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)
    case $phase in Running | Succeeded | Failed) return 0 ;; esac
    sleep 1
  done
  die "$job: pod did not start within 3m (see: kubectl -n $NS describe pod -l job-name=$job)"
}

# bench_wait NAME [TIMEOUT_S]: wait for Job kvbench-NAME (default $BENCH_TIMEOUT or 1800s), then save
# its logs to results/raw/NAME.log and the JSON after the last "RESULT " line to results/raw/NAME.json.
bench_wait() {
  local name=$1 job=kvbench-$1 st end raw=results/raw/$1
  end=$((SECONDS + ${2:-${BENCH_TIMEOUT:-1800}}))
  while :; do
    st=$(k get job "$job" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
    [[ $st == 1/* || $st == */[1-9]* ]] && break
    (( SECONDS < end )) || die "$job: timed out"
    sleep 2
  done
  k logs "job/$job" >"$raw.log" || die "$job: cannot read logs"
  [[ $st == 1/* ]] || die "$job failed; see $raw.log"
  tr -d '\r' <"$raw.log" | sed -n 's/^RESULT //p' | tail -n 1 >"$raw.json"
  [[ -s $raw.json ]] || die "no RESULT line in $raw.log"
  log "$job done: $raw.log, $raw.json"
}

# sleep_until SECS: sleep until SECS seconds after T0 (set T0=$SECONDS when the clock starts).
sleep_until() {
  local d=$((T0 + $1 - SECONDS))
  (( d <= 0 )) || sleep "$d"
}

# write_report KIND OUTFILE FILE...: write the kvreport table for FILEs to OUTFILE and echo it.
write_report() {
  local kind=$1 out=$2 table
  shift 2
  table=$(go run ./cmd/kvreport "$kind" "$@")
  printf '%s\n' "$table" | tee "$out"
  log "wrote $out"
}
