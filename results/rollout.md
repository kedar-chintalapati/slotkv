# Rolling restart

`kubectl rollout restart statefulset/kv` under steady load, comparing a server
that knows nothing about Raft with one that hands off leadership and reports
ready only once it has caught up.

Setup: one Windows 11 host (Intel i7-13620H, 16 threads) running kind in Docker
Desktop. Three server pods, one per zone, each limited to 2 CPUs and 1 GiB.
12 Raft groups. kvbench sends 2,000 requests/s open loop (20% reads, 100k keys)
for 150 s, with a 1 s deadline per request; a request that misses it counts as
failed. The restart begins at +20 s and replaces all three pods.

- **baseline**: `--shutdown=naive --readiness=serving`. The process exits on
  SIGTERM and reports ready as soon as it is listening.
- **slotkv**: `--shutdown=graceful --readiness=caught-up`. On SIGTERM it
  transfers leadership of every group it leads before exiting, and it reports
  ready only when each of its replicas has caught up with its leader.

| label | failed | error % | p99 (ms) | p999 (ms) | max (ms) | longest failing run (s) |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 44667 | 14.889% | 785.41 | 983.55 | 1003.52 | 13 |
| slotkv | 0 | 0.000% | 200.32 | 348.93 | 783.87 | 0 |

The baseline kills Raft leaders mid-traffic. Every group led by the exiting pod
loses its leader until an election finishes, and the restarted pod is marked
ready before it has caught up. With handoff and caught-up readiness, all
300,000 requests succeeded.

Reproduce with `bash scripts/exp-rollout.sh`.
