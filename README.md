# slotkv

A sharded, Raft-replicated key-value store in Go.

- **Sharding:** keys hash into 1,024 slots, spread over many Raft groups of three
  replicas each.
- **Routing:** clients send gRPC requests straight to each group's leader.
- **Kubernetes:** servers cooperate with it, so rolling upgrades don't drop
  requests.

## Results

Everything ran on one machine: an Intel i7-13620H (16 threads) with Docker
Desktop and kind, three server pods, one per zone. The linked pages have each
setup.

| Experiment | Baseline | slotkv |
|---|---|---|
| [Write throughput](results/throughput.md), 3 pods × 1 CPU, median of 3 runs | 1 Raft group: 19,608 ops/s | 3 groups: 24,610 ops/s (**1.26×**), p999 latency 39% lower |
| [Group commit](results/throughput.md), 12 groups | One log per group: 18,368 ops/s | Shared group-commit log: 23,297 ops/s (**1.27×**) |
| [Rolling restart](results/rollout.md) at 2,000 req/s | 44,667 failed requests (14.9%) | **0 failed** |
| [Zone outage](results/zone.md) plus forced pod kill, 18,000 ops | n/a | **Linearizable** (Porcupine) |
| [Fault simulation](results/simulator.md): crashes, partitions, lost responses | Negative controls: 40/40 runs illegal | **200/200 runs linearizable** |

## How it works

- **Sharding.**
  - Each key goes to `slot = crc32(key) mod 1024`, and every slot belongs to a
    Raft group.
  - Group `g` is replicated on nodes `g, g+1, g+2` and prefers node `g` as
    leader, so leadership spreads evenly. A balancer moves it back after
    restarts.
  - Every node and client derives the same placement from the peer list.
- **Replication.**
  - Each group is a hashicorp/raft instance. All groups on a node share one TCP
    port: connections start with a 4-byte group ID.
  - All groups on a node also share one bbolt log. A single committer merges
    pending appends from every group into one transaction, so one fsync makes
    all of them durable.
  - With a log per group, each group fsyncs on its own. 12 groups then fell
    below the throughput of a single group.
- **Consistency.**
  - Writes go through the group leader and are acknowledged once applied.
  - Reads are linearizable. A new leader applies a barrier, and the leader
    confirms it still holds a quorum before each read.
  - Each client session numbers its writes, so the state machine applies a
    retried write only once.
- **Kubernetes.**
  - On SIGTERM a node hands off leadership of every group it leads, then stops.
  - `/readyz` passes only once every local replica has caught up with its
    leader. A StatefulSet rollout waits for that before restarting the next pod.
  - A PodDisruptionBudget keeps node drains from taking out a quorum.
  - Pods are spread across zones.

[docs/design.md](docs/design.md) covers the protocol, failure handling, and
experiment setup.

## Run locally

Requires Go 1.27 or newer. Start three nodes, one per terminal, using the same
peer list for each:

```sh
PEERS='n0=127.0.0.1:7100:7101,n1=127.0.0.1:7200:7201,n2=127.0.0.1:7300:7301'
go run ./cmd/kvnode --id n0 --peers "$PEERS" --grpc-listen :7100 --raft-listen :7101 --http-listen :8100 --data-dir data/n0
go run ./cmd/kvnode --id n1 --peers "$PEERS" --grpc-listen :7200 --raft-listen :7201 --http-listen :8200 --data-dir data/n1
go run ./cmd/kvnode --id n2 --peers "$PEERS" --grpc-listen :7300 --raft-listen :7301 --http-listen :8300 --data-dir data/n2
```

The defaults are 12 groups, three replicas per group, and 1,024 slots. Each
node serves gRPC, plus `/healthz`, `/readyz` and `/status` over HTTP. Ctrl+C
shuts a node down gracefully.

Then load it, or check it for linearizability:

```sh
go run ./cmd/kvbench --seeds 127.0.0.1:7100 --mode throughput --workers 256 --duration 10s
go run ./cmd/kvbench --seeds 127.0.0.1:7100 --mode check --duration 15s --viz check.html
```

`kvbench` prints a final `RESULT` JSON line. `--history FILE` saves a check-mode
history, and `go run ./cmd/kvcheck FILE` rechecks it offline.

## Run on kind

These scripts need Docker, kind, kubectl, Go and Bash. They create a cluster
with three workers labeled as separate zones:

```sh
bash scripts/cluster-up.sh
bash scripts/build-image.sh
bash scripts/deploy.sh 12 graceful caught-up
bash scripts/bench.sh sample -- --mode throughput --workers 256 --duration 30s
```

Each experiment script redeploys with fresh data, then writes its table to
`results/`:

```sh
bash scripts/exp-throughput.sh  # groups 1-12, shared log vs. log per group
bash scripts/exp-rollout.sh     # rolling restart: naive baseline vs. graceful
bash scripts/exp-zone.sh        # zone pause and pod kill, checked with Porcupine
```

`bash scripts/cluster-down.sh` removes the cluster.

## Fault simulation

`kvsim` runs seeded fault schedules against in-process clusters and checks every
history:

```sh
go run ./cmd/kvsim --runs 200 --parallel 4
go run ./cmd/kvsim --runs 20 --read-mode any                # negative control: stale reads
go run ./cmd/kvsim --runs 20 --dedup=false --drop-prob 0.2  # negative control: duplicate writes
```

## Layout

| Path | Contents |
|---|---|
| `cmd/kvnode` | server |
| `internal/node` | per-group Raft, shared log store, transport mux, state machine, lifecycle, balancer |
| `client` | Go client: routing, leader hints, retries, sessions |
| `cmd/kvbench`, `cmd/kvcheck` | load generator and offline history checker |
| `internal/sim`, `cmd/kvsim` | in-process fault simulator |
| `internal/lincheck` | Porcupine model and history recording |
| `deploy/`, `scripts/` | kind cluster, manifests, experiment scripts |

## Limitations

- **Single-key only.** There are no multi-key transactions.
- **Fixed placement.** Changing the number of groups or nodes requires a fresh
  cluster.
- **No security.** There is no authentication or TLS.
- **Sessions grow forever.** Dedup records never expire, and a `Session` must
  not be used from more than one goroutine.
- **One machine.** The numbers above come from a single host. A real
  deployment's throughput depends on its disks and network.

Run the tests with `go test ./...`.
