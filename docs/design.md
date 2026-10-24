# slotkv design

slotkv is a key-value store that shards its keyspace across many Raft groups
and runs on Kubernetes. Each group has three replicas. In the kind deployment,
the three server pods are spread one per zone. Clients send each request to the
leader of the key's group.

It targets two limits of the usual setup:

1. **A single Raft group caps write throughput at one leader.** etcd runs every
   write through one group. slotkv hashes keys into slots, assigns slots to many
   groups, and spreads the group leaders across nodes.
2. **Kubernetes does not know about consensus.** The default StatefulSet rollout
   sends SIGTERM to Raft leaders mid-traffic, and Kubernetes marks a restarted
   replica ready before it has caught up. slotkv hands leadership off before
   exiting and only reports ready once its replicas have caught up.

Non-goals: multi-key transactions, live resharding, and dynamic membership.
Placement is fixed when the cluster is created.

## Layout

| Path | Contents |
|---|---|
| `proto/slotkv/v1`, `gen/slotkv/v1` | gRPC API and the Raft command format (generated with `buf generate`) |
| `internal/topology` | slot, group, and replica placement |
| `internal/node` | per-group Raft, state machine, gRPC server, lifecycle, leader balancer |
| `cmd/kvnode` | server binary |
| `client` | Go client: routing, retries, sessions |
| `internal/lincheck` | Porcupine model and history recorder |
| `cmd/kvbench` | load generator: throughput, steady-rate, and linearizability-check modes |
| `cmd/kvcheck` | offline checking and visualization of exported histories |
| `internal/sim`, `cmd/kvsim` | in-process cluster with fault injection |
| `deploy/kind`, `deploy/k8s`, `scripts` | kind cluster, manifests, experiment scripts |

## Placement

- `slot = crc32(key) mod num_slots` (default 1024 slots).
- `group = slot mod num_groups`. The slot table is the unit a future rebalancer
  would move. Nothing moves it today.
- Group `g` is replicated on nodes `g, g+1, g+2 (mod N)`. Its preferred leader is
  node `g mod N`. With N = 3, every node hosts a replica of every group, and each
  node is the preferred leader of a third of them.
- All nodes and clients compute the same topology from `--peers`, `--groups`,
  `--replication`, and `--slots`.

## Replication

- Each (node, group) pair runs one `hashicorp/raft` instance.
- Log and stable store (`--log-store=shared`, the default): one bbolt database
  per node, `<data>/raft-log.db`, with a log bucket and a stable-state bucket
  per group. Every write from every group goes through one committer goroutine.
  While a transaction syncs, new requests queue; the committer then takes all
  of them into the next transaction. One fsync makes appends from many groups
  durable at once (group commit).
- `--log-store=per-group` gives each group its own `raft-boltdb/v2` database at
  `<data>/g<id>/raft.db`, so every group fsyncs separately. It is kept as the
  baseline for the throughput ablation: with a sync per group, adding groups
  multiplies a node's fsyncs and shrinks the batch each one carries.
- Snapshots go to a `FileSnapshotStore` at `<data>/g<id>/snapshots` (retain 2).
- Transport: all groups share one TCP listener, the Raft port (default 7001).
  - A dialer writes the 4-byte big-endian group ID first.
  - The accept loop reads it and hands the connection to that group's
    `raft.StreamLayer`.
  - Each group uses `raft.NewNetworkTransport` over its stream layer.
  - `Addr()` returns the advertised Raft address from the topology, never the
    listen address.
- Raft server ID = node ID, server address = node Raft address. The address is
  the same for every group.
- Bootstrap: when `raft.HasExistingState` is false, every replica calls
  `BootstrapCluster` with the group's full voter configuration, identical on all
  replicas.
- Default timeouts:

  | Setting | Value |
  |---|---|
  | heartbeat | 1 s |
  | election | 1 s |
  | leader lease | 500 ms |
  | commit | 50 ms |
  | Raft RPC (network transport) | 3 s |

  Also `BatchApplyCh = true`, and `MaxAppendEntries = 1024`, hashicorp/raft's
  maximum, so a single group batches as much as Raft allows. The timeouts
  are hashicorp/raft's defaults. 500 ms heartbeats caused spurious elections
  when fsyncs stalled on a shared disk. Raft logs at `warn` through hclog.

## State machine

- Holds a `map[string][]byte` and a session table
  `client_id -> {seq, cached result}`.
- Commands are `slotkv.v1.Command` protos: put or delete, carrying a `Session`.
- Dedup: if `seq <= session.seq`, the command is a retry. Return the cached
  result and change nothing. Otherwise apply it and record `{seq, result}`.
  `--dedup=false` disables this. It is a negative control only.
- Snapshot and restore cover the map and the session table.

## Request path

Writes (Put, Delete):

1. Find the key's group. If this node is not its leader, answer
   `FailedPrecondition` with a `NotLeader` detail carrying the leader's ID and
   gRPC address when known.
2. `raft.Apply(command, 2s)` and wait. The response is the state machine's
   result.
3. Map Raft errors:
   - `ErrNotLeader` and `ErrLeadershipTransferInProgress`: not applied. Answer
     `NotLeader`.
   - `ErrLeadershipLost`, `ErrEnqueueTimeout`, `ErrRaftShutdown`, and timeouts:
     outcome unknown. Answer `Unavailable`. The client retries with the same
     session seq, and dedup makes the retry safe.

Reads (Get), `--read-mode=verify` (default):

1. The node must be the leader, and must have applied a `Barrier` in its current
   term (`read_ready`). Track leadership changes through `raft.Config.NotifyCh`
   (buffered): on gaining leadership, run `Barrier`, then record the ready term.
   On leadership loss, the node is not read-ready; readiness requires that it
   is leader now and the recorded barrier term equals its current term. Not
   leader: `NotLeader`. Not ready: `Unavailable`.
2. `VerifyLeader()`. A leadership failure answers `NotLeader`; a local wait
   timeout or Raft shutdown answers `Unavailable`.
3. Check `read_ready` again after `VerifyLeader()` completes. Leadership may
   have changed while the quorum check was in flight; a new term cannot serve
   reads until its own barrier has applied. If the node is still leader but
   that barrier is not ready, answer `Unavailable`; otherwise answer
   `NotLeader` when it lost leadership.
4. Read the local map. Writes are acknowledged only after the leader has applied
   them, so this read is linearizable.

`--read-mode=any` serves Get from any replica's local map. It is a negative
control: followers return stale data.

## Client

- Dial seeds, call `Topology`, open one `grpc.ClientConn` per node.
  - Targets use `passthrough:///host:port`, so every reconnect re-resolves DNS. A
    restarted pod gets a new IP.
  - Connect backoff: base 50 ms, max 1 s. The gRPC default of 120 s would strand
    a restarted node.
- Routing: key → slot → group → cached leader (from topology hints, else the
  preferred replica).
  - `NotLeader` with a hint: update the cache and retry at once.
  - `NotLeader` without a hint, `Unavailable`, or a transport error: try the next
    replica, back off 10–100 ms with jitter, and retry.
  - Each attempt has a 500 ms timeout. Retries continue until the caller's
    context expires.
- A `Session` has a random 64-bit `client_id` and a counter. A session has one
  request outstanding at a time. A retry reuses its seq. A failed Put or Delete
  has an unknown outcome.
- `Options.ReadAnyReplica` sends Gets to a random replica. It is only used with
  `--read-mode=any` as a negative control.

## Leader balancing

Every `--balance-interval` (default 1 s), a node that is not draining checks each
group it leads.

- If it is not the group's preferred leader, it asks the preferred node for
  fresh `Status`, bypassing the readiness cache so a recently draining target
  is not selected from a cached response. The status check and transfer are
  separate operations, so this reduces the race window without making them
  atomic.
- If that node is up, not draining, and caught up on the group
  (`applied_index + ready_max_lag >= leader commit_index`), the leader calls
  `LeadershipTransferToServer`. It also requires the target's applied index to
  be at least the leader's commit index from the previous tick. This avoids
  transferring to a follower whose Raft replication is still catching up from a
  longer outage.
- The target must report `Follower`, identify this source as its leader, and
  report the source's current term. Restored applied state alone does not prove
  that a restarted replica has heard from the current leader. The source checks
  its own leadership, term, and draining state again before starting the transfer.
- At most 2 transfers per node per tick.
- After a transfer fails and this node remains leader, it waits 10 balance
  intervals before retrying that group.

Without balancing, leadership piles onto whichever nodes restarted first, and
throughput drops.

## Lifecycle and Kubernetes

HTTP on `--http-listen` (default 8080):

- `/healthz` returns 200 while the process is up.
- `/status` returns `StatusResponse` as JSON.
- `/readyz` depends on `--readiness`:
  - `caught-up` (default): 200 only if the node is not draining and, for every
    local group, a leader is known and either this node is the leader or its
    `applied_index + ready_max_lag >= leader commit_index`. It fetches the
    leader's commit index with a `Status` RPC (300 ms timeout, cached 250 ms). The
    returned group must still identify that peer as leader, and the local
    leader view must stay unchanged during the lookup. The default for
    `--ready-max-lag` is 1000.
  - `serving` (baseline): 200 once the gRPC server is listening.

Shutdown on SIGTERM depends on `--shutdown`:

- `graceful` (default):
  1. Mark draining: `/readyz` returns 503, the balancer stops, and `Status`
     reports `draining`.
  2. Call `LeadershipTransfer()` on every group this node leads, in parallel,
     each with a 3 s bound. Repeat every 100 ms until the node leads no group,
     for up to 15 s. A transfer can fail while its target is briefly behind, and
     an election can hand this node another group while it drains.
  3. `grpc.Server.GracefulStop` (bounded at 5 s).
  4. Shut down the Raft groups and close the stores.
- `naive` (baseline): exit immediately, as a process with no signal handling
  would.

Manifests (`deploy/k8s`):

- Headless Service `kv` with `publishNotReadyAddresses: true`, so a restarting
  replica is reachable while it catches up.
- StatefulSet `kv`: 3 replicas, `podManagementPolicy: Parallel`, and topology
  spread on `topology.kubernetes.io/zone` (`maxSkew 1`, `DoNotSchedule`).
  `minReadySeconds: 5` requires a stable ready period before the next rolling
  replacement. Parallel creation allows all three Raft voters to bootstrap.
- Volumes and probes:
  - a 1 Gi PVC
  - a readiness probe on `/readyz` (period 1 s)
  - a lenient liveness probe on `/healthz`
  - `terminationGracePeriodSeconds: 30`
- A PodDisruptionBudget with `maxUnavailable: 1`.
- The kind cluster has 3 workers labeled as zones `zone-a`, `zone-b`, `zone-c`.

## kvnode flags

```
--id                    node ID (default $POD_NAME, then hostname)
--peers                 id=host:grpcPort:raftPort,... (required; same order on every node)
--groups 12  --replication 3  --slots 1024
--data-dir ./data/<id>
--grpc-listen :7000  --raft-listen :7001  --http-listen :8080
--shutdown graceful|naive         --readiness caught-up|serving
--read-mode verify|any            --dedup=true
--log-store shared|per-group
--balance-interval 1s             --ready-max-lag 1000
--heartbeat-timeout 1s  --election-timeout 1s  --leader-lease-timeout 500ms
--log-level info
```

## Testing and experiments

All experiments run on one machine.

- **Unit and integration tests:** `go test ./...`. They cover the state machine,
  the transport mux, a 3-node in-process cluster, and the Porcupine model.
- **Saved histories:** `kvbench --mode check --history PATH` exports a versioned
  JSON history before checking. Binary keys/values use base64 fields. A `-` path
  emits base64 `HISTORY` chunks, with each output write below 4 KiB so merged
  container logs cannot interleave diagnostics inside a large JSON line. The
  final `RESULT` line remains last.
  Export errors are logged and do not change the checker verdict.
  `kvcheck --timeout 10m --viz porcupine.html HISTORYFILE` rechecks offline.
  Unknown verdicts are inconclusive; they must not be counted as correctness
  proofs. The zone script extracts `results/raw/zone.history.json` for rechecking.
  The checker first tries a constructive positive certificate when all unknown
  writes can be deferred past the known history: it checks the unchanged known
  outputs and, only if legal, places those unknown writes at the tail. Early
  returning unknown writes cannot use this optimization. Otherwise the existing
  relaxed negative check and full check run within the remaining time budget.
- **kvsim:** in-process clusters over `raft.InmemTransport` and gRPC `bufconn`.
  The default is three nodes, four groups, replication factor three, eight
  sessions sharing eight keys, 50% Gets, 10% Deletes, and 40% Puts. Each
  session issues one request at a time, waits 120 ms between operations, and
  gives each operation a 1–2 s deadline. The default run injects faults for
  6 s, heals the cluster, restarts nodes one at a time, waits for readiness,
  then records a 1 s settling period before checking the history with
  Porcupine.
  - Faults are node crash and restart (same data directory), Raft network
    partitions of a node, and responses dropped after successful writes.
    Fault timing and choices use the configured seed; concurrent scheduling
    can still vary between runs.
  - `--read-mode=any` and `--dedup=false` with dropped responses are negative
    controls for stale reads and unsafe write retries. Their verdicts are
    reported by the checker rather than assumed here.
  - The command prints one JSON `RUN` line per run and a `RESULT` summary with
    `runs`, `ok`, `illegal`, `unknown`, `ops`, `failed_ops`, and `errors`. It
    exits nonzero when a run has an error; illegal and unknown verdicts are
    included in the summary and do not alone change the exit status.
- **Throughput:** kvbench `throughput` mode against the kind cluster.
  - Each server pod is limited to 1 CPU, so on one host the servers are the
    bottleneck rather than the shared disk or the load generator. At 2 CPUs a
    single group was not CPU-bound, and every configuration measured the round
    trip of fsyncs instead.
  - 4,096 closed-loop workers, 100% puts, 128-byte values, 100k keys, and a 5 s
    deadline. A closed loop's throughput is its concurrency divided by its
    latency, so the load must exceed what one group can batch per round.
  - 30 s after a 5 s warm-up. Each configuration runs three times, interleaved,
    and the report shows its median run.
  - `--groups` set to 1, 3, 6, and 12 with the shared log. One group is the
    etcd-style baseline. One more configuration uses 12 groups with
    `--log-store=per-group`.
- **Rolling restart:** kvbench `steady` mode, open loop at a fixed rate.
  - One-machine kind setup, 12 groups, default 2 CPU limit per server pod.
  - 2,000 requests/s, a 1 s request deadline, 150 s duration, and 20% reads.
    A request that misses its deadline counts as failed. `kubectl rollout
    restart` runs at +20 s.
  - Baseline: `--shutdown=naive --readiness=serving`. Compared with
    `--shutdown=graceful --readiness=caught-up`.
  - Diagnostic variants use `ROLLOUT_RATE`, `ROLLOUT_DURATION`, and
    `ROLLOUT_TAG`; a nonempty tag gives separate Jobs, raw files, and report.
    Capacity headroom must be checked before attributing errors to restart.
- **Zone outage:** one-machine kind setup, 12 groups, default 2 CPU limit per
  server pod. kvbench `check` mode runs at 200 requests/s over 256 keys with
  50% reads for 90 s, so each key's history stays small enough for Porcupine to
  reach a verdict. Docker pauses and resumes the kind worker in zone-b from
  +20 s to +50 s; pod `kv-0` is force-deleted at +75 s. Porcupine checks the
  recorded history.
