# Write throughput

Peak write throughput as the keyspace is split over more Raft groups.

Setup: the same host as the other experiments (Intel i7-13620H, 16 threads,
kind in Docker Desktop). There are three server pods, one per zone, each
limited to 1 CPU and 1 GiB. A pod therefore behaves like a single-core
machine, and the servers, not the shared disk or the load generator, are the
bottleneck. kvbench runs 4,096 closed-loop workers doing 128-byte puts over
100k keys, measured for 30 s after a 5 s warm-up, with a 5 s request deadline.
No request failed in any run. Durability is the same in every configuration:
an acknowledged write has been fsynced on a majority of its group.

Each configuration ran three times, interleaved. The table shows each
configuration's median run.

| label | ops/s | p50 (ms) | p99 (ms) | p999 (ms) | speedup |
|---|---:|---:|---:|---:|---:|
| groups=1 | 19608 | 192.13 | 463.87 | 735.74 | 1.00x |
| groups=3 | 24610 | 155.26 | 361.21 | 448.51 | 1.26x |
| groups=6 | 23524 | 150.40 | 485.38 | 867.84 | 1.20x |
| groups=12 | 23297 | 150.01 | 487.17 | 827.39 | 1.19x |
| groups=12, log per group | 18368 | 201.22 | 717.31 | 902.14 | 0.94x |

Ops/s in each round:

| configuration | round 1 | round 2 | round 3 |
|---|---:|---:|---:|
| groups=1 | 14,449 | 22,871 | 19,607 |
| groups=3 | 24,609 | 27,233 | 23,658 |
| groups=6 | 23,524 | 23,009 | 25,015 |
| groups=12 | 22,032 | 23,364 | 23,297 |
| groups=12, log per group | 17,927 | 18,368 | 18,621 |

- **One leader versus three.** With one group, every write goes through one
  leader. Three groups put a leader on each node: 1.26× the throughput, with
  p99 latency 22% lower and p999 39% lower.
- **Beyond one group per node.** Gains flatten past that point, because each
  group's batches get smaller and the per-write overhead rises.
- **Log per group.** With a separate log per group, each group fsyncs on its
  own, and 12 groups fall below a single group (0.94×). The shared log commits
  every group's appends in one transaction. At 12 groups it is 1.27× faster than
  per-group logs.

Reproduce with `bash scripts/exp-throughput.sh` (about 45 minutes).
