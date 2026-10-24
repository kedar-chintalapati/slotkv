# Fault simulation

`kvsim` runs each seed as an in-process cluster. Each cluster has three nodes and
four Raft groups. Raft runs over `raft.InmemTransport` and gRPC over `bufconn`,
and each node has its own on-disk shared log. Eight sessions issue Gets (50%),
Puts (40%) and Deletes (10%) on eight shared keys, with 1–2 s deadlines.

For 6 s, every 300–700 ms, the harness does one of the following:

- crashes a node and restarts it on the same data directory
- cuts the node off from its Raft peers
- drops 5% of successful write responses, so the client retries writes that were
  already applied

It then heals the cluster, restarts every node in turn, and checks the whole
history with Porcupine.

| Run | Seeds | Linearizable | Illegal | Recorded ops |
|---|---|---:|---:|---:|
| Default faults | 1001–1200 | 200 | 0 | 88,492 |
| Negative control: Gets served by any replica (`--read-mode any`) | 5001–5020 | 0 | 20 | 9,230 |
| Negative control: dedup off, 20% of write responses dropped (`--dedup=false --drop-prob 0.2`) | 6001–6020 | 0 | 20 | 8,327 |

The negative controls show that the checker catches the bugs this design
prevents:

- **Stale follower reads.** Linearizable reads need the leader's barrier and
  quorum check.
- **Retried writes applied twice.** Deduplication by session sequence number
  prevents this.

```sh
go run ./cmd/kvsim --runs 200 --parallel 4 --seed 1001
go run ./cmd/kvsim --runs 20 --parallel 4 --seed 5001 --read-mode any
go run ./cmd/kvsim --runs 20 --parallel 4 --seed 6001 --dedup=false --drop-prob 0.2
```
