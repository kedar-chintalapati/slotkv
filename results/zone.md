# Zone outage

Same host and deployment as the rolling restart: 12 groups, 2 CPUs per pod,
graceful shutdown, and caught-up readiness. kvbench check mode sends 200
requests/s for 90 s over 256 keys: 50% Gets, with 20% of writes being Deletes.
Each request has a 1 s deadline.

At +20 s the zone-b worker is frozen with `docker pause`, then resumed at
+50 s. At +75 s pod `kv-0` is force-deleted. Every operation is recorded and
the full history is checked with Porcupine. A failed write counts as possibly
applied.

| label | ops | ok | failed | linearizable |
|---|---:|---:|---:|---:|
| zone | 18000 | 17849 | 151 | ok |

The failed requests are deadline expirations while groups elected new leaders.
The history, including the frozen leader resuming after other nodes had
replaced it, is linearizable.

Reproduce with `bash scripts/exp-zone.sh`.
