# Scale and chaos tests (M5)

The question these tests answer: under bursts and failures, does every alert that should exist come out,
exactly once, and how fast?

## Tools

- **`cmd/loadgen`** writes listing changes straight to `listing.changes`, as the pollers would.
  - `steady`: a set rate.
  - `burst`: as fast as it can.
  - `replay`: the recorded PropTx pages at 100–1000×.

  Synthetic listings are spread over Canada's large markets by population and go through realistic
  lifecycles: listed, price drops and increases, conditional, sold or leased with a close price, expired,
  back on the market. About 1 in 10 changes is timestamp-only, which must produce nothing. Each listing
  appears on VOW and, for 80%, on IDX too; IDX drops closed listings. Every change produced is written to
  a log file.
- **`cmd/audit`** replays that log through the same pure functions the services run (`detect.Apply`,
  then the search index) to derive every listing event and alert that must exist. Event and alert IDs are
  deterministic, so this comparison is exact, not statistical. It then reads what the system actually
  wrote to `listing.events`, `alerts.pending` and the `deliveries` table, and reports missing, unexpected
  and duplicate items, plus latency from the timestamps carried in the messages.
- **`deploy/scale`** is the test stack:
  - a 3-broker Redpanda cluster with every topic replicated 3 ways (12 partitions);
  - 3 detectors, 3 matcher shards, delivery, the API, Postgres, Redis, Prometheus and Grafana.

  Delivery uses a `discard` channel there: claims, rate limits and bookkeeping all run, but nothing is
  sent, so the test measures this system rather than a mail server. Email end to end is covered by the
  Mailpit tests (M4).
- **`scripts/chaos.sh`** runs steady load while killing components with SIGKILL, so there is no graceful
  shutdown, and restarting them.

Everything runs on one laptop: an Apple M-series with 10 cores, and Docker Desktop with an 8 GB VM shared
by every container.

## Results

| Run | Changes | Events | Alerts | Missing / unexpected / duplicates | Result |
| --- | --- | --- | --- | --- | --- |
| steady 500/s, 30 s, 10K searches | 14,995 | 14,372 | 157,671 | 0 / 0 / 0 | PASS |
| burst 250K, 2K searches | 250,000 | 192,015 | 426,836 | 0 / 0 / 0 | PASS |
| chaos: 1,500/s for 150 s with kills (below) | 224,985 | 156,370 | 364,465 | 0 / 0 / 0 | PASS |

**Burst drain.** The generator wrote 250,000 changes in 4.2 s (60K/s). The last listing event came out
8.6 s after the first change, which meets the target of under 10 s for a Canada-wide re-send:

- **Detection:** about 29K changes/s end to end across 3 detectors.
- **Matching:** about 49K alerts/s across 3 shards.
- **Queueing:** while the burst drains, change → alert latency rises to p50 3.1 s, p99 4.6 s.

**Steady latency** (500 changes/s): change → listing event p50 3 ms, p99 83 ms; change → alert p50 6 ms,
p99 103 ms. Real Ontario volume averages about 0.1 changes/s, so production runs far below this.

Every expected alert reached the delivery queue. Most were held by the per-user rate limit (5 emails, then 1
per 10 minutes), which is what it is for. A small share were dropped on purpose because they repeat earlier
news for the same search: same listing, type and price.

**Chaos.** Under a steady 1,500 changes/s, each of these was killed with SIGKILL (no graceful shutdown) and
restarted:

- 1 of 3 detectors, down 15 s;
- matcher shard 1 of 3, down 20 s;
- Redpanda broker 1 of 3, down 25 s;
- the delivery service, down 15 s.

Every expected event and alert still came out exactly once, and every alert reached the delivery queue. No
partition was left under-replicated.

- **Median latency was unaffected:** change → event 8 ms, change → alert 18 ms.
- **The tail shows the outages:**
  - p99 change → event was 42 s: a killed consumer's partitions wait for its session to time out, then
    rebalance to the other detectors.
  - p99 change → alert was 1.5 min: the dead shard's part of the map waits until it restarts, reloads its
    searches and catches up.

  These are delays, not losses.
- **Shortening the delays:** lower the consumer session timeout so dead members are noticed sooner, and run
  2 replicas per shard in an active/standby pair. Both are standard follow-ups.

**API (k6).** About 960 requests/s for 60 s: creates at 300/s (a fifth of them deleted again) and lists at
600/s. p99 was 8 ms for creates and 5.7 ms for lists, with 0 errors and every check passing.

## Not done here: the 5-node Kubernetes cluster

The plan included a 5-node kind (Kubernetes-in-Docker) cluster. That doesn't fit: Docker Desktop's VM on this
Mac is 8 GB, and the Compose scale stack already uses most of it. The scale and chaos results above come from
the same topology in Compose: 3 brokers with replication factor 3, 3 detectors, 3 shards. Kubernetes would
add scheduling and restarts, not a different failure model.

## What a full disk taught us

The first burst ran against 10K saved searches. That is about 2.6M alerts, each about 2 KB of JSON, kept on 3
brokers and in Postgres. Docker's disk image grew until the Mac had 1 GB free, and Docker Desktop crashed.
The scale script now refuses to start a run with less than 15 GB free, and a watchdog stops the stack if free
space drops below 6 GB during a run. Bursts use 2K searches.

The production lesson is the same one. Alert fan-out (events × matching searches), not event volume, is what
sizes the storage. Production topics need retention by size as well as time, and the delivery table needs
pruning of sent rows.
