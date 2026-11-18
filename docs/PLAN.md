# Plan

A real-time listing alert engine shared by mertmetin.ca (homes for sale, IDX + VOW) and hotrentals.ca
(Ontario rentals, IDX + off-MLS). About six weeks of evenings, one working milestone at a time.

## Alerts

| Event | Trigger | Notes |
| --- | --- | --- |
| New listing | First time a listing matches a saved search | |
| Price drop | `ListPrice` lower than the last version we saw | |
| Sold / leased | Status moves to sold or leased | Sold or leased price only from the VOW feed, and only to registered, logged-in users |
| Back on market | Terminated, expired or a deal that fell through comes back as active | |

Off-MLS rentals (hotrentals private and landlord-submitted listings) are a source too.

## Targets

Real Canada-wide volume is roughly one change per second on average, so the targets are about bursts
(an MLS re-sending every listing, a poller catching up after an outage) and about matching cost:

1. **Burst drain:** a re-send of every Canadian listing (~250K events) is absorbed in under 10 s,
   with no alerts for listings that did not really change.
2. **Latency:** p99 from listing change to alert sent under 1 s at realistic load, and during the burst.
3. **Matching:** under 1 ms per event against 1M saved searches.
4. **Replay:** a year of recorded changes replays in minutes and produces the same alerts as live.

The README reports measured sustained throughput and what limited it.

## Milestones

| # | Milestone | Done when |
| --- | --- | --- |
| M0 | Repo skeleton: Compose stack (Redpanda, Postgres, Redis, Prometheus, Grafana, Mailpit), Makefile, CI, secret scanning | `make up` starts everything and the smoke test passes |
| M1 | Ingestion: Go poller with a keyset cursor on `(ModificationTimestamp, ListingKey)` for VOW sale, IDX lease and off-MLS sources; snapshots in Postgres; idempotent producer keyed by ListingKey; local recorder for replay | Tests cover cursor edge cases, dedup and re-delivery |
| M2 | Change detector: stateless consumer group emitting NEW, PRICE_DROP, SOLD/LEASED, BACK_ON_MARKET with version checks | Same output with 1 or N workers; replays are no-ops |
| M3 | Saved-search API + matcher: per-site API keys; geohash-cell index with range filters; consistent-hash ring of cells across matcher shards | Benchmark of 1M searches with p99 match time |
| M4 | Delivery: outbox keyed by (search, event), retries with backoff, dead-letter topic, per-user rate limit with digests, pluggable channels (Gmail email first) | Exactly one alert per (search, event) under retries |
| M5 | Scale proof: synthetic Canada-wide generator, 100-1000x replay, k6, 5-node kind cluster, latency dashboards, chaos runs with a lost-alert auditor | `make bench` and `make chaos` print reports |
| M6 | Production: one small VPS (Hetzner or OVH Canada) on Compose, shadow mode next to the current alerts, then cut over; "Save this search" in reso-idx and mertmetin.ca; hotrentals hookup; README write-up | Real alerts come from the new engine |

## Rules

- No secrets in git: `.env` only, gitleaks in a pre-commit hook and in CI.
- Recorded MLS data stays on the local machine (`/data`, `/recordings` are gitignored).
- Every milestone has tests and runs with one command.
