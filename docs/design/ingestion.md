# Ingestion (M1)

Pollers turn listing feeds into `listing.changes` events. They are deliberately simple: they report what
each source shows, and the change detector (M2) works out what changed.

## Sources

Measured against the live PropTx feeds on 2026-11-17:

| | VOW | IDX |
| --- | --- | --- |
| Active listings (all types) | 108,217 | 92,524 |
| Changes in 24 h (Ontario) | ~8,100 | ~5,400 |
| Non-active history kept | 1.1 M records | 4,301 |
| `ClosePrice` on sold / leased | yes | never |
| Coordinates | none | none |

That shapes the design:

- **VOW is the source of truth for the lifecycle.** Only VOW keeps closed listings and their sold or leased
  prices. Those prices may only reach registered, logged-in users, so every event says which source it
  came from and delivery (M4) enforces it.
- **IDX decides what may be shown publicly.** VOW marks ~107K active listings as displayable but IDX has
  ~92.5K: the difference is brokerages that opted out of IDX, and no VOW field says which. So IDX is
  polled too, and IDX membership is its own signal. Both pollers key events by `ListingKey`, so every
  observation of a listing, from either feed, lands on one partition in order.
- **IDX drops listings instead of closing them.** A sold home just disappears. The IDX poller reconciles
  hourly: it lists every key in the feed and emits `removed` for the ones that are gone.
- **Off-MLS rentals** (hotrentals.ca's own and landlord-submitted listings) are JSON files. That poller
  diffs a full snapshot every 30 s.
- **No coordinates in either feed.** Geohash sharding (M3) needs a geocoding step; hotrentals.ca already
  has an Ontario address-points database to reuse.

Real volume averages about 0.1 events per second for Ontario. Throughput only matters for bursts (see
[PLAN.md](../PLAN.md)).

## The cursor

The position is `(ModificationTimestamp, ListingKey)` of the last record read, and the next page is the
exact keyset:

```
ModificationTimestamp gt T or (ModificationTimestamp eq T and ListingKey gt 'K')
$orderby=ModificationTimestamp,ListingKey
```

A timestamp-only cursor either skips or re-reads records that share a timestamp, and MLS systems update
hundreds of listings in the same second. A keyset never does, whatever the page size. (The reso-idx plugin
gets the same result by filtering client-side; PropTx turned out to accept the compound filter, which is
simpler.)

**First run.** With no cursor, the poller pages through the current active and under-contract listings by
`ListingKey` (the baseline) and marks those events `backfill`, so the pipeline records state without
alerting anyone. Then it follows changes from 5 minutes before the baseline started, so nothing that changed
while the baseline ran is missed. On the live feed the baseline took about 3 minutes for 89K VOW listings.

**Late records.** A record can become visible after the cursor has passed its timestamp (replication lag
on the MLS side). When caught up, each poll re-reads the last 3 minutes. Because of dedup (below), the
re-read costs one small request and produces no events unless something was really missed.

## Dedup and idempotency

Three layers:

1. **Content hash.** Each listing is hashed over its alert-relevant fields, excluding
   `ModificationTimestamp` and `PhotosChangeTimestamp`. Feeds bump timestamps for many reasons. A record
   whose hash matches the stored version produces no event. On the live feed about two thirds of
   incremental records are dropped this way.
2. **Staleness.** A record older than the stored version of that listing is dropped, so re-reads never
   move a listing backwards.
3. **Deterministic event IDs.** `id = sha256(kind, source, key, content_hash, modified_at)`. The same
   observation always gets the same ID, and including `modified_at` keeps a listing that changes
   A → B → A from colliding with itself.

## Publish, then commit

```
fetch page → diff against stored versions → publish (acks=all, idempotent producer) → commit cursor + versions (one Postgres transaction)
```

- Publish fails → nothing is committed; the next step re-reads the same page.
- Publish succeeds but commit fails, or the process dies → the next step re-reads the same page and
  re-publishes the same events **with the same IDs**.

So delivery is at-least-once, and consumers dedupe by ID. Unit tests simulate both failures and check
that a deduping consumer sees each event exactly once.

**Why not a transactional outbox?** An outbox (write events to Postgres in the commit, relay them to
Kafka) gives the same guarantee with an extra hop and an extra moving part. Here the poller's input can be
replayed (it is just a cursor position), so "publish then commit" gets the same result without one.
Kafka transactions would also work, but only Kafka consumers would honour them, and the IDs are needed
anyway for end-to-end dedup through delivery.

**One writer per cursor.** Each source runs as a single replica. Polling is a few requests a minute, so
there is nothing to scale there, and a second writer would race on the cursor. Scale comes downstream
from the 12 partitions of `listing.changes`. If a poller dies, Docker or Kubernetes restarts it and it
resumes from the last committed cursor.

**Reconcile safety.** If IDX suddenly lists fewer than half the keys we hold, reconcile refuses to run
instead of "removing" everything. A missing off-MLS file is an error, not a mass removal. Reconcile runs
between poll steps, never alongside one, so a listing can't be observed and removed at the same time.

## Wire format

JSON with a version field (`"v": 1`). At a few events per second, readability in Redpanda Console beats
saving bytes. The batch is zstd-compressed, and Protobuf is a drop-in change behind `event.Encode` if
profiling at benchmark rates (M5) says otherwise. Headers carry `id`, `kind`, `source` and `observed_at_ms`,
so consumers can dedupe and measure latency without decoding the body.

## Recording for replay

With `RECORD_DIR` set, every raw page is appended to `data/recordings/<source>/<date>.jsonl.gz`, one gzip
member per page so a crash never truncates a file. This is the real-data input for the replay benchmark in
M5. Recordings are MLS data: they stay on the machine that made them (`/data` is gitignored).
