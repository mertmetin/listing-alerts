# Change detection (M2)

The detector reads `listing.changes` (what each source shows now) and writes `listing.events` (what
happened, as an alert would describe it).

## Events

| Type | Rule | Visibility |
| --- | --- | --- |
| `new_listing` | first time the listing is active | public if IDX or off-MLS shows it, otherwise VOW users only |
| `price_drop` | active → active, asking price lower, same transaction type | as above |
| `sold` / `leased` | active or conditional → closed (VOW); carries the close price | public if it was public at any point since it came on the market |
| `back_on_market` | off market, conditional or closed → active | as above |
| `pending` | active → sold/leased conditional | as above |
| `off_market` | active or conditional → expired, terminated, suspended, or gone from every feed | as above |
| `now_public` | already active, newly shown by IDX | public |

Rules that come from the data:

- **Only transitions alert.** A closed record for a listing we never saw on the market is history,
  not a sale: no event. The first-run baseline is applied silently.
- **VOW and IDX merge per listing.** The newest observation wins, with VOW first on ties because it
  carries close prices. IDX's job is visibility: when IDX drops a listing that VOW still has, only
  `public` changes. When every source has dropped it, the listing is off the market.
- **Sold is public if it was ever public in that market period.** IDX usually drops a sold home before
  VOW reports the sale (they are polled independently), so "is it in IDX right now" would hide most sales
  from public users. The state keeps `public_seen` and resets it when the listing comes back on the market.
- **`now_public` exists because VOW often sees a listing first.** A VOW-only `new_listing` is not
  allowed on public sites, so public saved searches treat `now_public` as their "new listing" a
  minute later.
- **Close prices are VOW data.** Events carry them; delivery (M4) removes them for anyone who is not a
  registered, logged-in VOW user.

## Rules are a pure function

`Apply(state, change) → (state, events, outcome)` has no I/O. Tests cover the lifecycle table above,
VOW and IDX arriving in either order, replays, and out-of-order observations. A property test runs a
random 300-listing stream through 1, 2, 6 and 12 simulated workers and checks they produce the same
set of event IDs. That holds because `listing.changes` is keyed by `ListingKey`: one listing's changes
always reach one worker, in order.

## Idempotency without an ID table

Every source version is remembered per listing: `(content_hash, modified_at, present, removed_at)`. A
change that is not newer than what the state already holds for its source is a `duplicate` or `stale`
no-op. So redeliveries, consumer restarts and full replays of the topic produce nothing, and there is
no ever-growing "processed IDs" table to keep.

`removed_at` is there because of a bug the replay test found. Replaying an old IDX observation after
IDX had removed the listing brought it back. Now an observation made before the removal can't undo it.
A genuine reappearance gets its own change ID from the poller, so it is never confused with a replay.

Event IDs are `hash(change_id, type)`. Reprocessing a change yields the same IDs, so anything published
twice is dropped downstream.

## One batch

```
poll ≤500 records → load states (one query) → Apply in order → publish events (acks=all)
  → save states (one statement, optimistic version check) → commit offsets → allow rebalance
```

- **Publish, then save.** A crash in between re-publishes the same IDs. Saving first would lose
  events.
- **Optimistic concurrency.** Each state row has a `version`. Save writes only rows whose version is
  still the one loaded, all or nothing. If two members briefly own a partition (an expired session
  during a rebalance), one wins and the other recomputes its batch from fresh state. The changes the
  winner already applied then come out as duplicates.
- **Rebalances wait for the batch** (`BlockRebalanceOnPoll`), so in the normal case a partition moves
  only between batches. The version check covers the abnormal case. A bug found here: returning without
  `AllowRebalance` made `LeaveGroup` hang on shutdown. The integration test now catches it.

## Scaling

Detectors are stateless (state in Postgres, offsets in Redpanda) and run as one consumer group. Compose
runs two; `docker compose up -d --scale detector=N` goes up to 12, the partition count. The integration
test runs two members on real Redpanda and Postgres, stops one mid-stream, and checks that all 900
expected events (300 each of new, price drop, sold) still come out.

**Where it will bottleneck.** Each batch costs one Postgres read and one write. That is plenty for real
volume, and it is the first thing M5 will measure at burst rates. The planned fallback is to keep state
in each worker's memory per assigned partition, rebuilt from a compacted state topic, which is the
Kafka Streams model. That trades simplicity for throughput, so it waits for numbers that justify it.

## Measured on the live feed (2026-11-17)

- First run: the detectors consumed the ~170K baseline and early live changes and built state for 89,831
  listings (85,269 active, 4,141 conditional).
- About 40 minutes of real changes produced 39 events, all unique: 7 price drops, 11 sold, 4 leased,
  7 new, 2 back on market, 3 conditional, 3 off market, 2 newly public. Example: a St. Catharines semi
  listed at $325,000 sold for $305,000.
- Latency on real-time events (15 events, evening traffic): **pipeline** (poller has the record → event
  published) p50 0.65 s; **end to end** (MLS modification timestamp → event published) p50 96 s, max
  119 s. The end-to-end figure is dominated by the 60 s poll interval and by how long PropTx takes to make
  a change visible. The pipeline's 5 s tail came from stamping "observed" before the poller's PropTx
  requests rather than after. That is fixed, and M5 measures the pipeline properly under load.

