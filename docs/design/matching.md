# Geocoding and matching (M3)

The matcher answers one question per listing event: which saved searches want this? With 1M saved searches
that has to be a reverse index (searches indexed by where they look), not a loop over searches.

## Geocoding

Neither PropTx feed has coordinates. The detector places each listing using hotrentals.ca's database of
3.27M civic address points from 16 municipal open-data sources (Toronto, Ottawa, Peel, York, Durham, Hamilton,
Niagara, London, Waterloo Region, Halton, Barrie, Guelph, Brantford). Addresses never leave the machine.

1. Same house number and street name (types and directions dropped, so "St. Laurent Blvd" = "ST LAURENT"),
   in the sources for the listing's county, preferring the same postal area (FSA).
2. Otherwise the centre of the listing's FSA (141 FSAs have one).
3. Otherwise no coordinates: the event can still match searches picked by city.

The result is cached in the detector's listing state with the address it was computed from, so each address
is looked up once, and again only if it changes. Events say how they were placed (`geo_precision`:
`address`, `fsa` or none).

**Gap:** rural counties outside those sources get FSA centres only where a source has postal codes. Statistics
Canada's FSA boundary file would give every FSA in Canada a fallback centre; that is a follow-up.

## The index

A saved search is up to 10 drawn polygons and/or city names, plus filters (price range, beds, baths,
property types, which alert types).

**Coverings.** Each polygon is covered with geohash cells of mixed size, from precision 3 (about 156 km) to 6
(about 1.2 × 0.6 km): big cells where the polygon is solid inside, small ones along its edge. Cells fully
inside need no geometry test; edge cells are marked exact. Covering a 1.5 km neighbourhood takes about 25
cells, a 20 km city about 750.

**Lookup.** For a listing at point p, the index reads p's cell at each precision 3..6: four map reads. Each
polygon's cells don't overlap, so a point hits at most one cell per polygon. Each hit is checked against the
search's filters, plus a point-in-polygon test for exact cells. City-picked searches live in a second map,
looked up by every name the listing goes by. PropTx puts the district in `City` ("Toronto W08",
"Barrhaven") and the city in `County`, so the listing's city, the city without its district code, and its
county are all tried. That came from the first live alerts: a "Toronto, Ottawa, Hamilton" search matched
nothing until it was fixed.

**Layout for speed.** Profiling the first version at 1M searches showed the time was in scattered memory
reads, not computation. What changed:

- Cell entries are split by transaction, so a lease event never scans sale searches.
- Each candidate's filters live in one flat 24-byte record, its liveness flag included.
- The dedup check is skipped for searches with a single area, which can only be reached once.

Those changes cut p50 from 0.88 ms to 0.40 ms.

**Updates.** Changing or deleting a search tombstones its slot and appends a new one, and the index compacts
itself once tombstones exceed a quarter of the slots. Matching takes a read lock and updates take a write
lock. Search changes are far rarer than events.

**Correctness.** A test checks the index against a brute-force scan written the obvious way, over 10,000
synthetic searches and 2,000 events. Another checks that coverings contain every point of a polygon, concave
ones included.

## Audience rules, applied at match time

- `public` searches see only listings IDX may show. A `now_public` event is their "new listing", since VOW
  often sees a listing a minute before IDX allows it on public sites. They never get sold or leased prices,
  and never the street address of a listing whose brokerage withheld it.
- `vow` searches (registered, logged-in users) see every listing and get close prices.

These are applied when the alert is built, the earliest point where the audience is known, so VOW data
can't leak through a later stage by mistake.

## Sharding

Shards split the map with a **consistent-hash ring** (128 virtual nodes per shard) over precision-5 geohash
cells (about 4.9 × 4.9 km). Listings without coordinates hash by city.

- A search is held by every shard whose cells its covering touches. Cells bigger than a shard cell (huge
  drawn areas) and city lists are kept on every shard. That replication (about 3.5× in the benchmark) is the
  memory cost of letting each event go to exactly one shard.
- An event is matched only by the shard that owns its cell. Every shard reads all of `listing.events` in
  its own consumer group and skips what it doesn't own. At real volume that is far cheaper than a router
  stage. A router keyed by cell is the change to make if event volume ever makes the skipping cost show.
- **Why precision 5, not 4.** With 4-character cells (about 39 × 20 km) most of Toronto fell on one shard: 31%
  of events against 10% on the quietest. With 5-character cells the shards get 14–24%.
- Adding a shard moves about 1/N of the cells (tested: 6th shard, ~1/6 moved, none between old shards).

**What sharding does not do** is cut the latency of one event. A downtown Toronto listing still meets every
search drawn over downtown, on one shard. Sharding buys throughput and memory per node; the index layout
buys latency.

## Saved searches and how shards learn about them

The API writes searches to Postgres (one row per search, soft deletes). Shards load everything at startup
(coverings computed on all cores), then poll for rows changed since their last poll, every 2 seconds.

- **No dual write.** The API never writes to both Postgres and Redpanda, so there is no partial failure to
  reconcile. The cost is up to 2 s before a new search is live, which is fine for alerts. An outbox table or
  Postgres logical replication (CDC) would be the next step if that delay mattered.
- **Late commits.** A transaction can commit after a later one and carry an older `updated_at`, so each poll
  re-reads the last 10 s. A search is only ever moved forward, which a test caught: the re-read must not roll
  a search back to an older version.

Site API keys are stored as SHA-256 hashes. A site only ever sees its own searches, can't move a search to
another user, and each user can have at most 50.

## Measured

**Benchmark** (`make matchbench`, Apple M-series laptop, 10 cores, Go 1.27): 1M synthetic saved searches spread
over 20 Ontario cities by population (75% drawn neighbourhoods of 0.7–4 km, 15% wider areas of 5–25 km, 10%
picked by city; most with a budget and bedroom minimum), and 200K synthetic listing events.

| | p50 | p99 | p99.9 | events/s |
| --- | --- | --- | --- | --- |
| 1 index, 1 core | 0.37 ms | 1.98 ms | 2.42 ms | 1,869 |
| 1 index, 10 cores | | | | 14,730 |
| 5 shards (event to its owner) | 0.47 ms | 2.34 ms | 2.83 ms | 1,502 |

- Index build: 13.6 s of coverings (parallel) plus 9.5 s of inserts. 133M cell entries (133 per search), about
  720 MB of heap for 1M searches.
- On average an event matches 3,858 searches. **That is what sets p99**: the cost grows with matches, and a
  downtown Toronto listing in this data genuinely matches thousands of searches. The target was under 1 ms
  at p99. The median meets it and p99 is about 2×. The remaining options are sorting each cell's entries by
  price so a range check can skip a block of them, or coarser edge cells to trade memory for more polygon
  tests. Both wait for M5, where real end-to-end latency is measured. Matching is a small part of it next to
  the 60 s poll.
- Shards and the single index find exactly the same matches. Events split 14–24% across 5 shards.

**Live** (2026-11-17, five broad test searches over real PropTx changes): the first 9 alerts in 4.5 minutes,
all unique. Pipeline latency (poller has the record → alert published) was 4–24 ms. Most listings were placed
by civic address, a rural one by postal area. Public alerts carried no sold prices.
