# listing-alerts

A real-time listing alert engine in Go. It ingests MLS® listing changes from RESO Web API feeds, works out
what changed, matches each change against saved searches, and sends alerts within a second: new listings,
price drops, sold or leased (with the price where the data allows it) and back on the market.

It is built for two production sites, [mertmetin.ca](https://mertmetin.ca) (homes for sale) and
[hotrentals.ca](https://hotrentals.ca) (Ontario rentals), and load-tested at Canada-wide volume.

> **Status: milestone 0 of 6.** The platform runs: Redpanda, Postgres, Redis, Prometheus, Grafana and Mailpit,
> plus an API service that checks all of them. The pipeline comes next. See [docs/PLAN.md](docs/PLAN.md).

## Architecture (target)

```mermaid
flowchart LR
    subgraph Sources
        V[PropTx VOW<br/>for sale + sold]
        I[PropTx IDX<br/>for lease]
        O[Off-MLS rentals]
    end
    V & I & O --> P[Pollers<br/>keyset cursor]
    P -- listing.changes<br/>keyed by ListingKey --> D[Change detector<br/>stateless workers]
    D -- listing.events --> M[Matcher shards<br/>geohash ring]
    M -- alerts.pending --> A[Delivery<br/>retry, rate limit]
    A -- failures --> DLQ[(alerts.dlq)]
    A --> E[Email / SMS / push]
    API[Saved-search API] --> PG[(Postgres)]
    PG -. searches .-> M
    S1[mertmetin.ca] & S2[hotrentals.ca] --> API
```

## Run it

You need Docker and Go 1.27+.

```bash
make up      # creates .env with random local passwords, builds, starts, runs the smoke test
make test    # unit tests with the race detector
make down    # stop (make clean also deletes the data)
```

| Service | URL |
| --- | --- |
| API readiness | http://localhost:8081/readyz |
| Grafana | http://localhost:3300 (user `admin`, password in `.env`) |
| Prometheus | http://localhost:9091 |
| Redpanda Console | http://localhost:8180 |
| Mailpit (caught emails) | http://localhost:8025 |

Run `make hooks` once to block commits that contain secrets.

## License

MIT
