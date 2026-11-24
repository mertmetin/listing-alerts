# Production (M6)

One small VPS: Bluehost NVMe 4 in Toronto, 2 vCPUs, 4 GB RAM, 100 GB NVMe, AlmaLinux 9, about CA$14/month.
Real volume is about one change every 10 seconds, so the box is sized by memory, not load.

## Layout

`deploy/prod/compose.yaml` runs the same services as the dev stack, one replica each:

| Service | Notes | Memory in use |
| --- | --- | --- |
| Redpanda | one broker, production mode (writes fsynced); topics keep 14 days with a 256 MB-per-partition cap | ~200 MB |
| Postgres | 128 MB shared buffers | ~270 MB |
| Redis | 64 MB cap | ~10 MB |
| poller-vow, poller-idx | PropTx tokens in `.env` | ~25 MB each |
| detector | geocodes from the municipal address points | ~25 MB |
| matcher | one shard (`SHARD_COUNT=1`) | ~5 MB |
| api | behind Caddy | ~3 MB |
| delivery | sends to Mailpit until Gmail is configured (shadow mode) | ~4 MB |
| Caddy | HTTPS for `alerts.mertmetin.ca`; only `/v1/*` and `/healthz` are public | — |

In total about 1.1 GB of 3.6 GB. Every container has a memory limit, so one service can't starve the rest.

**One broker is a deliberate trade-off.** Three brokers on one machine wouldn't survive that machine
failing, and every stage dedupes by ID. After any disaster, the pollers' cursors can be moved back and PropTx
re-read; detectors drop what they've already seen, so only genuinely missed alerts go out. Multi-broker
replication was proven in the chaos tests (M5) and is a deployment choice, not a code change.

## Server setup

`deploy/prod/harden.sh` is idempotent and was run once:

- SSH is key-only; root can log in with a key only.
- firewalld allows SSH, HTTP and HTTPS only (Cockpit closed).
- fail2ban watches SSH.
- Security updates install automatically.
- There is a 2 GB swap file.
- Docker starts at boot.

Secrets live in `/opt/listing-alerts/deploy/prod/.env` (mode 600):

- the PropTx tokens, copied from the Mac's Keychain over SSH and never printed;
- a random Postgres password and unsubscribe secret, generated for this server.

## Deploys

`scripts/deploy.sh` makes the server pull `main` from GitHub, build one image containing every service, and
restart. The build is a single compile capped at two parallel jobs and takes about a minute.

The first deploy instead ran `docker compose build`, which compiled seven Go images in parallel. That used all
4 GB plus swap, and the server stopped answering SSH until the compilers were killed. Images are also not
shipped from the Mac, because its home upload runs at about 20 KB/s.

## Backups

`listing-alerts-backup.timer` runs `deploy/prod/backup.sh` at 03:30 Toronto time. It writes a `pg_dump -Fc`
to `/var/backups/listing-alerts`, keeps 14 days, and makes the files readable by root only. The first dump
(39 MB) was checked to contain all 7 tables.

Still to do: an off-site copy, for example to object storage.

## First run (2026-11-24)

- The pollers loaded 88,549 VOW and 80,120 IDX residential listings, then switched to following changes.
- The detector holds state for 88,549 listings, with geocoding on.
- There were no errors in the logs.
- `https://alerts.mertmetin.ca` has a Let's Encrypt certificate. Unauthenticated API calls get 401 and
  internal endpoints get 404.
