#!/usr/bin/env bash
# Nightly Postgres backup (systemd timer listing-alerts-backup). Keeps 14
# days of compressed custom-format dumps in /var/backups/listing-alerts.
#
# Restore: docker compose -f deploy/prod/compose.yaml exec -T postgres \
#            pg_restore -U alerts -d alerts --clean < alerts-YYYY-MM-DD.dump
set -euo pipefail
DIR=/var/backups/listing-alerts
mkdir -p "$DIR"
chmod 700 "$DIR"
out="$DIR/alerts-$(date +%F).dump"
cd /opt/listing-alerts
docker compose -f deploy/prod/compose.yaml exec -T postgres pg_dump -U alerts -Fc alerts >"$out.tmp"
mv "$out.tmp" "$out"
find "$DIR" -name 'alerts-*.dump' -mtime +14 -delete
echo "backup: $out ($(du -h "$out" | cut -f1))"
