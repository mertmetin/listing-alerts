#!/usr/bin/env bash
# Helpers for the scale stack (deploy/scale). Usage: scripts/scale.sh <cmd> [args]
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"
set -a; . ./.env; set +a
export POSTGRES_URL="postgres://alerts:${POSTGRES_PASSWORD}@localhost:15433/alerts?sslmode=disable"
BROKERS=localhost:29092,localhost:29093,localhost:29094
DC="docker compose -f deploy/scale/compose.yaml --env-file .env"

# Load tests write a lot (every alert is stored on 3 brokers and in
# Postgres). A burst once filled the Mac's disk and crashed Docker, so:
# refuse to start under 15 GB free, and stop the stack if it drops under 6 GB.
free_gb() { df -g /System/Volumes/Data 2>/dev/null | awk 'NR==2{print $4}' || df -BG / | awk 'NR==2{print int($4)}'; }
guard() {
  local f; f=$(free_gb)
  if [ "${f:-0}" -lt 15 ]; then echo "only ${f} GB free on disk; need 15 GB to run a load test" >&2; exit 1; fi
}
watchdog() {
  while sleep 5; do
    if [ "$(free_gb)" -lt 6 ]; then
      echo "DISK WATCHDOG: under 6 GB free, stopping the scale stack" >&2
      $DC stop >/dev/null 2>&1
      kill 0
    fi
  done
}

case "${1:-}" in
  up)
    guard
    # Both stacks don't fit in Docker's 8 GB; the dev stack keeps its data.
    docker compose --profile live stop >/dev/null 2>&1 || true
    $DC up -d --build --wait
    $DC exec -T redpanda-0 rpk cluster health | head -3
    ;;
  down) $DC down -v ;;
  load) shift; guard; watchdog & go run ./cmd/loadgen -brokers "$BROKERS" -pg "$POSTGRES_URL" "$@"; kill %1 2>/dev/null ;;
  audit) shift; watchdog & go run ./cmd/audit -brokers "$BROKERS" -pg "$POSTGRES_URL" "$@"; kill %1 2>/dev/null ;;
  apikey) shift; go run ./cmd/apikey -site "${1:-k6}" -label "scale test" ;;
  k6)
    shift
    KEY=$(go run ./cmd/apikey -site k6 -label "k6 run")
    k6 run -q -e BASE=http://localhost:18081 -e KEY="$KEY" "$@" deploy/k6/api.js ;;
  dc) shift; $DC "$@" ;;
  *) echo "usage: $0 up|down|load|audit|apikey|k6|dc ..."; exit 2 ;;
esac
