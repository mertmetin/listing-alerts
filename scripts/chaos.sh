#!/usr/bin/env bash
# Chaos test on the scale stack: steady load while a detector, a matcher
# shard, a Redpanda broker and the delivery service are each killed with
# SIGKILL (no graceful shutdown) and restarted. Then the auditor checks that
# every expected event and alert still came out.
#
#   scripts/chaos.sh [rate] [seconds]      (default 1500 changes/s for 150 s)
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"
RATE=${1:-1500}
SECS=${2:-150}
RUN=chaos$(date +%H%M%S)
DC="docker compose -f deploy/scale/compose.yaml --env-file .env"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

# kill <seconds-into-run> <container> <down-seconds> <what>
kill_at() {
  sleep "$1"
  log "KILL  $4 ($2, SIGKILL)"
  docker kill -s KILL "$2" >/dev/null
  sleep "$3"
  log "START $4 ($2)"
  docker start "$2" >/dev/null
}

log "chaos run $RUN: ${RATE} changes/s for ${SECS}s, 2000 saved searches"
./scripts/scale.sh load -run "$RUN" -searches 2000 -mode steady -rate "$RATE" -duration "${SECS}s" >"data/load/$RUN.loadgen.txt" 2>&1 &
LOAD=$!
sleep 8 # searches seeded, load running

kill_at 20 la-scale-detector-1 15 "a detector" &
kill_at 45 la-scale-matcher-1-1 20 "matcher shard 1" &
kill_at 75 la-scale-redpanda-1-1 25 "Redpanda broker 1 of 3" &
kill_at 110 la-scale-delivery-1 15 "the delivery service" &

wait $LOAD
log "load finished: $(tail -2 "data/load/$RUN.loadgen.txt" | head -1)"
wait
$DC exec -T redpanda-0 rpk cluster health | grep -E "Healthy|Under-replicated" || true
log "auditing"
./scripts/scale.sh audit -run "$RUN" -log "data/load/$RUN.changes.gz" -wait 15m
