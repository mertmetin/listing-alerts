#!/usr/bin/env bash
# Deploys main to the production VPS: pull from GitHub, build, restart.
#
#   scripts/deploy.sh              deploy
#   scripts/deploy.sh status       container status
#   scripts/deploy.sh logs [svc]   recent logs
set -euo pipefail
HOST=${LISTING_ALERTS_HOST:-root@50.6.243.138}
KEY=${LISTING_ALERTS_KEY:-$HOME/.ssh/listing_alerts}
SSH=(ssh -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes "$HOST")
DC="docker compose -f deploy/prod/compose.yaml"

case "${1:-deploy}" in
  deploy)
    "${SSH[@]}" "set -e
      cd /opt/listing-alerts
      git fetch -q origin main && git reset -q --hard origin/main
      echo deploying \$(git log --oneline -1)
      $DC up -d --build --remove-orphans 2>&1 | grep -vE 'Pulling|Pulled|Waiting|Building|^ *#' || true
      $DC ps --format '{{.Service}} {{.Status}}'"
    ;;
  status) "${SSH[@]}" "cd /opt/listing-alerts && $DC ps --format '{{.Service}} {{.Status}}'; free -h | head -2; df -h / | tail -1" ;;
  logs) "${SSH[@]}" "cd /opt/listing-alerts && $DC logs --tail 50 ${2:-}" ;;
  *) echo "usage: $0 [deploy|status|logs [service]]"; exit 2 ;;
esac
