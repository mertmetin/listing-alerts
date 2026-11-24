#!/usr/bin/env bash
# Deploys main to the production VPS: the server pulls from GitHub and builds
# one image (all services, one memory-capped compile), then restarts. Nothing
# large crosses the home connection.
#
#   scripts/deploy.sh              pull, build, restart
#   scripts/deploy.sh status       container status and resources
#   scripts/deploy.sh logs [svc]   recent logs
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"
HOST=${LISTING_ALERTS_HOST:-root@50.6.243.138}
KEY=${LISTING_ALERTS_KEY:-$HOME/.ssh/listing_alerts}
SSH=(ssh -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20 "$HOST")
DC="docker compose -f deploy/prod/compose.yaml"

case "${1:-deploy}" in
  deploy)
    if [ -n "$(git status --porcelain)" ]; then echo "commit first: the server runs what is on GitHub" >&2; exit 1; fi
    git push -q origin main
    "${SSH[@]}" "set -e
      cd /opt/listing-alerts
      git fetch -q origin main && git reset -q --hard origin/main
      v=\$(git rev-parse --short HEAD)
      echo building \$v
      docker build -q --build-arg VERSION=\$v -t listing-alerts/app:prod . >/dev/null
      $DC up -d --no-build --remove-orphans 2>&1 | grep -vE 'Waiting|Running' || true
      docker image prune -f >/dev/null
      $DC ps --format '{{.Service}} {{.Status}}'"
    ;;
  status) "${SSH[@]}" "cd /opt/listing-alerts && $DC ps --format '{{.Service}} {{.Status}}'; free -h | head -3; df -h / | tail -1; uptime" ;;
  logs) "${SSH[@]}" "cd /opt/listing-alerts && $DC logs --tail 50 ${2:-}" ;;
  *) echo "usage: $0 [deploy|status|logs [service]]"; exit 2 ;;
esac
