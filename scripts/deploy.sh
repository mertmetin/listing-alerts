#!/usr/bin/env bash
# Deploys to the production VPS. Images are cross-compiled here (Go builds for
# x86 natively, no emulation), shipped over SSH, and only run on the server.
#
#   scripts/deploy.sh              build, ship, restart
#   scripts/deploy.sh status       container status and resources
#   scripts/deploy.sh logs [svc]   recent logs
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"
HOST=${LISTING_ALERTS_HOST:-root@50.6.243.138}
KEY=${LISTING_ALERTS_KEY:-$HOME/.ssh/listing_alerts}
SSH=(ssh -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20 "$HOST")
DC="docker compose -f deploy/prod/compose.yaml"
SERVICES=(poller detector matcher api delivery)

case "${1:-deploy}" in
  deploy)
    if [ -n "$(git status --porcelain)" ]; then echo "commit first: the server runs what is on GitHub" >&2; exit 1; fi
    git push -q origin main
    version=$(git rev-parse --short HEAD)
    echo "building $version for linux/amd64"
    for s in "${SERVICES[@]}"; do
      docker buildx build -q --platform linux/amd64 --build-arg SERVICE="$s" --build-arg VERSION="$version" \
        -t "listing-alerts/$s:prod" --load . >/dev/null
    done
    echo "shipping images"
    docker save $(printf 'listing-alerts/%s:prod ' "${SERVICES[@]}") | gzip -1 | "${SSH[@]}" 'gunzip | docker load -q'
    "${SSH[@]}" "set -e
      cd /opt/listing-alerts
      git fetch -q origin main && git reset -q --hard origin/main
      $DC up -d --no-build --remove-orphans 2>&1 | grep -vE 'Waiting|Running' || true
      docker image prune -f >/dev/null
      $DC ps --format '{{.Service}} {{.Status}}'"
    ;;
  status) "${SSH[@]}" "cd /opt/listing-alerts && $DC ps --format '{{.Service}} {{.Status}}'; free -h | head -3; df -h / | tail -1; uptime" ;;
  logs) "${SSH[@]}" "cd /opt/listing-alerts && $DC logs --tail 50 ${2:-}" ;;
  *) echo "usage: $0 [deploy|status|logs [service]]"; exit 2 ;;
esac
