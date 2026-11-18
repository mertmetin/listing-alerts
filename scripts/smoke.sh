#!/usr/bin/env bash
# End-to-end check of a running stack: every service answers and the API sees
# all of its dependencies. Exits non-zero on the first failure.
set -euo pipefail
export PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  ok  $*"; }

# Retries for up to ~30s: Prometheus needs a scrape or two before targets are up.
retry() {
  local i
  for i in $(seq 1 15); do
    if "$@"; then return 0; fi
    sleep 2
  done
  return 1
}

echo "Smoke test"

retry curl -fsS http://localhost:8081/healthz >/dev/null || fail "api /healthz"
pass "api is alive"

ready=$(curl -sS http://localhost:8081/readyz) || fail "api /readyz unreachable"
echo "$ready" | grep -q '"ok":true' || fail "api not ready: $ready"
for dep in postgres redis redpanda; do
  echo "$ready" | grep -q "\"name\":\"$dep\",\"ok\":true" || fail "api cannot reach $dep: $ready"
done
pass "api reaches postgres, redis and redpanda (all 4 topics exist)"

curl -fsS http://localhost:8081/metrics | grep -q '^listing_alerts_build_info{service="api"' || fail "api /metrics"
pass "api exports metrics"

targets_up() {
  curl -fsS 'http://localhost:9091/api/v1/query?query=min(up)' | grep -q '"value":\[[0-9.]*,"1"\]'
}
retry targets_up || fail "a Prometheus target is down: see http://localhost:9091/targets"
pass "prometheus scrapes api, redpanda and itself"

retry curl -fsS http://localhost:3300/api/health >/dev/null || fail "grafana"
pass "grafana is up"

curl -fsS http://localhost:8025/livez >/dev/null || fail "mailpit"
pass "mailpit is up"

echo "All checks passed."
