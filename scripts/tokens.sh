#!/usr/bin/env bash
# Copies the PropTx tokens into .env without ever printing them.
#   macOS: Keychain services market-report-proptx (VOW) and idx-proptx (IDX)
#   server: ~/.config/market-report/tokens.json with the same keys
set -euo pipefail
cd "$(dirname "$0")/.."

get() {
  if [ "$(uname)" = Darwin ]; then
    security find-generic-password -a "$USER" -s "$1" -w 2>/dev/null || true
  else
    python3 -c 'import json,os,sys; print(json.load(open(os.path.expanduser("~/.config/market-report/tokens.json"))).get(sys.argv[1],""))' "$1" 2>/dev/null || true
  fi
}

set_var() { # name value
  local tmp
  tmp=$(mktemp)
  grep -v "^$1=" .env > "$tmp" || true
  printf '%s=%s\n' "$1" "$2" >> "$tmp"
  mv "$tmp" .env
  chmod 600 .env
}

for pair in PROPTX_VOW_TOKEN:market-report-proptx PROPTX_IDX_TOKEN:idx-proptx; do
  var=${pair%%:*} svc=${pair#*:}
  tok=$(get "$svc")
  dots=$(printf '%s' "$tok" | tr -cd . | wc -c | tr -d ' ')
  if [ -z "$tok" ] || [ "$dots" != 2 ]; then
    echo "  $var: no valid token in $svc (want a JWT with 2 dots); skipped" >&2
    continue
  fi
  set_var "$var" "$tok"
  echo "  $var: set from $svc (${#tok} chars)"
done
