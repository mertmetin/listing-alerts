#!/usr/bin/env python3
"""Pretty-prints listing events (JSON lines on stdin): `make events`."""
import json
import sys

def money(v):
    return f"${v:,.0f}" if isinstance(v, (int, float)) else ""

for line in sys.stdin:
    if not line.strip():
        continue
    e = json.loads(line)
    l = e.get("listing", {})
    print(f"{e['detected_at'][:19]}  {e['type']:15} {e['listing_key']:10} {l.get('city', '')[:18]:18} "
          f"{l.get('transaction_type', ''):9} {money(e.get('prev_price')):>11} {money(e.get('price')):>11} "
          f"{money(e.get('close_price')):>11}  {'public' if e['public'] else 'VOW only'}")
