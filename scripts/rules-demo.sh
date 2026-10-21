#!/usr/bin/env bash
# The life of an analyst rule, end to end against the running stack:
#   analyst drafts -> backtests on the lakehouse -> submits -> a second person approves -> Flink runs it.
# Uses the local demo identities from docker-compose.yml.
set -euo pipefail
API=${RULES_API:-http://localhost:8090}
ANALYST="Authorization: Bearer dev-analyst"
APPROVER="Authorization: Bearer dev-approver"
ADMIN="Authorization: Bearer dev-admin"
j() { python3 -m json.tool; }

echo "== 1. Analyst drafts a rule: many small P2P transfers into one account from new senders"
DRAFT=$(curl -sf -X POST "$API/rules" -H "$ANALYST" -H 'Content-Type: application/json' -d '{
  "rule_id": "p2p-fan-in-small", "name": "Many small transfers into one account", "state": "SHADOW",
  "group_by": "receiver_id",
  "when": [{"field": "txn_type", "op": "==", "value": "P2P"},
           {"field": "receiver_id", "op": "starts_with", "value": "U"},
           {"field": "status", "op": "==", "value": "SUCCESS"}],
  "aggregate": {"fn": "COUNT_DISTINCT", "field": "sender_id"}, "window_s": 900,
  "threshold": {"op": ">=", "value": 10}, "cooldown_s": 1800, "action": "REVIEW"}')
VERSION=$(echo "$DRAFT" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])')
echo "   drafted v$VERSION"

echo "== 2. Backtest it on the last 24 hours of the lakehouse"
curl -sf -X POST "$API/rules/p2p-fan-in-small/versions/$VERSION/backtest" -H "$ANALYST" \
  -H 'Content-Type: application/json' -d '{"days": 1}' | python3 -c '
import json, sys
r = json.load(sys.stdin)
print("  ", json.dumps(r["summary"]))
print("   caught:", {k: v for k, v in r["caught_by_scenario"].items() if v["caught"]})'

echo "== 3. Analyst submits; the analyst cannot approve their own rule"
curl -sf -X POST "$API/rules/p2p-fan-in-small/versions/$VERSION/submit" -H "$ANALYST" >/dev/null
curl -s -o /dev/null -w "   self-approval -> HTTP %{http_code}\n" -X POST \
  "$API/rules/p2p-fan-in-small/versions/$VERSION/approve" -H "$ANALYST" -H 'Content-Type: application/json' -d '{}'

echo "== 4. Approver publishes it (in shadow: it alerts, flagged, without acting)"
curl -sf -X POST "$API/rules/p2p-fan-in-small/versions/$VERSION/approve" -H "$APPROVER" \
  -H 'Content-Type: application/json' -d '{"comment": "backtest reviewed, trial in shadow"}' >/dev/null
curl -sf "$API/rules" -H "$ANALYST" | j

echo "== 5. Audit log integrity"
curl -sf "$API/audit/verify" -H "$ADMIN" | j

echo "Flink now evaluates p2p-fan-in-small on live traffic. Watch its alerts:"
echo "  make sql  ->  SELECT * FROM silver.fraud_alerts WHERE rule_id = 'p2p-fan-in-small' ORDER BY detected_at DESC;"
