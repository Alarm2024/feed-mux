#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8787}"

echo "==> GET /health"
health=$(curl -sf "${BASE_URL}/health")
echo "${health}" | python3 -m json.tool
echo "${health}" | grep -q '"status":"ok"'

echo "==> POST /publish (mock)"
publish=$(curl -sf -X POST "${BASE_URL}/publish" \
  -H 'Content-Type: application/json' \
  -d '{"event":"smoke.test","source":"scripts/smoke.sh","data":{"ok":true}}')
echo "${publish}" | python3 -m json.tool
echo "${publish}" | grep -q '"ok":true'

echo "==> smoke OK"
