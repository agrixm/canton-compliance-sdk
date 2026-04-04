#!/usr/bin/env bash
set -euo pipefail

PARTY="${1:-}"
[[ -n "$PARTY" ]] || { echo "Usage: $0 <party-id>"; exit 1; }

BASE="http://${CANTON_HOST:-localhost}:${CANTON_PORT:-7575}"
TOKEN="${CANTON_JWT:-}"

echo "Checking compliance for: $PARTY"
curl -sf "$BASE/v1/compliance/check" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{"party": "$PARTY"}" | python3 -m json.tool
