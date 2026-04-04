#!/usr/bin/env bash
set -euo pipefail

CANTON_HOST="${CANTON_HOST:-localhost}"
CANTON_PORT="${CANTON_PORT:-6865}"
DAR=".daml/dist/canton-compliance-sdk-0.1.0.dar"

echo "Canton Compliance SDK — Rule Deployer"
echo "======================================"

[[ -f "$DAR" ]] || { echo "DAR not found. Run: daml build"; exit 1; }

echo "Uploading DAR to $CANTON_HOST:$CANTON_PORT..."
daml ledger upload-dar --host "$CANTON_HOST" --port "$CANTON_PORT" "$DAR"

echo "Running compliance gate setup script..."
daml script --dar "$DAR" \
  --script-name ComplianceTest:complianceTests \
  --ledger-host "$CANTON_HOST" --ledger-port "$CANTON_PORT"

echo ""
echo "✅ Compliance rules deployed successfully."
