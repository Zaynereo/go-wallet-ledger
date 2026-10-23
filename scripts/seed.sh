#!/usr/bin/env bash
# Create the demo wallets (Alice, Bob, Carol) through the ledger.
# Requires MYSQL_DSN, including parseTime=true and loc=UTC.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -z "${MYSQL_DSN:-}" ]]; then
  echo "MYSQL_DSN is required" >&2
  exit 1
fi
exec go run ./cmd/ledger seed
