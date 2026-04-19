#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[1/4] Updating country sample from ISO..."
"${SCRIPT_DIR}/update_country_sample.sh"

echo "[2/4] Updating currency sample from ISO..."
"${SCRIPT_DIR}/update_currency_sample.sh"

echo "[3/4] Updating OFAC SDN sample..."
"${SCRIPT_DIR}/update_ofac_sdn_sample.sh"

echo "[4/4] Updating Panama Papers samples..."
"${SCRIPT_DIR}/update_panama_papers_samples.sh"

echo "Done. Reference sample files refreshed."
