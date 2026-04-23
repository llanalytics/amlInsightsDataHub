#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LANDING_DIR="${APP_ROOT}/data/landing"
SAMPLE_DIR="${APP_ROOT}/data/sample"

if [ -f "${APP_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ROOT}/.env"
  set +a
fi

mkdir -p "${LANDING_DIR}"

existing_count="$(find "${LANDING_DIR}" -maxdepth 1 -type f -name '*.csv' | wc -l | tr -d ' ')"
if [ "${existing_count}" -gt 0 ]; then
  echo "Landing already contains ${existing_count} CSV file(s): ${LANDING_DIR}" >&2
  echo "Clear landing first to run external feed cleanly." >&2
  exit 1
fi

echo "[1/3] Generating external transfer feed sample files..."
PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/generate_external_transfer_feed.py" "$@"

cp_src="${SAMPLE_DIR}/dh_dim_counterparty_account_external_sample.csv"
cash_src="${SAMPLE_DIR}/dh_fact_cash_external_sample.csv"
if [ ! -f "${cp_src}" ] || [ ! -f "${cash_src}" ]; then
  echo "Expected generated files not found:" >&2
  echo "  ${cp_src}" >&2
  echo "  ${cash_src}" >&2
  exit 1
fi

echo "[2/3] Staging generated files to landing..."
timestamp="$(date +%Y%m%d_%H%M%S)"
cp_target="${LANDING_DIR}/dh_dim_counterparty_account_external_${timestamp}.csv"
cash_target="${LANDING_DIR}/dh_fact_cash_external_${timestamp}.csv"
cp -f "${cp_src}" "${cp_target}"
cp -f "${cash_src}" "${cash_target}"
echo "  staged: ${cp_target}"
echo "  staged: ${cash_target}"
echo "  note: pipeline table order guarantees dh_dim_counterparty_account loads before dh_fact_cash"

echo "[3/3] Running pipeline..."
"${SCRIPT_DIR}/run_job.sh"

