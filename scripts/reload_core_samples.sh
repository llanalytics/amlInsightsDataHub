#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SAMPLE_DIR="${APP_ROOT}/data/sample"
LANDING_DIR="${APP_ROOT}/data/landing"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/reload_core_samples.sh [--dry-run] [--no-external-feed]
                                 [--external-start YYYY-MM]
                                 [--external-end YYYY-MM]
                                 [--external-min-per-month N]
                                 [--external-max-per-month N]
                                 [--external-seed N]

Behavior:
  1) Deletes records from core pipeline-loaded tables (excludes OFAC/Panama tables)
  2) Clears landing CSV files
  3) Copies core sample files to landing
  4) Generates/stages external transfer feed files (default behavior)
  5) Seeds unknown (NA) keys to core dimensions in the database
  6) Runs pipeline job

Core sample files included:
  dh_dim_household_sample.csv
  dh_dim_customer_sample.csv
  dh_dim_associated_party_sample.csv
  dh_dim_account_sample.csv
  dh_dim_branch_sample.csv
  dh_dim_country_sample.csv
  dh_dim_currency_sample.csv
  dh_dim_transaction_type_sample.csv
  dh_bridge_household_customer_sample.csv
  dh_bridge_customer_account_sample.csv
  dh_bridge_customer_associated_party_sample.csv
  dh_fact_cash_sample.csv

Excluded:
  dh_dim_ofac_sdn_sample.csv
  dh_dim_panama_node_sample.csv
  dh_bridge_panama_relationship_sample.csv

External feed (enabled by default; disable with --no-external-feed):
  - Generates dh_dim_counterparty_account_external_sample.csv
  - Generates dh_fact_cash_external_sample.csv
  - Stages both files to landing for the same pipeline run
EOF
}

DRY_RUN="false"
INCLUDE_EXTERNAL_FEED="true"
EXTERNAL_START="2025-01"
EXTERNAL_END="2026-01"
EXTERNAL_MIN_PER_MONTH="0"
EXTERNAL_MAX_PER_MONTH="8"
EXTERNAL_SEED="101"
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    --dry-run)
      DRY_RUN="true"
      ;;
    --include-external-feed)
      INCLUDE_EXTERNAL_FEED="true"
      ;;
    --no-external-feed)
      INCLUDE_EXTERNAL_FEED="false"
      ;;
    --external-start=*)
      EXTERNAL_START="${arg#*=}"
      ;;
    --external-end=*)
      EXTERNAL_END="${arg#*=}"
      ;;
    --external-min-per-month=*)
      EXTERNAL_MIN_PER_MONTH="${arg#*=}"
      ;;
    --external-max-per-month=*)
      EXTERNAL_MAX_PER_MONTH="${arg#*=}"
      ;;
    --external-seed=*)
      EXTERNAL_SEED="${arg#*=}"
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage
      exit 1
      ;;
  esac
done

if [ -f "${APP_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ROOT}/.env"
  set +a
fi

CORE_TABLE_PREFIXES=(
  "dh_dim_country"
  "dh_dim_currency"
  "dh_dim_transaction_type"
  "dh_dim_household"
  "dh_dim_customer"
  "dh_dim_associated_party"
  "dh_dim_account"
  "dh_dim_branch"
  "dh_bridge_household_customer"
  "dh_bridge_customer_account"
  "dh_bridge_customer_associated_party"
  "dh_fact_cash"
)

# Safety guard: branch data must be staged before cash fact data.
branch_idx=-1
cash_idx=-1
for i in "${!CORE_TABLE_PREFIXES[@]}"; do
  if [ "${CORE_TABLE_PREFIXES[$i]}" = "dh_dim_branch" ]; then
    branch_idx="$i"
  fi
  if [ "${CORE_TABLE_PREFIXES[$i]}" = "dh_fact_cash" ]; then
    cash_idx="$i"
  fi
done
if [ "$branch_idx" -lt 0 ] || [ "$cash_idx" -lt 0 ] || [ "$branch_idx" -ge "$cash_idx" ]; then
  echo "Invalid CORE_TABLE_PREFIXES order: dh_dim_branch must appear before dh_fact_cash." >&2
  exit 1
fi

mkdir -p "${LANDING_DIR}"

if [ "${DRY_RUN}" = "true" ]; then
  echo "[dry-run] Resetting core tables"
  PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/reset_core_pipeline_tables.py" --dry-run

  echo "[dry-run] Would remove landing CSV files in ${LANDING_DIR}"
  find "${LANDING_DIR}" -maxdepth 1 -type f -name '*.csv' -print || true

  echo "[dry-run] Would copy core sample files to landing:"
  total_tables="${#CORE_TABLE_PREFIXES[@]}"
  table_idx=0
  for prefix in "${CORE_TABLE_PREFIXES[@]}"; do
    table_idx=$((table_idx + 1))
    src="${SAMPLE_DIR}/${prefix}_sample.csv"
    if [ -f "$src" ]; then
      echo "  [${table_idx}/${total_tables}] ${prefix} -> ${src}"
    else
      echo "  [${table_idx}/${total_tables}] ${prefix} -> MISSING: ${src}" >&2
    fi
  done

  if [ "${INCLUDE_EXTERNAL_FEED}" = "true" ]; then
    echo "[dry-run] Would generate and stage external feed files:"
    echo "  - dh_dim_counterparty_account_external_sample.csv"
    echo "  - dh_fact_cash_external_sample.csv"
    echo "  with args:"
    echo "    --start=${EXTERNAL_START} --end=${EXTERNAL_END} --min-per-month=${EXTERNAL_MIN_PER_MONTH} --max-per-month=${EXTERNAL_MAX_PER_MONTH} --seed=${EXTERNAL_SEED}"
  fi

  echo "[dry-run] Would seed unknown (NA) keys to core dimensions via scripts/seed_unknown_dimension_keys.py"
  echo "[dry-run] Would run pipeline job via scripts/run_job.sh"
  exit 0
fi

echo "[1/4] Resetting core tables..."
PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/reset_core_pipeline_tables.py"

echo "[2/4] Clearing landing CSV files..."
find "${LANDING_DIR}" -maxdepth 1 -type f -name '*.csv' -delete

echo "[3/4] Copying core sample files to landing..."
copied=0
total_tables="${#CORE_TABLE_PREFIXES[@]}"
table_idx=0
for prefix in "${CORE_TABLE_PREFIXES[@]}"; do
  table_idx=$((table_idx + 1))
  src="${SAMPLE_DIR}/${prefix}_sample.csv"
  echo "  [${table_idx}/${total_tables}] Preparing ${prefix}"
  if [ ! -f "$src" ]; then
    echo "  [${table_idx}/${total_tables}] Missing required sample file: ${src}" >&2
    exit 1
  fi
  target="${LANDING_DIR}/${prefix}_$(date +%Y%m%d_%H%M%S).csv"
  cp -f "$src" "$target"
  echo "  [${table_idx}/${total_tables}] Copied ${src} -> ${target}"
  copied=$((copied + 1))
done

echo "Copied ${copied} files to ${LANDING_DIR}"

if [ "${INCLUDE_EXTERNAL_FEED}" = "true" ]; then
  echo "[4/5] Generating and staging external transfer feed..."
  PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/generate_external_transfer_feed.py" \
    --start "${EXTERNAL_START}" \
    --end "${EXTERNAL_END}" \
    --min-per-month "${EXTERNAL_MIN_PER_MONTH}" \
    --max-per-month "${EXTERNAL_MAX_PER_MONTH}" \
    --seed "${EXTERNAL_SEED}"

  cp_src="${SAMPLE_DIR}/dh_dim_counterparty_account_external_sample.csv"
  cash_src="${SAMPLE_DIR}/dh_fact_cash_external_sample.csv"
  if [ ! -f "${cp_src}" ] || [ ! -f "${cash_src}" ]; then
    echo "External feed generation did not produce expected files:" >&2
    echo "  ${cp_src}" >&2
    echo "  ${cash_src}" >&2
    exit 1
  fi

  cp_target="${LANDING_DIR}/dh_dim_counterparty_account_external_$(date +%Y%m%d_%H%M%S).csv"
  cash_target="${LANDING_DIR}/dh_fact_cash_external_$(date +%Y%m%d_%H%M%S).csv"
  cp -f "${cp_src}" "${cp_target}"
  cp -f "${cash_src}" "${cash_target}"
  echo "  staged external counterparty feed: ${cp_target}"
  echo "  staged external cash feed: ${cash_target}"
  echo "  note: pipeline table order processes dh_dim_counterparty_account before dh_fact_cash"
  seed_step="[5/6]"
  run_step="[6/6]"
else
  seed_step="[4/5]"
  run_step="[5/5]"
fi

echo "${seed_step} Seeding unknown (NA) dimension keys..."
PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/seed_unknown_dimension_keys.py"

echo "${run_step} Running pipeline..."
"${SCRIPT_DIR}/run_job.sh"

echo "Core sample reload complete."
