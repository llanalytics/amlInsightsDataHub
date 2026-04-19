#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SAMPLE_DIR="${APP_ROOT}/data/sample"
LANDING_DIR="${APP_ROOT}/data/landing"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/load_single_table_sample.sh <table_name> [--allow-existing]

Example:
  ./scripts/load_single_table_sample.sh dh_dim_customer

Behavior:
  1) Copies data/sample/<table_name>_sample.csv to data/landing/<table_name>_<timestamp>.csv
  2) Runs ./scripts/run_job.sh to process landing files

Safety:
  By default, aborts if landing already has CSV files.
  Use --allow-existing to process alongside existing landing files.
EOF
}

TABLE_NAME=""
ALLOW_EXISTING="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --allow-existing)
      ALLOW_EXISTING="true"
      shift
      ;;
    --*)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      if [ -n "$TABLE_NAME" ]; then
        echo "Only one table_name may be provided." >&2
        usage
        exit 1
      fi
      TABLE_NAME="$1"
      shift
      ;;
  esac
done

if [ -z "$TABLE_NAME" ]; then
  echo "table_name is required." >&2
  usage
  exit 1
fi

if [ -f "${APP_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ROOT}/.env"
  set +a
fi

SAMPLE_FILE="${SAMPLE_DIR}/${TABLE_NAME}_sample.csv"
if [ ! -f "$SAMPLE_FILE" ]; then
  echo "Sample file not found: ${SAMPLE_FILE}" >&2
  exit 1
fi

mkdir -p "$LANDING_DIR"

if [ "$ALLOW_EXISTING" != "true" ]; then
  existing_count="$(find "$LANDING_DIR" -maxdepth 1 -type f -name '*.csv' | wc -l | tr -d ' ')"
  if [ "$existing_count" -gt 0 ]; then
    echo "Landing already contains ${existing_count} CSV file(s): ${LANDING_DIR}" >&2
    echo "Use --allow-existing if you want to run with current landing files." >&2
    exit 1
  fi
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
target_file="${LANDING_DIR}/${TABLE_NAME}_${timestamp}.csv"
cp -f "$SAMPLE_FILE" "$target_file"

echo "Copied sample to landing: ${target_file}"
echo "Running pipeline..."
"${SCRIPT_DIR}/run_job.sh"
