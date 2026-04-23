#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SAMPLE_DIR="${APP_ROOT}/data/sample"
LANDING_DIR="${APP_ROOT}/data/landing"
TABLE_NAME="dh_bridge_panama_relationship"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/load_dh_bridge_panama_relationship.sh [--full] [--allow-existing]

Behavior:
  - By default, uses data/sample/dh_bridge_panama_relationship_sample_small.csv when present.
  - Falls back to data/sample/dh_bridge_panama_relationship_sample.csv.
  - Use --full to force the full sample file.
EOF
}

USE_FULL="false"
ALLOW_EXISTING="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --full)
      USE_FULL="true"
      shift
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
      echo "Unexpected argument: $1" >&2
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

small_sample="${SAMPLE_DIR}/${TABLE_NAME}_sample_small.csv"
full_sample="${SAMPLE_DIR}/${TABLE_NAME}_sample.csv"

if [ "${USE_FULL}" = "true" ]; then
  sample_file="${full_sample}"
else
  if [ -f "${small_sample}" ]; then
    sample_file="${small_sample}"
  else
    sample_file="${full_sample}"
  fi
fi

if [ ! -f "${sample_file}" ]; then
  echo "Sample file not found: ${sample_file}" >&2
  exit 1
fi

mkdir -p "${LANDING_DIR}"

if [ "${ALLOW_EXISTING}" != "true" ]; then
  existing_count="$(find "${LANDING_DIR}" -maxdepth 1 -type f -name '*.csv' | wc -l | tr -d ' ')"
  if [ "${existing_count}" -gt 0 ]; then
    echo "Landing already contains ${existing_count} CSV file(s): ${LANDING_DIR}" >&2
    echo "Use --allow-existing if you want to run with current landing files." >&2
    exit 1
  fi
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
target_file="${LANDING_DIR}/${TABLE_NAME}_${timestamp}.csv"
cp -f "${sample_file}" "${target_file}"

echo "Copied sample to landing: ${target_file}"
echo "Source sample: ${sample_file}"
echo "Running pipeline..."
"${SCRIPT_DIR}/run_job.sh"
