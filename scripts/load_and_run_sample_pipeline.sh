#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${APP_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ROOT}/.env"
  set +a
fi

cd "${APP_ROOT}"

echo "[1/2] Copying sample files to landing..."
"${SCRIPT_DIR}/load_sample_data.sh"

echo "[2/2] Running cash pipeline..."
"${SCRIPT_DIR}/run_job.sh"

echo "Done. Sample files loaded and pipeline executed."
