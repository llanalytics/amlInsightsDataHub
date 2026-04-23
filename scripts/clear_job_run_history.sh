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

PYTHONPATH="${APP_ROOT}" python3 "${SCRIPT_DIR}/clear_job_run_history.py" "$@"

