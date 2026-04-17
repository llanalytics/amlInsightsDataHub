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

PORT="${PORT:-8100}"

cd "${APP_ROOT}"
PYTHONPATH="${APP_ROOT}" uvicorn app.main:app --host 0.0.0.0 --port "${PORT}" --reload
