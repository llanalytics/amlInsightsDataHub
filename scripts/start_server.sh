#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PATH="${APP_ROOT}/.venv/bin:${PATH}"

if [ -f "${APP_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ROOT}/.env"
  set +a
fi

PORT="${PORT:-8100}"

cd "${APP_ROOT}"
if [ -x "${APP_ROOT}/.venv/bin/uvicorn" ]; then
  PYTHONPATH="${APP_ROOT}" exec "${APP_ROOT}/.venv/bin/uvicorn" app.main:app --host 0.0.0.0 --port "${PORT}" --reload
fi

PYTHONPATH="${APP_ROOT}" exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT}" --reload
