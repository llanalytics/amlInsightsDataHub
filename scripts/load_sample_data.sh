#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LANDING_DIR="${APP_ROOT}/data/landing"

mkdir -p "${LANDING_DIR}"

copied=0
for f in "${APP_ROOT}"/data/sample/dh_*_sample.csv; do
  [ -f "${f}" ] || continue
  base="$(basename "${f}")"
  target_name="${base%_sample.csv}_$(date +%Y%m%d_%H%M%S).csv"
  cp -f "${f}" "${LANDING_DIR}/${target_name}"
  copied=$((copied + 1))
done

echo "Sample files copied to ${LANDING_DIR} (${copied} files)"
