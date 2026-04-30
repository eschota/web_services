#!/usr/bin/env bash
# Verify/install AutoRig from the canonical production tree (systemd WorkingDirectory).
# Single source of truth: /root/autorig-online. Run on the server after: cd /root && git pull
#
# Default layout matches deploy/autorig.service:
#   PROD_ROOT=/root/autorig-online  →  backend/ main.py, static/
#
set -euo pipefail
REPO_ROOT="${REPO_ROOT:-/root/autorig-online}"
PROD_ROOT="${PROD_ROOT:-/root/autorig-online}"

if [[ ! -f "${REPO_ROOT}/backend/main.py" ]]; then
  echo "ERROR: REPO_ROOT=${REPO_ROOT} has no backend/main.py" >&2
  exit 1
fi

if [[ "$(realpath "${PROD_ROOT}")" != "$(realpath "${REPO_ROOT}")" ]]; then
  echo "ERROR: AutoRig production must run directly from ${REPO_ROOT}; refusing alternate PROD_ROOT=${PROD_ROOT}" >&2
  exit 1
fi

sudo mkdir -p "${PROD_ROOT}/backend/db" "${PROD_ROOT}/static/i18n" "${PROD_ROOT}/static/js" "${PROD_ROOT}/static/css" "${PROD_ROOT}/static/fonts"

# Production venv must match backend/requirements.txt (nudenet, onnxruntime, google-api-python-client, etc.)
echo "==> pip install -r backend/requirements.txt → ${PROD_ROOT}/venv"
# Use `python -m pip` so packages always land in PROD_ROOT venv (pip shim can point elsewhere).
sudo "${PROD_ROOT}/venv/bin/python" -m pip install -r "${REPO_ROOT}/backend/requirements.txt" -q

sudo systemctl restart autorig

echo "OK: canonical root tree verified; autorig restarted."
