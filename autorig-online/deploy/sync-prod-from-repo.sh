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

# --- Renderfin service (render queue + character generation) ---
echo "==> renderfin: data dirs, masks, worker registry seed, systemd unit"
sudo mkdir -p /var/autorig/renderfin/render/masks /var/autorig/renderfin/servers /var/autorig/renderfin/db /var/autorig/renderfin/tmp
sudo cp -f "${REPO_ROOT}/backend/renderfin/assets/masks/"* /var/autorig/renderfin/render/masks/
# Seed worker registry only for servers not already registered (runtime edits win)
for seed in "${REPO_ROOT}/deploy/renderfin-servers/"*.json; do
  name="$(basename "${seed}")"
  if [[ ! -f "/var/autorig/renderfin/servers/${name}" ]]; then
    sudo cp "${seed}" "/var/autorig/renderfin/servers/${name}"
    echo "    seeded ${name}"
  fi
done
sudo chown -R www-data:www-data /var/autorig/renderfin
sudo cp -f "${REPO_ROOT}/deploy/autorig-renderfin.service" /etc/systemd/system/autorig-renderfin.service
sudo cp -f "${REPO_ROOT}/deploy/autorig-telegram.service" /etc/systemd/system/autorig-telegram.service
if [[ ! -f /etc/autorig-renderfin.env ]]; then
  sudo cp "${REPO_ROOT}/deploy/autorig-renderfin.env.example" /etc/autorig-renderfin.env
  echo "    installed /etc/autorig-renderfin.env from example — review RENDERFIN_WORKER_BASIC_AUTH"
fi
sudo systemctl daemon-reload
sudo systemctl enable autorig-renderfin >/dev/null 2>&1 || true

sudo systemctl restart autorig
sudo systemctl restart autorig-renderfin
sudo systemctl restart autorig-telegram || echo "WARN: autorig-telegram restart failed (unit missing?)"

echo "OK: canonical root tree verified; autorig + renderfin + telegram restarted."
