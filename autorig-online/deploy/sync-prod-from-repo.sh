#!/usr/bin/env bash
# Deploy AutoRig from the git checkout into the production tree (systemd WorkingDirectory).
# Single source of truth: repository. Run on the server after: cd /root && git pull
#
# Default layout matches deploy/autorig.service:
#   PROD_ROOT=/opt/autorig-online  →  backend/ main.py, static/
#
set -euo pipefail
REPO_ROOT="${REPO_ROOT:-/root/autorig-online}"
PROD_ROOT="${PROD_ROOT:-/opt/autorig-online}"

if [[ ! -f "${REPO_ROOT}/backend/main.py" ]]; then
  echo "ERROR: REPO_ROOT=${REPO_ROOT} has no backend/main.py" >&2
  exit 1
fi

sudo mkdir -p "${PROD_ROOT}/backend" "${PROD_ROOT}/static/i18n" "${PROD_ROOT}/static/js" "${PROD_ROOT}/static/css"

# Production venv must match backend/requirements.txt (nudenet, onnxruntime, google-api-python-client, etc.)
echo "==> pip install -r backend/requirements.txt → ${PROD_ROOT}/venv"
# Use `python -m pip` so packages always land in PROD_ROOT venv (pip shim can point elsewhere).
sudo "${PROD_ROOT}/venv/bin/python" -m pip install -r "${REPO_ROOT}/backend/requirements.txt" -q

sudo cp -a "${REPO_ROOT}/backend/"*.py "${PROD_ROOT}/backend/"
if [[ -f "${REPO_ROOT}/skill.md" ]]; then
  sudo cp -a "${REPO_ROOT}/skill.md" "${PROD_ROOT}/skill.md"
fi
sudo cp -a "${REPO_ROOT}/static/developers.html" "${PROD_ROOT}/static/"
sudo cp -a "${REPO_ROOT}/static/buy-credits.html" "${PROD_ROOT}/static/"
sudo cp -a "${REPO_ROOT}/static/dashboard.html" "${PROD_ROOT}/static/"
sudo cp -a "${REPO_ROOT}/static/task.html" "${PROD_ROOT}/static/"
sudo cp -a "${REPO_ROOT}/static/terms-of-use.html" "${REPO_ROOT}/static/user-agreement.html" "${PROD_ROOT}/static/"
sudo cp -a "${REPO_ROOT}/static/js/header.js" "${REPO_ROOT}/static/js/footer.js" "${REPO_ROOT}/static/js/site-layout.js" \
  "${REPO_ROOT}/static/js/rig-editor.js" "${REPO_ROOT}/static/js/sprite-sheet-mvp.js" "${PROD_ROOT}/static/js/"
sudo cp -a "${REPO_ROOT}/static/css/styles.css" "${PROD_ROOT}/static/css/"
sudo cp -a "${REPO_ROOT}/static/i18n/"*.json "${PROD_ROOT}/static/i18n/"

sudo systemctl restart autorig

echo "OK: backend *.py + static (task, dashboard, buy-credits, layout JS, styles, developers, i18n) → ${PROD_ROOT}; autorig restarted."
