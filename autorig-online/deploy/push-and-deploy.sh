#!/usr/bin/env bash
# Commit (if needed) + push to origin, then deploy AutoRig to /opt and restart services.
# Run on the machine that has the git repo and production paths (e.g. /root + /opt/autorig-online).
#
# Usage:
#   ./push-and-deploy.sh
#   ./push-and-deploy.sh "fix: API docs"
#   SKIP_NGINX=1 ./push-and-deploy.sh   # only rsync + restart autorig (no nginx reload)
#   RSYNC_STATIC_DELETE=1 ./push-and-deploy.sh   # mirror static/ exactly (deletes prod-only files under static/)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTORIG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$AUTORIG_ROOT"
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "${GIT_ROOT}" ]]; then
  echo "ERROR: not a git repository (expected autorig-online inside a repo clone)" >&2
  exit 1
fi

COMMIT_MSG="${1:-deploy: $(date -Iseconds)}"
PROD_ROOT="${PROD_ROOT:-/opt/autorig-online}"
NGINX_CONF_DST="${NGINX_CONF_DST:-/etc/nginx/sites-available/autorig.online}"

cd "$GIT_ROOT"

echo "==> Git: add / commit (if needed) / push"
git add -A
if git diff --cached --quiet; then
  echo "    nothing to commit"
else
  git commit -m "$COMMIT_MSG"
fi
git push

echo "==> Deploy: rsync → ${PROD_ROOT}"
sudo mkdir -p "${PROD_ROOT}/backend" "${PROD_ROOT}/static"

# Backend: all files except SQLite DB dir (prod keeps its own db)
sudo rsync -a \
  --exclude 'db/' \
  "${AUTORIG_ROOT}/backend/" "${PROD_ROOT}/backend/"

# Static: add/update from repo; skip heavy runtime caches. Default: no --delete (avoids wiping
# prod-only assets that were never committed). Set RSYNC_STATIC_DELETE=1 to mirror repo exactly.
STATIC_RSYNC=(sudo rsync -a)
if [[ "${RSYNC_STATIC_DELETE:-0}" == "1" ]]; then
  STATIC_RSYNC+=(--delete)
fi
"${STATIC_RSYNC[@]}" \
  --exclude 'tasks/' \
  --exclude 'glb_cache/' \
  "${AUTORIG_ROOT}/static/" "${PROD_ROOT}/static/"

if [[ "${SKIP_NGINX:-0}" != "1" ]]; then
  echo "==> Nginx: install config + reload"
  sudo cp -a "${AUTORIG_ROOT}/deploy/nginx.conf" "${NGINX_CONF_DST}"
  sudo nginx -t
  sudo systemctl reload nginx
else
  echo "==> Nginx: skipped (SKIP_NGINX=1)"
fi

# Uvicorn often blocks shutdown on "Waiting for background tasks to complete" — without a cap,
# systemctl restart can leave the site down until manual SIGKILL. Drop-in merges with any host unit.
if [[ "${SKIP_SYSTEMD_AUTORIG_DROPIN:-0}" != "1" ]]; then
  echo "==> Systemd: autorig stop timeout (drop-in)"
  sudo mkdir -p /etc/systemd/system/autorig.service.d
  sudo cp -a "${AUTORIG_ROOT}/deploy/autorig.service.d/timeout.conf" /etc/systemd/system/autorig.service.d/timeout.conf
  sudo systemctl daemon-reload
else
  echo "==> Systemd: skipped drop-in (SKIP_SYSTEMD_AUTORIG_DROPIN=1)"
fi

echo "==> Backend: restart autorig"
sudo systemctl restart autorig

BACKEND_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1:8000/}"
echo "==> Backend: wait for ${BACKEND_URL}"
_ok=0
for _i in $(seq 1 45); do
  if curl -sf -o /dev/null --connect-timeout 2 --max-time 5 "${BACKEND_URL}"; then
    _ok=1
    break
  fi
  sleep 1
done
if [[ "${_ok}" != "1" ]]; then
  echo "ERROR: backend did not respond after restart. Check: systemctl status autorig; journalctl -u autorig -n 80" >&2
  exit 1
fi

echo "OK: pushed, deployed to ${PROD_ROOT}, autorig restarted and healthy."
