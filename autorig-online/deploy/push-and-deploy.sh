#!/usr/bin/env bash
# Commit (if needed) + push to origin, then deploy AutoRig to /opt and restart services.
# Run on the machine that has the git repo and production paths (e.g. /root + /opt/autorig-online).
#
# Usage:
#   ./push-and-deploy.sh
#   ./push-and-deploy.sh "fix: API docs"
#   SKIP_NGINX=1 ./push-and-deploy.sh   # only rsync + restart autorig (no nginx reload)
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

# Static: mirror repo; drop files removed in git; skip heavy runtime caches on disk
sudo rsync -a --delete \
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

echo "==> Backend: restart autorig"
sudo systemctl restart autorig

echo "OK: pushed, deployed to ${PROD_ROOT}, autorig restarted."
