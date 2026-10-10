#!/usr/bin/env bash
# Install / update the gallery poster service (Gallery · V3). Run from a directory holding gallery_poster.py and
# autorig-gallery-poster.service:  sudo bash install.sh
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"
APP=/srv/autorig/gallery-poster
OUT=/srv/autorig/data/static/posters-v3
install -d -o root -g autorig -m 755 "$APP"
install -d -o autorig -g autorig -m 755 "$OUT"
tmp="$APP/.gallery_poster.py.$$"
install -o root -g autorig -m 644 "$SRC/gallery_poster.py" "$tmp"
/srv/autorig/venv/bin/python3 -m py_compile "$tmp"
mv -f "$tmp" "$APP/gallery_poster.py"
rm -rf "$APP/__pycache__"
install -o root -g root -m 644 "$SRC/autorig-gallery-poster.service" /etc/systemd/system/autorig-gallery-poster.service
systemctl daemon-reload
systemctl enable autorig-gallery-poster.service >/dev/null
systemctl restart autorig-gallery-poster.service
sleep 2
systemctl is-active autorig-gallery-poster.service
