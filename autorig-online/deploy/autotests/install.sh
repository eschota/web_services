#!/usr/bin/env bash
# Install the regression autotests on the VPS (idempotent; run from a copy of autorig-online/deploy/autotests):
#   sudo bash install.sh
# Code: /srv/autorig/autotests (outside the release tree, so a web release never drops it).
# Data: /srv/autorig/data/autotests/{corpus (read-only inputs), reports, work, mt-tree}.
# rig_canary.py (deploy/fleet) goes to /srv/autorig/fleet/rig_canary.py when it sits next to this dir.
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"
DST=/srv/autorig/autotests
DATA=/srv/autorig/data/autotests
install -d -m 755 "$DST" "$DST/vendor"
put() {  # atomic: temp file in the target dir, then rename
  local src="$1" dst="$2" mode="$3" tmp
  tmp="$(mktemp "$(dirname "$dst")/.$(basename "$dst").XXXXXX")"
  cp "$src" "$tmp"; chmod "$mode" "$tmp"; chown root:root "$tmp"; mv -f "$tmp" "$dst"
}
put "$SRC/autotests.py" "$DST/autotests.py" 755
put "$SRC/checks.py" "$DST/checks.py" 644
put "$SRC/corpus.json" "$DST/corpus.json" 644
put "$SRC/gate" "$DST/gate" 755
put "$SRC/vendor/deformation_probe.py" "$DST/vendor/deformation_probe.py" 644
if [ -f "$SRC/../fleet/rig_canary.py" ]; then
  put "$SRC/../fleet/rig_canary.py" /srv/autorig/fleet/rig_canary.py 755
fi
for u in autorig-autotests-nightly.service autorig-autotests-nightly.timer; do
  put "$SRC/$u" "/etc/systemd/system/$u" 644
done
systemctl daemon-reload
systemctl enable --now autorig-autotests-nightly.timer >/dev/null
install -d -m 755 "$DATA" "$DATA/corpus"
install -d -m 755 -o autorig -g autorig "$DATA/reports" "$DATA/work" "$DATA/mt-tree"
/srv/autorig/venv/bin/python3 -P "$DST/autotests.py" sync
echo "installed: $DST ($(sha256sum "$DST/corpus.json" | cut -c1-12) corpus.json)"
