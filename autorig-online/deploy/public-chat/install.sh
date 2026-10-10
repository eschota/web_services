#!/usr/bin/env bash
# Install / update the AutoRig public chat (Public chat · V3) on the autorig.online host.
# Run from a directory holding public_chat.py, autorig-public-chat.service and nginx-public-chat.conf:
#     sudo bash install.sh            # service + nginx include (idempotent)
#     sudo bash install.sh --service  # only the service file and a restart (code update)
# Never touches autorig-storage. nginx: backs up the site config, inserts one include line, `nginx -t`,
# reload; restores the backup if the test fails. Refuses to run if any enabled site listens on IP:443.
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"
APP=/srv/autorig/public-chat
DATA=/srv/autorig/data/public-chat
SECRETS=/srv/autorig/secrets
LIVE_CFG=/srv/autorig/live/config/public-chat.json
SITE=/etc/nginx/sites-available/autorig.online-storage
SNIPPET=/etc/nginx/snippets/autorig-public-chat.conf
INCLUDE='    include /etc/nginx/snippets/autorig-public-chat.conf;'
ONLY_SERVICE=0
[ "${1:-}" = "--service" ] && ONLY_SERVICE=1
stamp() { date -u +%Y%m%dT%H%M%SZ; }

install -d -o root -g autorig -m 755 "$APP"
install -d -o autorig -g autorig -m 750 "$DATA"
install -d -o autorig -g autorig -m 755 "$DATA/avatars"

# code: atomic replace (temp + rename), checked by compiling first
tmp="$APP/.public_chat.py.$$"
install -o root -g autorig -m 644 "$SRC/public_chat.py" "$tmp"
/srv/autorig/venv/bin/python3 -m py_compile "$tmp"
mv -f "$tmp" "$APP/public_chat.py"
# the avatar maker runs as a child process (own memory, time limit) and is installed beside the service
tmp="$APP/.avatar_make.py.$$"
install -o root -g autorig -m 644 "$SRC/avatar_make.py" "$tmp"
/srv/autorig/venv/bin/python3 -m py_compile "$tmp"
mv -f "$tmp" "$APP/avatar_make.py"
rm -rf "$APP/__pycache__"

if [ "$ONLY_SERVICE" = 1 ]; then
    install -d -o autorig -g autorig -m 755 "$DATA/avatars"
    systemctl restart autorig-public-chat.service
    sleep 2
    systemctl is-active autorig-public-chat.service
    curl -fsS http://127.0.0.1:8278/api/public-chat/health; echo
    exit 0
fi

# secrets: the HMAC key (identities are stored only as HMACs) and the agent token for Astra
if [ ! -s "$SECRETS/public-chat.key" ]; then
    umask 077; openssl rand -hex 32 > "$SECRETS/public-chat.key"
fi
chown root:autorig "$SECRETS/public-chat.key"; chmod 640 "$SECRETS/public-chat.key"
if [ ! -s "$SECRETS/public-chat-astra.token" ]; then
    umask 077; openssl rand -hex 32 > "$SECRETS/public-chat-astra.token"
    chmod 600 "$SECRETS/public-chat-astra.token"
fi
if [ ! -s "$SECRETS/public-chat-agents.json" ]; then
    sum=$(tr -d '\n' < "$SECRETS/public-chat-astra.token" | sha256sum | cut -d' ' -f1)
    printf '{"agents": [{"name": "astra", "sha256": "%s"}]}\n' "$sum" > "$SECRETS/public-chat-agents.json"
fi
chown root:autorig "$SECRETS/public-chat-agents.json"; chmod 640 "$SECRETS/public-chat-agents.json"

# live switches (kill switch etc.): created once, then edited atomically by admins or by hand
if [ ! -s "$LIVE_CFG" ]; then
    cat > "$LIVE_CFG.tmp" <<'JSON'
{
 "schema": "autorig.public-chat-config/1",
 "enabled": true,
 "task_rooms": true,
 "guests_can_post": true,
 "guest_links": false,
 "user_links": 1,
 "slow_mode_seconds": 0,
 "llm_moderation": true,
 "translate": true,
 "astra": "builtin",
 "note": "Public chat · V3 live switches; edit atomically (temp file + rename), read on change. enabled=false hides the chat site-wide."
}
JSON
    chown autorig:autorig "$LIVE_CFG.tmp"; chmod 640 "$LIVE_CFG.tmp"
    mv -f "$LIVE_CFG.tmp" "$LIVE_CFG"
fi

install -o root -g root -m 644 "$SRC/autorig-public-chat.service" /etc/systemd/system/autorig-public-chat.service
systemctl daemon-reload
systemctl enable autorig-public-chat.service >/dev/null
systemctl restart autorig-public-chat.service
for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -fsS http://127.0.0.1:8278/api/public-chat/health >/dev/null 2>&1 && break
    sleep 1
done
curl -fsS http://127.0.0.1:8278/api/public-chat/health; echo

# nginx: snippet + one include line in the autorig.online 443 block
if grep -rnE 'listen\s+[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:443' /etc/nginx/sites-enabled/ >/dev/null 2>&1; then
    echo "REFUSING: an enabled site listens on IP:443 (it would take autorig.online's HTTPS); fix that first" >&2
    exit 3
fi
install -o root -g root -m 644 "$SRC/nginx-public-chat.conf" "$SNIPPET"
if ! grep -qF 'include /etc/nginx/snippets/autorig-public-chat.conf;' "$SITE"; then
    backup="$SITE.bak-pchat-$(stamp)"
    cp -a "$SITE" "$backup"
    anchor='    include /etc/nginx/snippets/autorig-uploader.conf;'
    if ! grep -qF "$anchor" "$SITE"; then
        echo "anchor line not found in $SITE" >&2
        exit 4
    fi
    python3 - "$SITE" "$anchor" "$INCLUDE" <<'PY'
import sys
path, anchor, line = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path, encoding="utf-8").read()
assert text.count(anchor) == 1, "anchor must be unique"
text = text.replace(anchor, anchor + "\n" + line, 1)
tmp = path + ".pchat.tmp"
open(tmp, "w", encoding="utf-8").write(text)
import os, shutil
shutil.copymode(path, tmp)
os.replace(tmp, path)
PY
    if ! nginx -t 2>&1; then
        echo "nginx -t failed: restoring $backup" >&2
        cp -a "$backup" "$SITE"
        exit 5
    fi
    echo "site config backup: $backup"
else
    nginx -t 2>&1
fi
systemctl reload nginx
echo "installed: $(systemctl is-active autorig-public-chat.service)"
