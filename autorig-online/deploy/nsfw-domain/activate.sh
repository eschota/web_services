#!/usr/bin/env bash
# Switch on the AutoRig adult domain once it is registered (owner order 2026-10-11).
#
#   sudo bash activate.sh <host>            e.g. autorig.red
#
# 1. DNS @ and www -> this VPS (Namecheap API, other records kept)
# 2. a port-80 block, nginx -t, reload, Let's Encrypt (certbot --nginx, like autorig.online)
# 3. the full gated server block, nginx -t, reload (never `listen <IP>:443`)
# 4. site-modes.json: nsfw_hosts = [host] (read-modify-write, atomic). The backend re-reads it, no restart.
#
# It does not buy anything and does not switch the main-site split on; that is
#   site-modes.json "main_split_section": true, "main_split_items": true
# after the domain answers and the owner has tried the gate.
set -euo pipefail
HOST="${1:?usage: activate.sh <host>}"
HOST="$(echo "$HOST" | tr 'A-Z' 'a-z')"
HERE="$(cd "$(dirname "$0")" && pwd)"
AVAIL=/etc/nginx/sites-available/$HOST
CFG=/srv/autorig/live/config/site-modes.json
IP=37.187.57.177

case "$HOST" in autorig.online|*.autorig.online) echo "refusing: $HOST is the main site"; exit 2;; esac

echo "== DNS"
python3 "$HERE/namecheap_domain.py" dns "$HOST" "$IP"
for i in $(seq 1 60); do
  got="$(getent ahostsv4 "$HOST" | awk 'NR==1{print $1}')" || true
  [ "$got" = "$IP" ] && break
  sleep 10
done
[ "${got:-}" = "$IP" ] || { echo "DNS for $HOST does not point here yet (got '${got:-}'); run again later"; exit 3; }

echo "== nginx port 80 + certificate"
if [ ! -f "/etc/letsencrypt/live/$HOST/fullchain.pem" ]; then
  cat > "$AVAIL.tmp" <<EOF
server {
    listen $IP:80;
    server_name $HOST www.$HOST;
    location / { return 301 https://$HOST\$request_uri; }
}
EOF
  mv -f "$AVAIL.tmp" "$AVAIL"
  ln -sfn "$AVAIL" "/etc/nginx/sites-enabled/$HOST"
  nginx -t
  systemctl reload nginx
  certbot certonly --nginx --non-interactive --agree-tos --keep-until-expiring -d "$HOST" -d "www.$HOST"
fi

echo "== nginx gated server block"
sed "s/__HOST__/$HOST/g" "$HERE/nginx-adult-domain.conf.template" > "$AVAIL.tmp"
if grep -nE "listen\s+[0-9.]+:443" "$AVAIL.tmp"; then echo "IP-bound 443 listener found, refusing"; exit 4; fi
cp -a "$AVAIL" "$AVAIL.bak-$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
mv -f "$AVAIL.tmp" "$AVAIL"
ln -sfn "$AVAIL" "/etc/nginx/sites-enabled/$HOST"
if ! nginx -t; then echo "nginx -t failed; previous file kept as .bak"; exit 5; fi
systemctl reload nginx
if grep -rnE "listen\s+[0-9.]+:443" /etc/nginx/sites-enabled/; then echo "WARNING: IP-bound 443 listener elsewhere"; fi

echo "== site mode config"
mkdir -p "$(dirname "$CFG")"
python3 - "$CFG" "$HOST" <<'PY'
import json, os, sys
path, host = sys.argv[1], sys.argv[2]
try:
    cfg = json.load(open(path, encoding="utf-8"))
except FileNotFoundError:
    cfg = {}
hosts = [h for h in cfg.get("nsfw_hosts", []) if h != host]
cfg["nsfw_hosts"] = [host] + hosts
cfg["nsfw_canonical_host"] = host
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(cfg, fh, indent=2, ensure_ascii=False)
    fh.write("\n")
os.chmod(tmp, 0o644)
os.replace(tmp, path)
print(json.dumps(cfg, indent=2))
PY

echo "== checks"
sleep 6
curl -sS -o /dev/null -w "gate %{http_code}\n" "https://$HOST/age-gate"
curl -sS -o /dev/null -w "nodes without sign-in %{http_code} -> %{redirect_url}\n" "https://$HOST/nodes"
curl -sS -o /dev/null -w "graphs api without sign-in %{http_code}\n" "https://$HOST/api/ai/graphs"
curl -sS -o /dev/null -w "autorig.online %{http_code}\n" "https://autorig.online/"
echo | openssl s_client -connect autorig.online:443 -servername autorig.online 2>/dev/null | openssl x509 -noout -subject
echo "Google OAuth: add https://$HOST/auth/callback to the authorized redirect URIs of the AutoRig OAuth client."
