#!/usr/bin/env bash
set -euo pipefail
base=/srv/autorig/webgl-assets/gravityhouse
incoming="$base/incoming"
code="$base/settings-service"
install -d -m 0755 "$code"
install -d -m 0755 -o www-data -g www-data "$base/data"
install -m 0644 "$incoming/server.py" "$code/server.py"
install -m 0644 "$incoming/schema.json" "$code/schema.json"
install -m 0644 "$incoming/test_store.py" "$code/test_store.py"
python3 "$code/test_store.py"
install -m 0644 "$incoming/gravityhouse-settings.service" /etc/systemd/system/gravityhouse-settings.service
systemctl daemon-reload
systemctl enable gravityhouse-settings.service
systemctl restart gravityhouse-settings.service
curl --fail --silent --show-error --retry 5 --retry-all-errors --retry-delay 1 http://127.0.0.1:8263/gravityhouse/api/health
backup="$base/deploy/settings-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup"
cp /etc/nginx/snippets/autorig-gravityhouse-webgl.conf "$backup/nginx.before"
install -m 0644 "$incoming/nginx.conf" /etc/nginx/snippets/autorig-gravityhouse-webgl.conf
if ! /usr/sbin/nginx -t; then cp "$backup/nginx.before" /etc/nginx/snippets/autorig-gravityhouse-webgl.conf; exit 1; fi
systemctl reload nginx
echo 'Settings service installed. Existing data/settings.json was preserved.'
