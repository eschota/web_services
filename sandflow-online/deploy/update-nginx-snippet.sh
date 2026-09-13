#!/usr/bin/env bash
set -euo pipefail
before_sha="${1:?expected current snippet SHA256 required}"
candidate_sha="${2:?candidate SHA256 required}"
[[ "$before_sha" =~ ^[a-fA-F0-9]{64}$ && "$candidate_sha" =~ ^[a-fA-F0-9]{64}$ ]] || exit 2
target=/etc/nginx/snippets/sandflow-online.conf
work=/srv/sandflow/.work
candidate="$work/sandflow-online.nginx.conf"
asset_root=/srv/autorig/webgl-assets/realflow/releases/sandflow-v0135-f957395
[[ -f "$asset_root/index.html" && -f "$asset_root/Build/web.wasm.unityweb" && -f "$asset_root/Build/web.data.unityweb" ]] || exit 4
printf '%s  %s\n' "$before_sha" "$target" | sha256sum --check --status
printf '%s  %s\n' "$candidate_sha" "$candidate" | sha256sum --check --status
backup="$work/sandflow-nginx.$before_sha.backup.conf"
cp -p "$target" "$backup"
install -m 644 "$candidate" "$target"
if /usr/sbin/nginx -t; then
    systemctl reload nginx
else
    # Roll back only our exact candidate, never a concurrent administrator edit.
    printf '%s  %s\n' "$candidate_sha" "$target" | sha256sum --check --status
    cp -p "$backup" "$target"
    /usr/sbin/nginx -t
    exit 3
fi
