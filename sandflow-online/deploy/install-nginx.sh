#!/usr/bin/env bash
set -euo pipefail
before_sha="${1:?expected current site SHA256 required}"
candidate_sha="${2:?candidate SHA256 required}"
[[ "$before_sha" =~ ^[a-fA-F0-9]{64}$ && "$candidate_sha" =~ ^[a-fA-F0-9]{64}$ ]] || exit 2
site=/etc/nginx/sites-available/autorig.online-storage
work=/srv/sandflow/.work
candidate="$work/autorig.candidate.conf"
printf '%s  %s\n' "$before_sha" "$site" | sha256sum --check --status
printf '%s  %s\n' "$candidate_sha" "$candidate" | sha256sum --check --status
grep -Fq 'include /etc/nginx/snippets/sandflow-online.conf;' "$candidate"
backup="$work/autorig.$before_sha.backup.conf"
cp -p "$site" "$backup"
install -m 644 "$work/sandflow-online.nginx.conf" /etc/nginx/snippets/sandflow-online.conf
install -m 644 "$candidate" "$site"
if /usr/sbin/nginx -t; then
    systemctl reload nginx
else
    # Only undo our exact candidate. Never overwrite a concurrent administrator change.
    printf '%s  %s\n' "$candidate_sha" "$site" | sha256sum --check --status
    cp -p "$backup" "$site"
    /usr/sbin/nginx -t
    exit 3
fi
