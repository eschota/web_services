#!/usr/bin/env bash
set -euo pipefail
base=/srv/autorig/webgl-assets/gravityhouse
release=${1:?release required}
digest=${2:?archive SHA-256 required}
[[ "$release" =~ ^[a-f0-9]{12}-[0-9]{8}T[0-9]{6}Z$ ]]
[[ "$digest" =~ ^[a-f0-9]{64}$ ]]
archive="$base/incoming/$release.tar.gz"
destination="$base/releases/$release"
site=/etc/nginx/sites-available/autorig.online-storage
snippet=/etc/nginx/snippets/autorig-gravityhouse-webgl.conf
mkdir -p "$base/deploy" "$base/releases" "$base/shared/gravityhouse/Build"
exec 9>"$base/deploy/deploy.lock"
flock -x 9
printf '%s  %s\n' "$digest" "$archive" | sha256sum -c -
test ! -e "$destination"
mkdir "$destination"
tar --no-same-owner -xzf "$archive" -C "$destination"
test -f "$destination/gravityhouse/index.html"
test -f "$destination/gravityhouse/release.json"
python3 - "$destination/gravityhouse" <<'PY'
import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1]);data=json.loads((root/'release.json').read_text())
for item in data['files']:
 p=root/item['path'];assert p.resolve().is_relative_to(root.resolve())
 assert hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'],p
print('Release manifest verified:',data['release'])
PY
for file in "$destination/gravityhouse/Build/"*; do
    name=$(basename "$file")
    target="$base/shared/gravityhouse/Build/$name"
    if [ -f "$target" ]; then cmp -s "$file" "$target"; else install -m 0644 "$file" "$target"; fi
done
backup="$base/deploy/$release"
mkdir "$backup"
cp "$site" "$backup/site.before"
if [ -f "$snippet" ]; then cp "$snippet" "$backup/snippet.before"; fi
install -m 0644 "$base/incoming/nginx.conf" "$snippet"
python3 - "$site" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]);text=p.read_text()
line='    include /etc/nginx/snippets/autorig-gravityhouse-webgl.conf;'
anchor='    root /srv/autorig/current/autorig-online/static;'
if line not in text:
 assert text.count(anchor)==1,'Unexpected nginx layout'
 p.write_text(text.replace(anchor,anchor+'\n'+line,1))
PY
if ! /usr/sbin/nginx -t; then
    cp "$backup/site.before" "$site"
    if [ -f "$backup/snippet.before" ]; then cp "$backup/snippet.before" "$snippet"; fi
    exit 1
fi
previous=$(readlink -f "$base/current" 2>/dev/null || true)
if [ -e "$base/current" ] && [ ! -L "$base/current" ]; then echo 'current must be a symlink' >&2; exit 1; fi
ln -s "$destination" "$base/.current-$release"
mv -Tf "$base/.current-$release" "$base/current"
systemctl reload nginx
printf '%s\n' "$previous" > "$backup/previous-release.txt"
curl --fail --silent --show-error --compressed --resolve autorig.online:443:127.0.0.1 https://autorig.online/gravityhouse/release.json > "$backup/live-release.json"
python3 - "$backup/live-release.json" "$release" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['release']==sys.argv[2]
print('Published and verified:',sys.argv[2])
PY
