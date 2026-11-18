#!/usr/bin/env bash
set -Eeuo pipefail
umask 022

base=/srv/autorig/abris
vhost=/etc/nginx/sites-enabled/autorig.online-storage
snippet=/etc/nginx/snippets/autorig-abris.conf
limits=/etc/nginx/conf.d/autorig-abris-limits.conf
unit=/etc/systemd/system/autorig-abris@.service
release=${1:?Pass the release id}
checksum=${2:?Pass the archive SHA-256}
[[ "$(id -u)" == 0 ]]
[[ "$release" =~ ^[a-f0-9]{12}-[0-9]{8}T[0-9]{6}Z$ ]]
[[ "$checksum" =~ ^[a-f0-9]{64}$ ]]
archive="$base/incoming/$release.tar.gz"
dest="$base/releases/$release"
record="$base/deploy/$release"

install -d "$base/releases" "$base/deploy"
exec 9>"$base/deploy/deploy.lock"
flock -x 9
printf '%s  %s\n' "$checksum" "$archive" | sha256sum -c -
mkdir "$dest" "$record"

# Accept only regular files inside the three sections of an Abris release.
python3 - "$archive" "$dest" <<'PY'
import pathlib
import shutil
import sys
import tarfile

archive, destination = sys.argv[1:]
total = 0
with tarfile.open(archive, "r:gz") as package:
    for member in package:
        rel = pathlib.PurePosixPath(member.name)
        if (
            rel.is_absolute()
            or ".." in rel.parts
            or not rel.parts
            or rel.parts[0] not in {"backend", "static", "deploy"}
            or not (member.isdir() or member.isfile())
        ):
            raise SystemExit(f"Unexpected package path: {member.name}")
        target = pathlib.Path(destination, *rel.parts)
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        total += member.size
        if total > 64 * 1024 * 1024:
            raise SystemExit("Abris release exceeds 64 MiB")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as output:
            shutil.copyfileobj(package.extractfile(member), output)
PY

for file in backend/server.py backend/recognition.py backend/dwg.py backend/requirements.txt static/abris/index.html static/abris/app.mjs \
    backend/train_handwriting.py deploy/autorig-abris@.service deploy/autorig-abris-train.service \
    deploy/autorig-abris-train.timer deploy/bootstrap.sh deploy/setup-dwg.sh \
    deploy/nginx-site.conf deploy/nginx-proxy.conf deploy/nginx-limits.conf; do
    test -s "$dest/$file"
done
venv=$(bash "$dest/deploy/bootstrap.sh" "$dest/backend/requirements.txt")
[[ -x "$venv/bin/gunicorn" ]]
bash "$dest/deploy/setup-dwg.sh" "$base"
(
    cd "$dest/backend"
    runuser -u autorig-abris -- env PYTHONDONTWRITEBYTECODE=1 "$venv/bin/python" \
        -c 'from server import application; from recognition import recognize; from dwg import convert_dxf'
)
printf '%s\n' "$checksum" > "$record/archive.sha256"
"$venv/bin/python" -m pip freeze > "$record/runtime-requirements.txt"

previous=''
old_slot=''
if [[ -L "$base/current" ]]; then
    previous=$(readlink -f "$base/current")
    [[ "$previous" == "$base/releases/"* ]]
    read -r old_slot < "$base/deploy/${previous##*/}/slot"
    [[ "$old_slot" == primary || "$old_slot" == secondary ]]
else
    test ! -e "$base/current"
fi
slot=primary
port=8307
if [[ "$old_slot" == primary ]]; then
    slot=secondary
    port=8308
fi
service="autorig-abris@$slot.service"
test -z "$(ss -Hltn "sport = :$port")"
printf '%s\n' "$previous" > "$record/previous-release"
printf '%s\n' "$slot" > "$record/slot"
printf 'ABRIS_GUNICORN=%s\nABRIS_BACKEND_DIR=%s/backend\nABRIS_PORT=%s\n' "$venv/bin/gunicorn" "$dest" "$port" > "$record/service.env"

install_atomically() {
    local source=$1 target=$2
    install -m 0644 "$source" "$record/atomic"
    mv -Tf "$record/atomic" "$target"
}

started=0
nginx_changed=0
include_added=0
current_changed=0
change_include() {
    python3 - "$vhost" "$record/vhost.tmp" "$1" <<'PY'
import os
import pathlib
import sys

path, stage = map(pathlib.Path, sys.argv[1:3])
operation = sys.argv[3]
include = "    include /etc/nginx/snippets/autorig-abris.conf;\n"
anchor = "    root /srv/autorig/current/autorig-online/static;\n"
old = path.read_text()
if operation == "add" and include not in old:
    if old.count(anchor) != 1:
        raise SystemExit("AutoRig nginx vhost layout changed")
    new = old.replace(anchor, anchor + include, 1)
elif operation == "remove":
    new = old.replace(include, "", 1)
else:
    new = old
if new != old:
    stage.write_text(new)
    file_stat = path.stat()
    os.chmod(stage, file_stat.st_mode & 0o777)
    os.chown(stage, file_stat.st_uid, file_stat.st_gid)
    os.replace(stage, path)
PY
}

recover() {
    local status=$?
    if (( status == 0 )); then
        return
    fi
    set +e
    if (( nginx_changed )); then
        if (( include_added )); then change_include remove; fi
        if [[ -e "$record/site.before" ]]; then
            install_atomically "$record/site.before" "$snippet"
        else
            rm -f -- "$snippet"
        fi
        if [[ -e "$record/limits.before" ]]; then
            install_atomically "$record/limits.before" "$limits"
        else
            rm -f -- "$limits"
        fi
        nginx -t && systemctl reload nginx
    fi
    if (( current_changed )); then
        if [[ -n "$previous" ]]; then
            ln -s "$previous" "$record/previous"
            mv -Tf "$record/previous" "$base/current"
        else
            rm -f -- "$base/current"
        fi
    fi
    if (( started )); then systemctl disable --now "$service"; fi
    printf 'Abris deployment failed. See %s\n' "$record" >&2
}
trap recover EXIT

install_atomically "$dest/deploy/autorig-abris@.service" "$unit"
install_atomically "$record/service.env" "$base/deploy/$slot.env"
systemctl daemon-reload
started=1
systemctl enable --now "$service"
curl --fail --silent --show-error --retry 10 --retry-delay 1 --retry-all-errors --max-time 5 \
    "http://127.0.0.1:$port/abris/api/health" > "$record/backend-health.json"

sed "s|@@PORT@@|$port|g" "$dest/deploy/nginx-proxy.conf" > "$record/nginx-proxy.conf"
sed -e "s|@@RELEASE@@|$dest|g" -e "s|@@PROXY@@|$record/nginx-proxy.conf|g" \
    "$dest/deploy/nginx-site.conf" > "$record/nginx-site.conf"

exec 8>/srv/autorig/webgl-assets/.nginx-site.lock
flock -x 8
[[ ! -e "$snippet" ]] || cp "$snippet" "$record/site.before"
[[ ! -e "$limits" ]] || cp "$limits" "$record/limits.before"
nginx_changed=1
install_atomically "$record/nginx-site.conf" "$snippet"
install_atomically "$dest/deploy/nginx-limits.conf" "$limits"
if ! grep -Fqx '    include /etc/nginx/snippets/autorig-abris.conf;' "$vhost"; then
    include_added=1
    change_include add
fi
if ! nginx -t > "$record/nginx-test.log" 2>&1; then
    cat "$record/nginx-test.log" >&2
    exit 1
fi
systemctl reload nginx

origin=https://autorig.online
fetch=(curl --fail --silent --show-error --max-time 7 \
    --resolve autorig.online:443:127.0.0.1 --compressed)
verify_static() {
    local route=$1 expected=$2 actual=$3 headers=$4
    for attempt in {1..16}; do
        if "${fetch[@]}" --dump-header "$headers" "$origin$route" > "$actual" 2>> "$record/smoke.log" \
            && cmp -s "$expected" "$actual"; then
            return
        fi
        sleep 0.5
    done
    cmp "$expected" "$actual"
}
verify_static /abris "$dest/static/abris/index.html" "$record/index.live.html" "$record/index.headers"
grep -qi '^content-type: text/html' "$record/index.headers"
verify_static /static/abris/app.mjs "$dest/static/abris/app.mjs" "$record/app.live.mjs" "$record/app.headers"
grep -qi '^content-type: application/javascript' "$record/app.headers"
"${fetch[@]}" "$origin/abris/api/health" > "$record/public-health.json"
redirect=$("${fetch[@]}" --output /dev/null --write-out '%{http_code} %{redirect_url}' "$origin/abris/")
[[ "$redirect" == "308 $origin/abris" ]]

ln -s "$dest" "$record/current"
mv -Tf "$record/current" "$base/current"
current_changed=1
trap - EXIT
flock -u 8
if [[ -n "$old_slot" ]]; then
    systemctl disable "autorig-abris@$old_slot.service"
    systemctl stop --no-block "autorig-abris@$old_slot.service"
fi
printf 'Published %s at %s/abris\n' "$release" "$origin"

# The timer can run only with an operator opt-in and labeled corrections.
printf 'ABRIS_TRAIN_PYTHON=%s\nABRIS_TRAIN_SCRIPT=%s/backend/train_handwriting.py\n' "$venv/bin/python" "$dest" > "$record/training.env"
install_atomically "$record/training.env" "$base/deploy/training.env"
for entry in autorig-abris-train.service autorig-abris-train.timer; do
    install_atomically "$dest/deploy/$entry" "/etc/systemd/system/$entry"
done
systemctl daemon-reload
systemctl enable --now autorig-abris-train.timer
