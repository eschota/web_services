#!/usr/bin/env bash
set -euo pipefail
# Run as root. All inputs and outputs are confined to the dedicated SandFlow project.
release="${1:?commit SHA required}"
archive_sha="${2:?archive SHA256 required}"
[[ "$release" =~ ^[a-f0-9]{40}$ ]] || exit 2
[[ "$archive_sha" =~ ^[a-fA-F0-9]{64}$ ]] || exit 2
root=/srv/sandflow
archive="$root/.work/$release.tar.gz"
target="$root/releases/$release"
[[ -f "$archive" ]] || exit 3
printf '%s  %s\n' "$archive_sha" "$archive" | sha256sum --check --status
if ! id sandflow >/dev/null 2>&1; then
    useradd --system --home-dir "$root" --shell /usr/sbin/nologin sandflow
fi
install -d -m 755 "$root" "$root/releases" "$root/.work"
install -d -m 700 -o sandflow -g sandflow "$root/data" "$root/.work/tmp"
install -d -m 700 "$root/secrets"
if [[ ! -d "$target" ]]; then
    install -d -m 755 "$target"
    tar -xzf "$archive" --no-same-owner -C "$target"
    printf '%s\n' "$archive_sha" > "$target/.archive-sha256"
    printf '%s\n' "$release" > "$target/REVISION"
else
    [[ -f "$target/.archive-sha256" && "$(cat "$target/.archive-sha256")" == "$archive_sha" ]] || exit 6
fi
[[ -x "$target/SandFlow.Server" ]] || chmod 755 "$target/SandFlow.Server"
[[ -f "$root/.work/sandflow-online.service" ]] || exit 4
install -m 644 "$root/.work/sandflow-online.service" /etc/systemd/system/sandflow-online.service
previous="$(readlink -f "$root/current" 2>/dev/null || true)"
ln -sfn "$target" "$root/current.next"
mv -Tf "$root/current.next" "$root/current"
systemctl daemon-reload
systemctl enable sandflow-online.service
systemctl restart sandflow-online.service
ok=false
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS http://127.0.0.1:8270/health; then ok=true; break; fi
    sleep 1
done
if [[ "$ok" != true ]]; then
    if [[ "$previous" == "$root/releases/"* && -d "$previous" ]]; then
        ln -sfn "$previous" "$root/current.next"
        mv -Tf "$root/current.next" "$root/current"
        systemctl restart sandflow-online.service
    else
        systemctl stop sandflow-online.service
    fi
    exit 5
fi
systemctl show sandflow-online.service -p MainPID -p ActiveState -p WorkingDirectory
