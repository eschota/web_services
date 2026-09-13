#!/usr/bin/env bash
set -euo pipefail
release="${1:?source commit required}"
archive_sha="${2:?archive SHA256 required}"
before_sha="${3:?current SandFlow snippet SHA256 required}"
qa_sha="${4:?candidate QA snippet SHA256 required}"
[[ "$release" =~ ^[a-f0-9]{40}$ ]] || exit 2
for value in "$archive_sha" "$before_sha" "$qa_sha"; do [[ "$value" =~ ^[a-fA-F0-9]{64}$ ]] || exit 2; done
root=/srv/sandflow
work="$root/.work"
archive="$work/web-qa-$release.tar.gz"
target="$root/web-releases/$release"
main=/etc/nginx/snippets/sandflow-online.conf
qa=/etc/nginx/snippets/sandflow-qa.conf
candidate="$work/sandflow-qa.nginx.conf"
printf '%s  %s\n' "$archive_sha" "$archive" | sha256sum --check --status
printf '%s  %s\n' "$before_sha" "$main" | sha256sum --check --status
printf '%s  %s\n' "$qa_sha" "$candidate" | sha256sum --check --status
grep -Fq "$target/" "$candidate" || exit 3
if [[ ! -d "$target" ]]; then
    install -d -m 755 "$target"
    tar -xzf "$archive" --no-same-owner --no-same-permissions -C "$target"
    printf '%s\n' "$archive_sha" > "$target/.archive-sha256"
    printf '%s\n' "$release" > "$target/SOURCE_COMMIT"
else
    [[ -f "$target/.archive-sha256" && "$(cat "$target/.archive-sha256")" == "$archive_sha" ]] || exit 4
fi
[[ -f "$target/index.html" ]] || exit 5
# Hash-named Unity outputs avoid stale data-cache objects across alias switches.
for suffix in 'loader.js' 'framework.js.unityweb' 'wasm.unityweb' 'data.unityweb'; do
    escaped_suffix="${suffix//./\\.}"
    mapfile -t files < <(grep -oE "[a-f0-9]{32}\\.$escaped_suffix" "$target/index.html" | sort -u)
    [[ "${#files[@]}" == 1 && -f "$target/Build/${files[0]}" ]] || exit 5
done
grep -Fq 'routeBase: "/sandflow/qa/"' "$target/index.html" || exit 5
grep -Fq 'mode: "live"' "$target/index.html" || exit 5
preflight="$work/web-qa-$release.preflight.conf"
printf 'pid %s/nginx-qa-preflight.pid;\nerror_log %s/nginx-qa-preflight.log warn;\nevents { worker_connections 16; }\nhttp { include /etc/nginx/mime.types; access_log off; server { listen 127.0.0.1:18270; include %s; } }\n' "$work" "$work" "$candidate" > "$preflight"
/usr/sbin/nginx -t -c "$preflight"
backup="$work/web-qa-$release.main.backup.conf"
cp -p "$main" "$backup"
had_qa=false
if [[ -f "$qa" ]]; then cp -p "$qa" "$work/web-qa-$release.qa.backup.conf"; had_qa=true; fi
cp -p "$main" "$work/web-qa-$release.main.next.conf"
if ! grep -Fq 'include /etc/nginx/snippets/sandflow-qa.conf;' "$main"; then
    printf '\ninclude /etc/nginx/snippets/sandflow-qa.conf;\n' >> "$work/web-qa-$release.main.next.conf"
fi
install -m 644 "$candidate" "$qa"
install -m 644 "$work/web-qa-$release.main.next.conf" "$main"
installed_main_sha="$(sha256sum "$main" | cut -d ' ' -f 1)"
if ! /usr/sbin/nginx -t || ! systemctl reload nginx; then
    printf '%s  %s\n' "$installed_main_sha" "$main" | sha256sum --check --status
    printf '%s  %s\n' "$qa_sha" "$qa" | sha256sum --check --status
    cp -p "$backup" "$main"
    if [[ "$had_qa" == true ]]; then cp -p "$work/web-qa-$release.qa.backup.conf" "$qa"; fi
    /usr/sbin/nginx -t && systemctl reload nginx
    exit 6
fi
printf 'QA static candidate installed: %s\n' "$release"
sha256sum "$main" "$qa"
