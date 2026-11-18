#!/usr/bin/env bash
set -euo pipefail

base=/srv/autorig/abris
requirements=${1:?Pass the release requirements.txt path}
[[ "$(id -u)" == 0 ]]
[[ "$(dpkg --print-architecture)" == amd64 ]]
test -f "$requirements"

install -d -m 0755 "$base" "$base/deploy" "$base/releases" "$base/runtime"
exec 9>"$base/deploy/bootstrap.lock"
flock -x 9

if ! getent passwd autorig-abris >/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin autorig-abris
fi
if ! getent passwd autorig-abris-training >/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin autorig-abris-training
fi
install -d -o autorig-abris -g autorig-abris -m 0700 "$base/tmp"
install -d -m 0755 "$base/training"
install -d -o root -g autorig-abris-training -m 0750 "$base/training/corrections"
install -d -o autorig-abris-training -g autorig-abris -m 2750 "$base/training/models"
install -d -m 0700 "$base/runtime/build-tmp" "$base/runtime/debs"
export TMPDIR="$base/runtime/build-tmp"
export PIP_DISABLE_PIP_VERSION_CHECK=1

if [[ ! -x "$base/runtime/tesseract/usr/bin/tesseract" ]]; then
    cd "$base/runtime/debs"
    apt-get -o APT::Sandbox::User=root download \
        libgif7 liblept5 libtesseract5 tesseract-ocr tesseract-ocr-eng tesseract-ocr-osd tesseract-ocr-rus
    staged_ocr=$(mktemp -d "$base/runtime/tesseract.XXXXXXXX")
    chmod 0755 "$staged_ocr"
    for package in ./*.deb; do
        dpkg-deb -x "$package" "$staged_ocr"
    done
    mv -T "$staged_ocr" "$base/runtime/tesseract"
fi

ocr="$base/runtime/tesseract"
languages=$(LD_LIBRARY_PATH="$ocr/usr/lib/x86_64-linux-gnu" TESSDATA_PREFIX="$ocr/usr/share/tesseract-ocr/5/tessdata" \
    "$ocr/usr/bin/tesseract" --list-langs)
grep -qx eng <<<"$languages"
grep -qx rus <<<"$languages"

# Lock each environment to the complete declared dependency set. A dependency
# update stages a new environment without changing the serving release.
digest=$(tr -d '\r' < "$requirements" | sha256sum | cut -d ' ' -f 1)
env_dir="$base/runtime/venv-${digest:0:16}"
if [[ ! -e "$env_dir/.ready" ]]; then
    python3 -m venv "$env_dir"
    "$env_dir/bin/python" -m pip install --only-binary=:all: --no-cache-dir 'gunicorn==23.0.0' -r "$requirements" >&2
    "$env_dir/bin/python" -m pip check >&2
    "$env_dir/bin/python" -c 'import cv2, numpy, PIL, pypdfium2'
    touch "$env_dir/.ready"
fi
printf '%s\n' "$env_dir"
