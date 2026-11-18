#!/usr/bin/env bash
set -euo pipefail

app_root=$(realpath -- "${1:-/srv/autorig/abris}")
version=0.14
source_archive="libredwg-$version.tar.xz"
source_hash=62ebb73b984f865960f20ed26619ea5f8789d5e3fd088fa40a2598384da81275
build_root="$app_root/dwg-build"
prefix="$app_root/tools/libredwg-$version"

if [[ -x "$prefix/bin/dxf2dwg" ]] && [[ $("$prefix/bin/dxf2dwg" --version) == "dxf2dwg $version" ]]; then
    exit 0
fi

umask 077
mkdir -p -- "$build_root"
cd -- "$build_root"
export TMPDIR="$build_root"
if ! command -v pkg-config >/dev/null; then
    # Debian 12 x86_64 has these build dependencies in its signed package index.
    apt-get download libpkgconf3=1.8.1-1 pkgconf=1.8.1-1 pkgconf-bin=1.8.1-1
    mkdir -p -- build-deps
    for package in libpkgconf3 pkgconf pkgconf-bin; do
        dpkg-deb -x "${package}_1.8.1-1_amd64.deb" build-deps
    done
    export PATH="$build_root/build-deps/usr/bin:$PATH"
    export LD_LIBRARY_PATH="$build_root/build-deps/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# Pin the unmodified GPLv3 source from the official LibreDWG release mirror.
curl -fsSL --max-time 120 -o "$source_archive" \
    "https://github.com/LibreDWG/libredwg/releases/download/$version/$source_archive"
printf '%s  %s\n' "$source_hash" "$source_archive" | sha256sum -c -
tar -xJf "$source_archive"
cd -- "libredwg-$version"
CFLAGS='-O0 -g0' ./configure --prefix="$prefix" --disable-shared --enable-static \
    --disable-bindings --disable-docs > "$build_root/configure.log" 2>&1
nice -n 15 make -j3 > "$build_root/make.log" 2>&1
[[ $(programs/dxf2dwg --version) == "dxf2dwg $version" ]]

install -d -m 0755 -- "$prefix" "$prefix/bin"
install -m 0644 -- COPYING "$prefix/COPYING"
install -m 0755 -- programs/dxf2dwg "$prefix/bin/dxf2dwg.new"
mv -fT -- "$prefix/bin/dxf2dwg.new" "$prefix/bin/dxf2dwg"
