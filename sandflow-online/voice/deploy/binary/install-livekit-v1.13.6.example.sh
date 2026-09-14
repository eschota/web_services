#!/usr/bin/env bash
set -euo pipefail

# Source-only installation template. Review before running as root on the target host.
version='1.13.6'
archive="livekit_${version}_linux_amd64.tar.gz"
expected_sha256='2b61abef2b9ba14b4b8ca38b37de9a37ffc682b9931d5fc03ceca2f0b77d3e33'
release_url="https://github.com/livekit/livekit/releases/download/v${version}/${archive}"
service_root='/srv/sandflow/voice'
source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
staging_dir="${service_root}/.staging/livekit-${version}"
binary_path="${service_root}/bin/livekit-server-v${version}"

if [[ "${EUID}" -ne 0 ]]; then
  echo 'Run as root after reviewing this template.' >&2
  exit 1
fi

getent passwd sandflow-voice >/dev/null
getent group sandflow-voice >/dev/null
install -d -o root -g root -m 0755 "${service_root}" "${service_root}/bin"
# Shared namespace parents contain no secret material and must remain traversable by
# the separate LiveKit and coturn users. Never place runtime files directly in them.
install -d -o root -g root -m 0755 "${service_root}/config" "${service_root}/certs"
install -d -o root -g sandflow-voice -m 0750 \
  "${service_root}/config/livekit" "${service_root}/certs/livekit"
install -d -o sandflow-voice -g sandflow-voice -m 0750 "${service_root}/logs"
install -d -o sandflow-voice -g sandflow-voice -m 0700 "${service_root}/secrets"
install -d -o sandflow-voice -g sandflow-voice -m 0700 \
  "${service_root}/.work/tmp/livekit" "${service_root}/.work/run/livekit" "${service_root}/.work/logs/livekit"
install -d -o root -g root -m 0700 "${staging_dir}"

curl --fail --location --proto '=https' --tlsv1.2 \
  --output "${staging_dir}/${archive}" "${release_url}"
printf '%s  %s\n' "${expected_sha256}" "${staging_dir}/${archive}" | sha256sum --check --strict

if tar --list --gzip --file "${staging_dir}/${archive}" | grep -Eq '(^/|(^|/)\.\.(/|$))'; then
  echo 'Archive contains an absolute or traversal path.' >&2
  exit 1
fi

tar --extract --gzip --file "${staging_dir}/${archive}" --directory "${staging_dir}" \
  --no-same-owner --no-same-permissions
test -f "${staging_dir}/livekit-server"
install -o root -g root -m 0755 "${staging_dir}/livekit-server" "${binary_path}"
install -o root -g root -m 0755 "${source_dir}/run-livekit.example.sh" \
  "${service_root}/bin/run-livekit"

actual_sha256="$(sha256sum "${staging_dir}/${archive}" | awk '{print $1}')"
test "${actual_sha256}" = "${expected_sha256}"
"${binary_path}" --version

echo "Installed pinned binary at ${binary_path}."
echo 'No service was installed or started by this source template until the operator runs it.'
