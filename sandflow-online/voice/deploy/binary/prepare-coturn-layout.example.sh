#!/usr/bin/env bash
set -euo pipefail

# Layout only. This script does not install coturn, generate a secret, copy a key,
# install a unit, change firewall rules, or start a service.
service_root='/srv/sandflow/voice'

if [[ "${EUID}" -ne 0 ]]; then
  echo 'Run as root after reviewing this template.' >&2
  exit 1
fi

getent passwd sandflow-turn >/dev/null
getent group sandflow-turn >/dev/null
# Reassert only the non-secret shared parents. This is safe before or after the
# LiveKit installer and never changes either service's private child directory.
install -d -o root -g root -m 0755 "${service_root}/config" "${service_root}/certs"
install -d -o root -g sandflow-turn -m 0750 \
  "${service_root}/config/coturn" "${service_root}/certs/coturn"
install -d -o sandflow-turn -g sandflow-turn -m 0700 \
  "${service_root}/.work/tmp/coturn" "${service_root}/.work/run/coturn" \
  "${service_root}/.work/logs/coturn"

echo 'Coturn directories prepared; runtime config, secret and certificate are still required.'
