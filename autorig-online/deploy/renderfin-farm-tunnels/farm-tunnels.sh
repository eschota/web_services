#!/usr/bin/env bash
# Hold SSH tunnels from the VPS to farm converter boxes so renderfin can call
# their Hunyuan3D API with an intact Authorization header.
#
# The public https facades (converter-fN.freestock.online) do NOT forward the
# Authorization header, so bearer-authenticated endpoints must be reached over
# a tunnel to the box's own loopback port.
#
# Config: /etc/autorig-farm-tunnels.conf, one tunnel per line:
#   <name> <ssh_port> <local_port> <remote_port>
# e.g.
#   f7  45131 15131 5131
#   f13 45267 15267 5267
set -uo pipefail

CONF="${FARM_TUNNELS_CONF:-/etc/autorig-farm-tunnels.conf}"
KEY="${FARM_TUNNELS_KEY:-/root/.ssh/renderfin_farm_tunnel}"
GATEWAY="${FARM_TUNNELS_GATEWAY:-5.129.157.224}"
USER_NAME="${FARM_TUNNELS_USER:-user}"

[[ -f "$CONF" ]] || { echo "missing $CONF" >&2; exit 1; }
[[ -f "$KEY" ]] || { echo "missing key $KEY" >&2; exit 1; }

pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

while read -r name ssh_port local_port remote_port; do
  [[ -z "${name:-}" || "${name:0:1}" == "#" ]] && continue
  echo "tunnel ${name}: 127.0.0.1:${local_port} -> ${GATEWAY}:${ssh_port} -> 127.0.0.1:${remote_port}"
  ssh -N \
      -i "$KEY" \
      -o BatchMode=yes \
      -o StrictHostKeyChecking=accept-new \
      -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=30 \
      -o ServerAliveCountMax=3 \
      -p "$ssh_port" \
      -L "${local_port}:127.0.0.1:${remote_port}" \
      "${USER_NAME}@${GATEWAY}" &
  pids+=("$!")
done < "$CONF"

[[ ${#pids[@]} -gt 0 ]] || { echo "no tunnels configured" >&2; exit 1; }

# If any tunnel dies, exit so systemd restarts the whole set.
wait -n
echo "a tunnel exited; restarting all" >&2
exit 1
