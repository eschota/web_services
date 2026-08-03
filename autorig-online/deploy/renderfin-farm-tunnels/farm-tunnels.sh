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
# How long a tunnel must survive before we believe it was ever established.
# A box whose key is not authorised fails in well under a second.
SETTLE_SECONDS="${FARM_TUNNELS_SETTLE_SECONDS:-3}"

[[ -f "$CONF" ]] || { echo "missing $CONF" >&2; exit 1; }
[[ -f "$KEY" ]] || { echo "missing key $KEY" >&2; exit 1; }

pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

start_tunnel() {
  ssh -N \
      -i "$KEY" \
      -o BatchMode=yes \
      -o StrictHostKeyChecking=accept-new \
      -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=30 \
      -o ServerAliveCountMax=3 \
      -p "$1" \
      -L "${2}:127.0.0.1:${3}" \
      "${USER_NAME}@${GATEWAY}" &
  echo "$!"
}

names=()
while read -r name ssh_port local_port remote_port; do
  [[ -z "${name:-}" || "${name:0:1}" == "#" ]] && continue
  echo "tunnel ${name}: 127.0.0.1:${local_port} -> ${GATEWAY}:${ssh_port} -> 127.0.0.1:${remote_port}"
  pid="$(start_tunnel "$ssh_port" "$local_port" "$remote_port")"
  pids+=("$pid")
  names+=("$name")
done < "$CONF"

[[ ${#pids[@]} -gt 0 ]] || { echo "no tunnels configured" >&2; exit 1; }

# A box we cannot reach at all must not take the working tunnels down with it.
# Adding one unauthorised host used to kill every other tunnel on the next
# restart loop, which is a much worse outage than the box we were adding.
sleep "$SETTLE_SECONDS"
alive_pids=()
alive_names=()
for i in "${!pids[@]}"; do
  if kill -0 "${pids[$i]}" 2>/dev/null; then
    alive_pids+=("${pids[$i]}")
    alive_names+=("${names[$i]}")
  else
    echo "tunnel ${names[$i]} could not be established; continuing without it" >&2
  fi
done

if [[ ${#alive_pids[@]} -eq 0 ]]; then
  echo "no tunnel could be established" >&2
  exit 1
fi
echo "tunnels up: ${alive_names[*]}"
pids=("${alive_pids[@]}")

# A tunnel that WAS working and then dropped is a different matter: the set is
# restarted so it reconnects.
#
# Polled rather than `wait -n`, which waits on EVERY background job including
# the ones we just decided to live without — it returns their long-dead exit
# status immediately and restarts the world in a loop.
while true; do
  sleep 15
  for i in "${!pids[@]}"; do
    if ! kill -0 "${pids[$i]}" 2>/dev/null; then
      echo "established tunnel ${alive_names[$i]} exited; restarting all" >&2
      exit 1
    fi
  done
done
