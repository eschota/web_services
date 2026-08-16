#!/usr/bin/env bash
# Hold SSH tunnels from the VPS to farm converter boxes so renderfin can call
# their Hunyuan3D API with an intact Authorization header.
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
CONNECT_TIMEOUT_SECONDS="${FARM_TUNNELS_CONNECT_TIMEOUT_SECONDS:-10}"
RETRY_INITIAL_SECONDS="${FARM_TUNNELS_RETRY_INITIAL_SECONDS:-5}"
RETRY_MAX_SECONDS="${FARM_TUNNELS_RETRY_MAX_SECONDS:-60}"
STABLE_SECONDS="${FARM_TUNNELS_STABLE_SECONDS:-300}"

[[ -f "$CONF" ]] || { echo "missing $CONF" >&2; exit 1; }
[[ -f "$KEY" ]] || { echo "missing key $KEY" >&2; exit 1; }

supervisor_pids=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${supervisor_pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

tunnel_loop() {
  local name="$1"
  local ssh_port="$2"
  local local_port="$3"
  local remote_port="$4"
  local delay="$RETRY_INITIAL_SECONDS"
  local started_at=0
  local lived=0
  local rc=0

  while true; do
    echo "tunnel ${name}: 127.0.0.1:${local_port} -> ${GATEWAY}:${ssh_port} -> 127.0.0.1:${remote_port}"
    started_at="$(date +%s)"
    ssh -N \
        -i "$KEY" \
        -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new \
        -o ExitOnForwardFailure=yes \
        -o ConnectTimeout="$CONNECT_TIMEOUT_SECONDS" \
        -o ConnectionAttempts=1 \
        -o ServerAliveInterval=30 \
        -o ServerAliveCountMax=3 \
        -p "$ssh_port" \
        -L "127.0.0.1:${local_port}:127.0.0.1:${remote_port}" \
        "${USER_NAME}@${GATEWAY}"
    rc=$?
    lived=$(( $(date +%s) - started_at ))
    if (( lived >= STABLE_SECONDS )); then
      delay="$RETRY_INITIAL_SECONDS"
    fi
    echo "tunnel ${name} exited rc=${rc} after ${lived}s; reconnecting only this tunnel in ${delay}s" >&2
    sleep "$delay"
    if (( delay < RETRY_MAX_SECONDS )); then
      delay=$(( delay * 2 ))
      if (( delay > RETRY_MAX_SECONDS )); then
        delay="$RETRY_MAX_SECONDS"
      fi
    fi
  done
}

names=()
while read -r name ssh_port local_port remote_port; do
  [[ -z "${name:-}" || "${name:0:1}" == "#" ]] && continue
  tunnel_loop "$name" "$ssh_port" "$local_port" "$remote_port" &
  supervisor_pids+=("$!")
  names+=("$name")
done < "$CONF"

[[ ${#supervisor_pids[@]} -gt 0 ]] || { echo "no tunnels configured" >&2; exit 1; }
echo "independent tunnel supervisors started: ${names[*]}"

# Each child reconnects only its own endpoint. One flaky farm box must never
# tear down healthy tunnels to the rest of the fleet.
wait
