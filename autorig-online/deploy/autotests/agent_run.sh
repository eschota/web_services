#!/bin/bash
# Agent heavy work on the VPS, customer tasks first (owner 2026-10-20: «делай стабильно, так много параллельно не надо,
# приоритет все равно должен отдаваться авториг задачам»).
# Usage: sudo /srv/autorig/autotests/agent_run.sh <agent name> -- <command and args>
# One agent run at a time (flock), launched only when: no AutoRig customer task is waiting (fleet queues: created = 0,
# processing <= AGENT_MAX_PROCESSING [20]), the 1-minute load is under AGENT_MAX_LOAD [10], no release gate runs;
# rechecked 20 s later. Runs as user autorig at nice 19 in a systemd scope capped at AGENT_MEM [5G], CPUWeight 20.
set -u
NAME=${1:-anon}; shift; [ "${1:-}" = "--" ] && shift
MAXLOAD=${AGENT_MAX_LOAD:-10}; MAXPROC=${AGENT_MAX_PROCESSING:-20}   # processing is ~12 in steady state; created = 0 is the real «no customer waits»
MEM=${AGENT_MEM:-5G}
LOG=/var/log/autorig-agent-queue.log
exec 9>/run/lock/autorig-agent.lock
echo "$(date -u +%FT%TZ) $NAME: waiting for the agent lock" >&2
flock 9
quiet() {
  local load q created processing busy
  load=$(cut -d" " -f1 /proc/loadavg)
  q=$(curl -s -m 5 http://127.0.0.1:8255/api/fleet | python3 -c 'import json,sys
d=json.load(sys.stdin); a=d.get("queues_object",{}).get("autorig_tasks_object") or {}
print(a.get("created_int", a.get("created", 0)), a.get("processing_int", a.get("processing", 0)))' 2>/dev/null || echo "0 0")
  created=${q%% *}; processing=${q##* }
  busy=$(pgrep -f "python3 (-P )?/srv/autorig/autotests/autotests.py (mt-deploy|gate)" | wc -l)
  [ "$busy" = 0 ] && [ "${created:-0}" = 0 ] && [ "${processing:-0}" -le "$MAXPROC" ] && awk -v l="$load" -v m="$MAXLOAD" 'BEGIN{exit !(l<m)}'
}
n=0
while :; do
  if quiet; then sleep 20; quiet && break; fi
  n=$((n+1)); [ $((n % 10)) = 1 ] && echo "$(date -u +%FT%TZ) $NAME: waiting (load $(cut -d" " -f1 /proc/loadavg), customer queue busy or a gate runs)" >&2
  sleep 30
done
echo "$(date -u +%FT%TZ) $NAME: launching: $*" | tee -a "$LOG" >&2
systemd-run --scope -q -p MemoryMax=$MEM -p CPUWeight=20 --uid=autorig --gid=autorig nice -n 19 "$@" 9>&-; rc=$?
echo "$(date -u +%FT%TZ) $NAME: done rc=$rc" | tee -a "$LOG" >&2
exit $rc
