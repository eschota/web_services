#!/bin/bash
# One release gate at a time on this VPS (lead, 2026-10-20, after two thrash incidents: 10:44 and 11:20 UTC).
# Usage: sudo /srv/autorig/autotests/gate_queue.sh <agent name> -- <the mt-deploy / gate command and its args>
# Holds /run/lock/autorig-gate.lock for the whole run, then waits until the 1-minute load is under $GATE_MAX_LOAD
# (default 12) and no real gate runner process exists (the python of autotests.py, not the waiting queue entries
# whose argv carries the same words), rechecked 20 s later right before the launch. Runs the command at nice 19 with
# AUTORIG_GATE_PARALLEL=1, because autotests.py takes this same lock file itself and would deadlock behind the queue.
set -u
NAME=${1:-anon}; shift; [ "${1:-}" = "--" ] && shift
MAXLOAD=${GATE_MAX_LOAD:-12}
LOCK=/run/lock/autorig-gate.lock
RUNNER='python3 (-P )?/srv/autorig/autotests/autotests.py (mt-deploy|gate)'
exec 9>"$LOCK"
echo "$(date -u +%FT%TZ) $NAME: waiting for the gate lock" >&2
flock 9
echo "$(date -u +%FT%TZ) $NAME: lock taken" >&2
quiet() { local load busy; load=$(cut -d" " -f1 /proc/loadavg); busy=$(pgrep -f "$RUNNER" | wc -l)
          [ "$busy" = 0 ] && awk -v l="$load" -v m="$MAXLOAD" 'BEGIN{exit !(l<m)}'; }
while :; do
  if quiet; then sleep 20; quiet && break; fi
  sleep 30
done
load=$(cut -d" " -f1 /proc/loadavg)
echo "$(date -u +%FT%TZ) $NAME: launching (load $load): $*" | tee -a /var/log/autorig-gate-queue.log >&2
export AUTORIG_GATE_PARALLEL=1
nice -n 19 "$@" 9>&-; rc=$?
echo "$(date -u +%FT%TZ) $NAME: done rc=$rc" | tee -a /var/log/autorig-gate-queue.log >&2
exit $rc
