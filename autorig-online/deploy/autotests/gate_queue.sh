#!/bin/bash
# One release gate at a time on this VPS (lead, 2026-10-20, after two thrash incidents: 10:44 and 11:20 UTC).
# Usage: sudo /srv/autorig/autotests/gate_queue.sh <agent name> -- <the mt-deploy / gate command and its args>
# Holds /run/lock/autorig-gate.lock for the whole run, then waits until the 1-minute load is under $GATE_MAX_LOAD
# (default 12) and no mt-deploy / gate process exists, rechecked right before the launch. Runs the command at nice 19.
set -u
NAME=${1:-anon}; shift; [ "${1:-}" = "--" ] && shift
MAXLOAD=${GATE_MAX_LOAD:-12}
LOCK=/run/lock/autorig-gate.lock
exec 9>"$LOCK"
echo "$(date -u +%FT%TZ) $NAME: waiting for the gate lock" >&2
flock 9
echo "$(date -u +%FT%TZ) $NAME: lock taken" >&2
while :; do
  load=$(cut -d" " -f1 /proc/loadavg); busy=$(pgrep -f "autotests.py (mt-deploy|gate)" | grep -v "^$$\$" | wc -l)
  if [ "$busy" = 0 ] && awk -v l="$load" -v m="$MAXLOAD" "BEGIN{exit !(l<m)}"; then
    sleep 20
    load=$(cut -d" " -f1 /proc/loadavg); busy=$(pgrep -f "autotests.py (mt-deploy|gate)" | grep -v "^$$\$" | wc -l)
    [ "$busy" = 0 ] && awk -v l="$load" -v m="$MAXLOAD" "BEGIN{exit !(l<m)}" && break
  fi
  sleep 30
done
echo "$(date -u +%FT%TZ) $NAME: launching (load $load): $*" >&2
echo "$(date -u +%FT%TZ) $NAME: $*" >> /var/log/autorig-gate-queue.log
nice -n 19 "$@"; rc=$?
echo "$(date -u +%FT%TZ) $NAME: done rc=$rc" | tee -a /var/log/autorig-gate-queue.log >&2
exit $rc
