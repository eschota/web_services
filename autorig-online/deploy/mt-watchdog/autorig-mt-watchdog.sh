#!/bin/bash
# autorig-mt watchdog (2026-10-11): twice today the MT event loop hung inside asyncio.create_subprocess_exec
# (v3_conveyor) while the port stayed open, so intake stood still. Every minute: if /api/mt/rig-version does not
# answer within 20 s three checks in a row, restart autorig-mt (runs and branches resume by themselves).
S=/run/autorig-mt-watchdog.fails
code=$(curl -s -m 20 -o /dev/null -w "%{http_code}" http://127.0.0.1:8251/api/mt/rig-version)
if [ "$code" = 200 ]; then echo 0 > $S; exit 0; fi
n=$(( $(cat $S 2>/dev/null || echo 0) + 1 )); echo $n > $S
[ "$(systemctl is-active autorig-mt)" = active ] || exit 0          # stopped on purpose / starting: not ours
up=$(( $(date +%s) - $(date -d "$(systemctl show autorig-mt -p ExecMainStartTimestamp --value)" +%s) ))
[ $up -lt 180 ] && exit 0                                           # still starting
if [ $n -ge 3 ]; then
  echo "$(date -u +%FT%TZ) watchdog: /api/mt/rig-version failed $n times (last $code), restarting autorig-mt" >> /var/log/autorig-mt-restart.log
  echo 0 > $S
  systemctl restart autorig-mt
fi
