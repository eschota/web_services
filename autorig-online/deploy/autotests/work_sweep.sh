#!/bin/bash
# Release-gate scratch retention (2026-10-17): every mt-deploy gate leaves ~2 GB of rigged copies under work/ and
# nothing removed them (~6 GB/h on a busy day). Reports stay in reports/; a work dir is removed once no running gate
# names it and nothing inside changed for AUTOTESTS_WORK_KEEP_MIN minutes (default 90).
set -u
WORK=/srv/autorig/data/autotests/work
KEEP=${AUTOTESTS_WORK_KEEP_MIN:-90}
RUNNING=$(ps -eo args | grep -o "autotests/work/[0-9TZ]*-[a-z-]*" | sort -u)
cd "$WORK" 2>/dev/null || exit 0
for d in */; do
  d=${d%/}
  [[ "$d" =~ ^[0-9]{8}T[0-9]{6}Z- ]] || continue
  echo "$RUNNING" | grep -qx "autotests/work/$d" && continue
  [ -n "$(find "$d" -newermt "-${KEEP} minutes" -print -quit 2>/dev/null)" ] && continue
  size=$(du -sm "$d" 2>/dev/null | cut -f1)
  rm -rf --one-file-system "$d" && echo "removed $d ${size} MB"
done
