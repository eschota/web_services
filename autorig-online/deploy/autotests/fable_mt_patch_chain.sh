#!/bin/bash
# Fable · monsters/animals chain template (2026-10-13): shared MT files go through --mt-patch, never --mt-file.
# usage: vps_deploy_patch.sh <tag> <rel>=<patch_script.py> [<rel>=<patch_script.py> ...]
#   each patch script is `python3 <script> <dir>` editing <dir>/<rel> (my patch_*.py convention);
#   it runs on a copy of CURRENT prod mt/<rel>, the result is diffed, and mt-deploy applies the diff
#   onto prod under the gate lock, so a stale candidate cannot happen.
set -e; set -o pipefail
D=/home/debian/monster; A=/srv/autorig/autotests; MT=/srv/autorig/data/motion_transfer/mt
TAG=$1; shift
ST=$D/${TAG}_$(date -u +%H%M%S); mkdir -p $ST/base $ST/new
ARGS=()
for spec in "$@"; do
  rel=${spec%%=*}; script=${spec#*=}
  cp $MT/$rel $ST/base/$rel; cp $MT/$rel $ST/new/$rel
  /srv/autorig/venv/bin/python3 $script $ST/new
  /srv/autorig/venv/bin/python3 -c "import sys; s=open('$ST/new/$rel','rb').read(); assert s.count(bytes([8]))==0, 'backspace bytes in $rel'"
  /srv/autorig/venv/bin/python3 -m py_compile $ST/new/$rel
  diff -u $ST/base/$rel $ST/new/$rel > $ST/$rel.diff || true
  [ -s $ST/$rel.diff ] || { echo "no change for $rel"; exit 1; }
  ARGS+=(--mt-patch mt/$rel=$ST/$rel.diff)
done
echo "START $TAG $(date -u +%H:%M) ${ARGS[*]}"
sudo $A/autotests.py mt-deploy --jobs 2 "${ARGS[@]}" 2>&1 | tee $D/${TAG}_deploy.log | grep -E "GATE|FAIL|XPASS|ERROR|BLOCKS|report:|installed|NOT INSTALLED|does not apply" | head -40 || true
echo "FINAL $TAG $(date -u +%H:%M)"
