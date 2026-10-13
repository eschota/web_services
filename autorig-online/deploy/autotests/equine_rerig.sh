#!/bin/bash
# Re-rig the equine set as new versions through the rerig_open worker (one request per task and rig version, the file
# is the lock), then report each run's rig version, clip count and rig_check. usage: equine_rerig.sh [task ids...]
cd /srv/autorig/data/motion_transfer
V=$(python3 -c "import re;print(re.search(r'^RIG_VERSION = \"([^\"]+)\"', open('mt/rig_version.py').read(), re.M).group(1))")
TASKS=${@:-d3d37033-08d2-4fd2-94e7-69895481aba4 31cc3ff3-1526-41e2-bcf4-aaac56c38015 8a67fa01-434b-47fe-bb77-5fe4aa7d69db 7fd6fef6-9b33-4d22-bcd3-0ed571c96d99 a742491a-981e-43aa-bfcb-3b465be8ff91 ebc474ba-fb5c-4d3e-b0a6-4fa15c655fb4 74f7cd37-1350-4ac2-985d-885503589e17}
echo "rig version $V"
for t in $TASKS; do
  r=$(grep -l "\"$t\"" runs/*/run.json 2>/dev/null | tail -1 | cut -d/ -f2)
  [ -z "$r" ] && r=$(grep -l "${t:0:8}" runs/*/run.json 2>/dev/null | tail -1 | cut -d/ -f2)
  sudo -u autorig python3 - "$t" "$V" "$r" <<PY
import json,os,time,sys
tid,ver,run=sys.argv[1],sys.argv[2],sys.argv[3]
p=f"rerig_requests/{tid}__{ver}.json"
if os.path.exists(p):
    d=json.load(open(p)); print("exists", tid[:8], d.get("state"))
    if d.get("state")=="done": os.rename(p, p+f".fable{int(time.time())}")   # a second pass at the same version: new library
    else: sys.exit(0)
fd=os.open(p, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o644)
os.write(fd, json.dumps({"schema":"autorig.rerig-request/1","task":tid,"from_version":"equine-library","run":run,"pipeline":"v3","task_status":"done","state":"queued","at":time.time(),"by":"fable-equines","note":"equine reference library (gait gate) + head_own"}, indent=1).encode()); os.close(fd); print("queued", tid[:8], run)
PY
done
echo "waiting $(date -u +%H:%M)"
for i in $(seq 1 120); do
  left=0
  for t in $TASKS; do s=$(python3 -c "import json;print(json.load(open('rerig_requests/${t}__${V}.json')).get('state'))" 2>/dev/null); case "$s" in done|error|failed) ;; *) left=$((left+1));; esac; done
  [ $left -eq 0 ] && break; sleep 30
done
for t in $TASKS; do
  python3 - "$t" "$V" <<PY
import json,sys,glob
tid,ver=sys.argv[1],sys.argv[2]
d=json.load(open(f"rerig_requests/{tid}__{ver}.json")); run=d.get("run")
try:
    r=json.load(open(f"runs/{run}/rig/rig.json")); q=json.load(open(f"runs/{run}/rig/numeric-qa.json")) if glob.glob(f"runs/{run}/rig/numeric-qa.json") else {}
    c=q.get("clips") or {}
    print(tid[:8], d.get("state"), d.get("version"), "rig_check 2x/4x", d.get("rig_check_before_2x_4x"), "->", d.get("rig_check_after_2x_4x"), "clips", r.get("clips"), "QA", sum(1 for v in c.values() if v.get("numeric_deformation_passed")), "/", len(c))
except Exception as e: print(tid[:8], d.get("state"), "ERR", e)
PY
done
echo "EQUINE_RERIG_FINAL $(date -u +%H:%M)"
