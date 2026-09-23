#!/bin/bash
# Post-cleanup health check, run on autorig-vps.
for bp in f1:15132 f2:15279 f7:15131 f11:15533 f13:15267; do
  b=${bp%%:*}; p=${bp##*:}
  curl -s -m 20 http://127.0.0.1:$p/api-converter-glb/server-status | python3 -c "
import json,sys
b='$b'
try: d=json.load(sys.stdin)
except Exception as e: print(b,'STATUS FAIL',e); sys.exit()
s=d.get('tasks_summary') or {}; a=d.get('ai_models') or {}; si=d.get('system_info') or {}
pf=(d.get('asset_preflight') or {})
print(f\"{b:4} tunnel=ok ver={d.get('server_version')} drift={d.get('deploy_drift')}({d.get('deploy_drift_count')}) proc={s.get('processing')} pend={s.get('pending')} failed={s.get('failed')} preflight={'healthy' if pf.get('healthy') else pf.get('status', pf.get('healthy'))} llm=installed:{a.get('installed')}/enabled:{a.get('enabled')}/running:{a.get('running')} disk%={si.get('disk_percent')} mode={d.get('gpu_mode')}\")
"
done
