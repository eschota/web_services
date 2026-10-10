"""Rig-speed probe for one converter over its VPS tunnel (run on the VPS; Converter speed · V3, 2026-10-10).

    python3 rig_speed_probe.py f1 15132 [input_url] [--keep-temp-cmd "<shell command with {guid}>"]

Submits an only_rig conversion straight to the node (the backend never sees it), polls the status every
2 s and prints one JSON line: seconds from submit to pickup, to viewer_rig_ready (both viewer GLBs
complete, the "rig in the viewer" point of the owner's one-minute rule) and to Completed, plus the
per-stage durations of logs/stage_timing.jsonl. The default input is the 66ba97ba evidence model
(read only). --keep-temp-cmd runs once the rig is ready (e.g. to drop SAVETEMP.txt for a diagnosis).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request

DEFAULT_INPUT = "https://autorig.online/u/8f8a45e1-9a39-41e1-bf44-b39a2769fe90/tripo_female_warrior_rigged.glb"


def _get(url: str, timeout: float = 20.0):
    return json.loads(urllib.request.urlopen(url, timeout=timeout).read())


def main() -> int:
    args = sys.argv[1:]
    keep_cmd = None
    if "--keep-temp-cmd" in args:
        i = args.index("--keep-temp-cmd")
        keep_cmd = args[i + 1]
        del args[i:i + 2]
    box, port = args[0], int(args[1])
    input_url = args[2] if len(args) > 2 else DEFAULT_INPUT
    base = f"http://127.0.0.1:{port}"
    payload = {"input_url": input_url, "type": "t_pose", "mode": "only_rig",
               "queue_class": "interactive", "workload_class": "autorig_interactive"}
    req = urllib.request.Request(base + "/api-converter-glb", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.time()
    body = json.loads(urllib.request.urlopen(req, timeout=60).read())
    task_id = body.get("task_id")
    guid = str(body.get("progress_page") or "").rstrip("/").rsplit("/", 2)[-2]
    out = {"box": box, "task_id": task_id, "guid": guid, "progress_page": body.get("progress_page"), "input_url": input_url}
    marks = {}
    final = {}
    while time.time() - t0 < 1800:
        try:
            final = _get(f"{base}/api-converter-glb/status/{task_id}")
        except Exception:
            time.sleep(2)
            continue
        st = str(final.get("status"))
        if "pickup" not in marks and st not in ("Queued", "Pending", "queued", "pending"):
            marks["pickup"] = round(time.time() - t0, 1)
        if "viewer_rig_ready" not in marks and final.get("viewer_rig_ready"):
            marks["viewer_rig_ready"] = round(time.time() - t0, 1)
            if keep_cmd:
                subprocess.run(keep_cmd.format(guid=guid), shell=True, timeout=60)
        if st in ("Completed", "Done", "Failed", "Error"):
            marks["terminal"] = round(time.time() - t0, 1)
            break
        time.sleep(2)
    out["status"] = final.get("status")
    out["error"] = (final.get("error") or "")[:200]
    out["marks_s"] = marks
    stages = {}
    first_start = None
    try:
        raw = urllib.request.urlopen(f"{base}/converter/glb/{guid}/logs/stage_timing.jsonl", timeout=30).read()
        for line in raw.decode("utf-8", "replace").splitlines():
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if ev.get("event") == "start" and first_start is None:
                first_start = ev.get("started_at_unix")
            if ev.get("event") == "finish" and ev.get("duration_s") is not None:
                key = ev.get("stage")
                stages[key] = round(stages.get(key, 0.0) + float(ev["duration_s"]), 1)
    except Exception as exc:
        out["stage_timing_error"] = str(exc)[:120]
    out["stages_s"] = stages
    rig_at = final.get("viewer_rig_ready_at")
    if rig_at and first_start:
        out["pipeline_start_to_rig_ready_s"] = round(float(rig_at) - float(first_start), 1)
    print(json.dumps(out, ensure_ascii=False))
    return 0 if str(final.get("status")) in ("Completed", "Done") else 1


if __name__ == "__main__":
    sys.exit(main())
