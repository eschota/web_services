"""Legacy only_rig canary through a converter's VPS tunnel (run on the VPS).

    python3 rig_canary.py f13 15267 [timeout_seconds]

Submits the site's public default T-pose GLB as an only_rig conversion straight
to the node (the backend never sees it), waits for a terminal state and checks
that the eight deliverables are served through the same tunnel. It also makes
the converter run its lazy asset preflight, so the node counts as healthy.
Prints one JSON line; exit code 0 only when every check passed.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

INPUT = "https://autorig.online/static/glb/default_t_pose.glb"
EXPECTED_SUFFIXES = ("_model_prepared.glb", "_video_poster.jpg", "_video.mp4", "_hdrp.unitypackage",
                     "_all_animations_unity.fbx", "_all_animations.blend", "_model_prepared_rigged.blend")


def main() -> int:
    box, port = sys.argv[1], int(sys.argv[2])
    limit = float(sys.argv[3]) if len(sys.argv) > 3 else 1200.0
    base = f"http://127.0.0.1:{port}"
    payload = {"input_url": INPUT, "type": "t_pose", "mode": "only_rig",
               "queue_class": "interactive", "workload_class": "autorig_interactive"}
    request = urllib.request.Request(base + "/api-converter-glb", data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    started = time.time()
    body = json.loads(urllib.request.urlopen(request, timeout=60).read())
    task_id = body.get("task_id")
    result = {"box": box, "task_id": task_id, "progress_page": body.get("progress_page")}
    final = {}
    while time.time() - started < limit:
        try:
            final = json.loads(urllib.request.urlopen(f"{base}/api-converter-glb/status/{task_id}", timeout=20).read())
        except Exception:
            time.sleep(5)
            continue
        if str(final.get("status")) in ("Completed", "Done", "Failed", "Error"):
            break
        time.sleep(5)
    result["status"] = final.get("status")
    result["error"] = (final.get("error") or "")[:300]
    result["seconds"] = round(time.time() - started, 1)
    outputs = final.get("output_urls") or []
    result["outputs"] = len(outputs)
    served = {}
    for url in outputs:
        name = url.rsplit("/", 1)[-1]
        guid = url.rstrip("/").rsplit("/", 2)[-2]
        local = f"{base}/converter/glb/{guid}/{name}"
        try:
            req = urllib.request.Request(local, headers={"Range": "bytes=0-1023"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                served[name] = resp.status
        except urllib.error.HTTPError as exc:
            served[name] = exc.code
        except Exception as exc:
            served[name] = type(exc).__name__
    result["served"] = served
    missing = [s for s in EXPECTED_SUFFIXES if not any(n.endswith(s) for n in served)]
    result["missing"] = missing
    result["ok"] = bool(result["status"] in ("Completed", "Done") and not missing
                        and all(v in (200, 206) for k, v in served.items() if not k.endswith(".zip")))
    try:
        status = json.loads(urllib.request.urlopen(f"{base}/api-converter-glb/server-status", timeout=30).read())
        preflight = status.get("asset_preflight") or {}
        result["preflight"] = preflight.get("status") or ("healthy" if preflight.get("healthy") else "unknown")
        result["preflight_healthy"] = preflight.get("healthy")
        result["deploy_commit"] = status.get("deploy_commit")
    except Exception as exc:
        result["preflight_error"] = str(exc)[:120]
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["ok"] and result.get("preflight_healthy") is not False else 1


if __name__ == "__main__":
    sys.exit(main())
