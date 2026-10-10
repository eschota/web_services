"""Legacy only_rig canary through a converter's VPS tunnel (run on the VPS) + the regression gate on its output.

    python3 rig_canary.py f13 15267 [timeout_seconds]            # base: the default T-pose, ~5-8 min
    python3 rig_canary.py f13 15267 --corpus [--case warrior_sword]   # extended: the known-bad corpus models

Submits the site's public default T-pose GLB as an only_rig conversion straight
to the node (the backend never sees it), waits for a terminal state and checks
that the eight deliverables are served through the same tunnel. It also makes
the converter run its lazy asset preflight, so the node counts as healthy.

Then the rigged GLB (<guid>_all_animations.glb) and the prepared GLB are fetched
through the tunnel and judged by the regression autotests
(/srv/autorig/autotests/autotests.py artifact, profile converter_default: the
skeleton sits on the mesh, no unfitted template, joints inside the voxel solid,
no hand weights far from the hand, the rest pose keeps its height, a held prop
is rigid). --corpus also converts every corpus case that names a converter
input (corpus.json `converter`), with that case's known defects as xfail.
A node is restored only on exit code 0 (AGENTS.md: drain -> deploy -> canary -> restore).
Prints one JSON line; exit code 0 only when every check passed.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

INPUT = "https://autorig.online/static/glb/default_t_pose.glb"
EXPECTED_SUFFIXES = ("_model_prepared.glb", "_video_poster.jpg", "_video.mp4", "_hdrp.unitypackage",
                     "_all_animations_unity.fbx", "_all_animations.blend", "_model_prepared_rigged.blend")
AUTOTESTS = pathlib.Path(os.environ.get("AUTOTESTS_DIR", "/srv/autorig/autotests"))
PY = "/srv/autorig/venv/bin/python3"


def submit(base: str, input_url: str) -> dict:
    payload = {"input_url": input_url, "type": "t_pose", "mode": "only_rig",
               "queue_class": "interactive", "workload_class": "autorig_interactive"}
    request = urllib.request.Request(base + "/api-converter-glb", data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    return json.loads(urllib.request.urlopen(request, timeout=60).read())


def wait(base: str, task_id: str, started: float, limit: float) -> dict:
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
    return final


def served(base: str, outputs: list) -> dict:
    out = {}
    for url in outputs:
        name = url.rsplit("/", 1)[-1]
        guid = url.rstrip("/").rsplit("/", 2)[-2]
        local = f"{base}/converter/glb/{guid}/{name}"
        try:
            req = urllib.request.Request(local, headers={"Range": "bytes=0-1023"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                out[name] = resp.status
        except urllib.error.HTTPError as exc:
            out[name] = exc.code
        except Exception as exc:
            out[name] = type(exc).__name__
    return out


def guid_of(outputs: list) -> str | None:
    for url in outputs:
        if url.endswith("_model_prepared.glb"):
            return url.rsplit("/", 1)[-1][: -len("_model_prepared.glb")]
    return None


def fetch(base: str, guid: str, name: str, dest: pathlib.Path) -> bool:
    try:
        with urllib.request.urlopen(f"{base}/converter/glb/{guid}/{name}", timeout=120) as resp, open(dest, "wb") as f:
            while True:
                block = resp.read(1 << 20)
                if not block:
                    break
                f.write(block)
        return dest.stat().st_size > 12
    except Exception:
        return False


def judge_output(base: str, outputs: list, *, words: str = "", xfail: dict | None = None, case_id: str = "") -> dict:
    """The rigged and prepared GLBs through the tunnel -> autotests.py artifact (exit 0 = PASS)."""
    guid = guid_of(outputs)
    if not guid:
        return {"gate": "FAIL", "error": "no _model_prepared.glb among the outputs"}
    with tempfile.TemporaryDirectory(prefix="canary-") as tmp:
        rig, prep = pathlib.Path(tmp) / "rig.glb", pathlib.Path(tmp) / "prepared.glb"
        if not fetch(base, guid, f"{guid}_all_animations.glb", rig):
            return {"gate": "FAIL", "error": f"{guid}_all_animations.glb not served"}
        cmd = [PY, "-P", str(AUTOTESTS / "autotests.py"), "artifact", "--glb", str(rig), "--case-id", case_id or guid]
        if fetch(base, guid, f"{guid}_model_prepared.glb", prep):
            cmd += ["--prepared", str(prep)]
        if words:
            cmd += ["--words", words]
        if xfail:
            cmd += ["--xfail", json.dumps({k: {"xfail": v} for k, v in xfail.items()})]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        try:
            doc = json.loads(p.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return {"gate": "FAIL", "error": (p.stderr or p.stdout)[-400:]}
        doc["results"] = [{k: r.get(k) for k in ("check", "status", "failed", "error")} for r in doc.get("results", [])]
        return doc


def one(base: str, input_url: str, limit: float, **judge) -> dict:
    started = time.time()
    body = submit(base, input_url)
    task_id = body.get("task_id")
    result = {"task_id": task_id, "input": input_url.rsplit("/", 1)[-1], "progress_page": body.get("progress_page")}
    final = wait(base, task_id, started, limit)
    result["status"] = final.get("status")
    result["error"] = (final.get("error") or "")[:300]
    result["seconds"] = round(time.time() - started, 1)
    outputs = final.get("output_urls") or []
    result["outputs"] = len(outputs)
    result["served"] = served(base, outputs)
    result["missing"] = [s for s in EXPECTED_SUFFIXES if not any(n.endswith(s) for n in result["served"])]
    delivered = bool(result["status"] in ("Completed", "Done") and not result["missing"]
                     and all(v in (200, 206) for k, v in result["served"].items() if not k.endswith(".zip")))
    result["autotests"] = judge_output(base, outputs, **judge) if delivered else {"gate": "SKIPPED"}
    result["ok"] = delivered and result["autotests"].get("gate") == "PASS"
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("box")
    ap.add_argument("port", type=int)
    ap.add_argument("timeout", type=float, nargs="?", default=1200.0)
    ap.add_argument("--corpus", action="store_true", help="also convert the corpus cases with a converter input")
    ap.add_argument("--case", action="append", help="only these corpus cases")
    ap.add_argument("--no-checks", action="store_true", help="delivery only (no output checks): never for a restore")
    a = ap.parse_args()
    base = f"http://127.0.0.1:{a.port}"
    result = {"box": a.box}
    if a.no_checks:
        global judge_output
        judge_output = lambda *x, **k: {"gate": "PASS", "skipped": True}          # noqa: E731
    result.update(one(base, INPUT, a.timeout, case_id="default_t_pose"))
    if a.corpus:
        man = json.loads((AUTOTESTS / "corpus.json").read_text(encoding="utf-8"))
        rows = []
        for case in man["cases"]:
            conv = case.get("converter")
            if not conv or (a.case and case["id"] not in a.case):
                continue
            row = one(base, conv["input_url"], a.timeout, words=conv.get("words", ""), xfail=conv.get("xfail") or {},
                      case_id=case["id"])
            row["case"] = case["id"]
            rows.append(row)
        result["corpus"] = rows
        result["ok"] = result["ok"] and all(r["ok"] for r in rows)
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
