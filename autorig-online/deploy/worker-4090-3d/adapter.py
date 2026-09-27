"""worker-4090 as a fleet 3D box: the converter API the site uses, on the local Hunyuan3D-2.1.

    GET  /api-converter-glb/server-status          503 while the 4090 renders for the farm
    POST /api-converter-glb/generate-3d            {"image_url", "quality", "background_method"}
    GET  /api-converter-glb/generate-3d/status/ID  {"status", "current_stage", "progress",
                                                    "elapsed_seconds", "output_urls": {"glb", "preview"}, "error"}

Hunyuan's Gradio app is started on demand and stopped after IDLE_STOP_SECONDS so the
card is free for the ComfyUI render worker and the owner. It never runs while the render
worker has a prompt on the card (the box then reports busy and the site picks f7/f13),
and before starting it asks that ComfyUI to unload its models. Nothing else on the PC is
touched. Results (GLB + a render of the mesh) are published to autorig.online/dev/api/scratch.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
import urllib.request
import uuid
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

sys.path.insert(0, r"R:\AssetStore\LittleTiltConstructor\tools")
import hy_local as H  # noqa: E402  (start/stop/wait_ready/alpha_concept/QUALITY)

HERE = Path(__file__).resolve().parent
TOKEN = (HERE / "token.txt").read_text().strip()
WORK = HERE / "jobs"
COMFY = "http://127.0.0.1:8988"
PORT = 18777
HY_PORT = H.PORT
IDLE_STOP_SECONDS = int(os.getenv("HY_ADAPTER_IDLE_STOP", "600"))
SCRATCH = "https://autorig.online/dev/api/scratch"

app = FastAPI()
jobs: dict[str, dict] = {}
lock = threading.Lock()
state = {"proc": None, "last_used": 0.0, "busy": False}


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(HERE / "adapter.log", "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def render_worker_busy() -> bool:
    try:
        q = json.load(urllib.request.urlopen(COMFY + "/queue", timeout=5))
        return bool(q.get("queue_running") or q.get("queue_pending"))
    except Exception:
        return False


def comfy_free() -> None:
    try:
        req = urllib.request.Request(COMFY + "/free", json.dumps({"unload_models": True, "free_memory": True}).encode(),
                                     {"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=20)
    except Exception as exc:
        log(f"comfy /free skipped: {exc}")


def publish(path: Path) -> str:
    boundary = uuid.uuid4().hex
    data = path.read_bytes()
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{path.name}\"\r\n"
            f"Content-Type: application/octet-stream\r\n\r\n").encode() + data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(SCRATCH, body, {"Content-Type": f"multipart/form-data; boundary={boundary}"})
    return json.load(urllib.request.urlopen(req, timeout=300))["url"]


def strip_ground(path_in, path_out):
    """Drop a flat ground slab the background cut left under the feet (2026-09-27)."""
    import numpy as np
    import trimesh
    scene = trimesh.load(path_in)
    geoms = scene.geometry if hasattr(scene, "geometry") else {"m": scene}
    removed = 0
    for name, g in geoms.items():
        height = float(np.ptp(g.vertices[:, 1])) or 1.0
        span = float(max(np.ptp(g.vertices[:, 0]), np.ptp(g.vertices[:, 2]))) or 1.0
        labels = trimesh.graph.connected_component_labels(g.face_adjacency, node_count=len(g.faces))
        keep = np.ones(len(g.faces), bool)
        for lab in np.unique(labels):
            faces = np.where(labels == lab)[0]
            v = g.vertices[np.unique(g.faces[faces])]
            ext = np.ptp(v, axis=0)
            if ext[1] < 0.03 * height and max(ext[0], ext[2]) > 0.5 * span:
                keep[faces] = False
        if not keep.all():
            removed += int((~keep).sum())
            g.update_faces(keep); g.remove_unreferenced_vertices()
    (scene if hasattr(scene, "geometry") else geoms["m"]).export(path_out)
    return removed


def render_preview(glb: Path, out: Path) -> None:
    """A lit front three-quarter render of the mesh itself (vertex colours / texture)."""
    import numpy as np
    import trimesh
    from PIL import Image
    mesh = trimesh.load(str(glb), force="mesh")
    try:
        colours = mesh.visual.to_color().vertex_colors[:, :3].astype(float)
    except Exception:
        colours = np.full((len(mesh.vertices), 3), 190.0)
    a = np.radians(30)
    rot = np.array([[np.cos(a), 0, np.sin(a)], [0, 1, 0], [-np.sin(a), 0, np.cos(a)]])
    v = (mesh.vertices - mesh.bounds.mean(axis=0)) @ rot.T
    v = v - (v.max(axis=0) + v.min(axis=0)) / 2
    n = mesh.vertex_normals @ rot.T
    light = np.array([0.3, 0.5, 0.8]); light /= np.linalg.norm(light)
    shade = np.clip(n @ light, 0, 1) * 0.7 + 0.3
    W = Hh = 768
    s = 0.82 * Hh / max(np.ptp(v[:, 0]), np.ptp(v[:, 1]))
    img = np.full((Hh, W, 3), 245, np.uint8)
    zbuf = np.full((Hh, W), -1e9)
    tri = v[mesh.faces]
    col = (colours[mesh.faces].mean(axis=1) * shade[mesh.faces].mean(axis=1)[:, None]).clip(0, 255)
    order = np.argsort(tri[:, :, 2].mean(axis=1))
    from PIL import ImageDraw
    canvas = Image.fromarray(img)
    draw = ImageDraw.Draw(canvas)
    for i in order:
        pts = [((p[0]) * s + W / 2, Hh / 2 - p[1] * s) for p in tri[i]]
        draw.polygon(pts, fill=tuple(int(c) for c in col[i]))
    canvas.save(out)


def ensure_hunyuan(profile: dict) -> None:
    proc = state["proc"]
    if proc is not None and proc.poll() is None:
        return
    comfy_free()
    log("starting Hunyuan")
    if H.free(HY_PORT):
        state["proc"] = H.start(profile, WORK / "cache", HY_PORT)
        H.wait_ready(HY_PORT, state["proc"])
    log("Hunyuan ready")


def run_job(job_id: str) -> None:
    job = jobs[job_id]
    started = time.time()
    folder = WORK / job_id / "el"
    folder.mkdir(parents=True, exist_ok=True)
    try:
        with lock:
            state["busy"] = True
            job.update(status="Downloading", current_stage="Downloading", progress=5)
            from PIL import Image
            raw = folder.parent / "source"
            urllib.request.urlretrieve(job["image_url"], raw)
            with Image.open(raw) as im:
                # An RGBA file only counts as already cut when a real share of it
                # is transparent; a few clear pixels on an opaque backdrop is not
                # a cut-out (that shipped a slab on 2026-09-27).
                import numpy as np
                keep_alpha = im.mode in ("RGBA", "LA") and \
                    float((np.asarray(im.getchannel("A")) < 16).mean()) > 0.05
                im = im.convert("RGBA" if keep_alpha else "RGB")
                scale = max(1, int(1024 / max(im.size)))
                if scale > 1:
                    im = im.resize((im.width * scale, im.height * scale), Image.LANCZOS)
                im.save(folder / "concept.png")
            reference = folder / "concept.png" if keep_alpha or job["background_method"] == "none" \
                else H.alpha_concept(folder)
            profile = H.QUALITY.get(job["quality"], H.QUALITY["standard"])
            job.update(status="LoadingModels", current_stage="LoadingModels", progress=15)
            ensure_hunyuan(profile)
            job.update(status="GeneratingShape", current_stage="GeneratingShape", progress=30)
            from gradio_client import Client, handle_file
            client = Client(f"http://127.0.0.1:{HY_PORT}", verbose=False)
            result = client.predict(handle_file(str(reference)), None, None, None, None,
                                    profile["steps"], 7.5, 1234, profile["octree"], False, 200000, False,
                                    api_name="/generation_all")
            values = result if isinstance(result, (list, tuple)) else [result]
            glb = next((p for p in (H.path_of(v) for v in values) if p.lower().endswith(".glb") and Path(p).is_file()), "")
            if not glb:
                # gradio_client hands back its own download of the file; the
                # app writes the original into its cache folder for this run.
                made = [p for p in (WORK / "cache").rglob("textured_mesh.glb") if p.stat().st_mtime >= started]
                glb = str(max(made, key=lambda p: p.stat().st_mtime)) if made else ""
            if not glb:
                raise RuntimeError("Hunyuan returned no textured GLB")
            out = folder.parent / "model.glb"
            out.write_bytes(Path(glb).read_bytes())
            try:
                removed = strip_ground(str(out), str(out))
                if removed:
                    log(f"{job_id}: removed a {removed}-face ground slab")
            except Exception as exc:
                log(f"{job_id}: ground strip skipped: {exc}")
            job.update(status="Publishing", current_stage="Publishing", progress=90)
            preview = folder.parent / "preview.png"
            render_preview(out, preview)
            job["output_urls"] = {"glb": publish(out), "preview": publish(preview)}
            job.update(status="Completed", current_stage="Completed", progress=100)
            log(f"{job_id} done in {time.time() - started:.0f}s: {job['output_urls']['glb']}")
    except Exception as exc:
        job.update(status="Failed", error=str(exc)[:500])
        log(f"{job_id} FAILED: {exc}\n{traceback.format_exc()}")
    finally:
        job["elapsed_seconds"] = round(time.time() - started, 1)
        state["busy"] = False
        state["last_used"] = time.time()


def idle_stopper() -> None:
    while True:
        time.sleep(30)
        proc = state["proc"]
        if proc is not None and proc.poll() is None and not state["busy"] \
                and time.time() - state["last_used"] > IDLE_STOP_SECONDS:
            log("idle: stopping Hunyuan to free the 4090")
            H.stop(proc)
            state["proc"] = None


def check(auth: str | None) -> None:
    if (auth or "") != f"Bearer {TOKEN}":
        raise HTTPException(status_code=401, detail="bad token")


@app.get("/api-converter-glb/server-status")
def server_status(authorization: str | None = Header(default=None)):
    check(authorization)
    active = sum(1 for j in jobs.values() if j["status"] not in ("Completed", "Failed"))
    if render_worker_busy() and not state["busy"]:
        return JSONResponse(status_code=503, content={"busy": True, "reason": "the 4090 is rendering for the farm"})
    return {"server": "worker-4090 hunyuan adapter", "maintenance": False,
            "tasks_summary": {"queue_size": max(0, active - (1 if state["busy"] else 0)),
                              "processing": 1 if state["busy"] else 0, "pending": 0},
            "hunyuan_loaded": state["proc"] is not None and state["proc"].poll() is None}


@app.post("/api-converter-glb/generate-3d")
def generate(body: dict, authorization: str | None = Header(default=None)):
    check(authorization)
    if render_worker_busy():
        raise HTTPException(status_code=503, detail="the 4090 is rendering for the farm")
    if state["busy"]:
        raise HTTPException(status_code=503, detail="a 3D job is already running on the 4090")
    url = str(body.get("image_url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="image_url required")
    job_id = uuid.uuid4().hex
    jobs[job_id] = {"status": "Queued", "current_stage": "Queued", "progress": 0, "error": "",
                    "image_url": url, "quality": str(body.get("quality") or "standard"),
                    "background_method": str(body.get("background_method") or "auto"),
                    "output_urls": {}, "elapsed_seconds": 0.0, "created": time.time()}
    threading.Thread(target=run_job, args=(job_id,), daemon=True).start()
    log(f"{job_id} accepted: {url}")
    return {"task_id": job_id, "status": "Queued"}


@app.get("/api-converter-glb/generate-3d/status/{job_id}")
def status(job_id: str, authorization: str | None = Header(default=None)):
    check(authorization)
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="no such task")
    out = {k: job[k] for k in ("status", "current_stage", "progress", "error", "output_urls", "elapsed_seconds")}
    if job["status"] not in ("Completed", "Failed"):
        out["elapsed_seconds"] = round(time.time() - job["created"], 1)
    return out


if __name__ == "__main__":
    import uvicorn
    WORK.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=idle_stopper, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
