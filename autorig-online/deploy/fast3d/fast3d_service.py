"""AutoRig Fast 3D: Hunyuan3D-2mini on every GPU box (owner 2026-10-17: «делай мини-30, выбор моделей должен быть
опцией во вьювере»; «нельзя это на фаст мини перевести, чтобы 3д модель появлялась через 5 секунд, а потом просто
улучшалась постепенно»; AGENTS.md «Fast 3D Generator On Every Box, worker-4090 First And Batched»).

R&D (handoff 2026-10-16_nvs-rnd-3d.md, 6 ground-truth cars): mini at 30 steps F@5 0.676, mini-turbo (5 steps) 0.666;
plan C's Hunyuan 2.1 0.66-0.80 in ~15 min on the farm queue.

    python fast3d_service.py --port 8990 --box raptor [--idle-unload 900] [--hunyuan <Hunyuan3D-2 dir>] [--pylib DIR]

The box's python with torch-CUDA and Hunyuan3D-2's hy3dgen (NOT the 2.1 hy3dshape); 127.0.0.1 only, the VPS reaches it
through the box's SSH tunnel. Weights: $HY3DGEN_MODELS/tencent/Hunyuan3D-2mini/<subfolder>/{config.yaml,
model.fp16.safetensors} (DiT + VAE + DINOv2 in one 3.8 GB file per model), else the HF cache under --hf.
One GPU job at a time (a batch of pictures is one job); one pipeline resident (switching frees the other); the GPU is
freed after --idle-unload idle seconds (the card is shared with renders, ComfyUI and the LLM). Before a job the free
VRAM is checked (--min-free-mb + --batch-mb per extra picture): when short, an idle ComfyUI on the box is asked to free
its cache once; still short -> 503 vram (the queue tries later or another box). Every job -> logs/jobs.jsonl.

    GET  /health                    {ok, box, models_array, loaded, busy, jobs, seconds_median {model: s per picture},
                                     vram_free_mb, max_batch, queue_depth}
    POST /generate?model=M          body: one picture (PNG / JPEG bytes) -> model/gltf-binary; header X-Fast3d (and
                                    X-Mini3d): the job's JSON (seconds per phase, faces, peak GB)
    POST /generate_batch            JSON {model, images: [base64 PNG/JPEG, ...], <options>} -> {results: [{glb_b64,
                                    row} | {error}], row}: the pictures in one forward (batch <= max_batch)
    Options (query or JSON): model hunyuan_mini30 | hunyuan_mini_turbo, steps (1..100), octree (64..512, 380),
    faces (0 = no reduction, 200000), seed (12345), floaters (1), vdm (1 = FlashVDM decoding), wait (s for the GPU lock)
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import statistics
import sys
import threading
import time
import traceback
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

MODELS = {
    # id -> (HF subfolder of tencent/Hunyuan3D-2mini, default steps)
    "hunyuan_mini30": ("hunyuan3d-dit-v2-mini", 30),
    "hunyuan_mini_turbo": ("hunyuan3d-dit-v2-mini-turbo", 5),
}
STATE = {"lock": threading.Lock(), "pipe": None, "model": None, "rembg": None, "busy": False, "jobs": 0, "errors": 0,
         "pictures": 0, "last": None, "last_used": 0.0, "loads": 0, "idle_unload_s": 900.0, "seconds": {}, "box": "",
         "root": "", "min_free_mb": 5200, "batch_mb": 700, "max_batch": 4, "comfy": "http://127.0.0.1:8289",
         "waiting": 0, "dtype": "fp16", "pipes": {}, "keep": 1}


def _log(doc):
    print(json.dumps(doc, ensure_ascii=False), flush=True)
    try:
        with open(os.path.join(STATE["root"], "logs", "jobs.jsonl"), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _vram_free_mb():
    """Free VRAM of the card; nvidia-smi until this process has a CUDA context (a /health probe never makes one)."""
    try:
        import torch
        if torch.cuda.is_initialized():
            free, _total = torch.cuda.mem_get_info()
            return int(free / 2**20)
    except Exception:                                     # noqa: BLE001
        pass
    try:
        import subprocess
        out = subprocess.run(["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
                             capture_output=True, text=True, timeout=8).stdout
        return int(float(out.split()[0]))
    except Exception:                                     # noqa: BLE001
        return None


def unload():
    import gc
    import torch
    STATE["pipe"], STATE["model"] = None, None
    STATE["pipes"].clear()
    gc.collect()
    torch.cuda.empty_cache()


def load(model, vdm=True):
    """The pipeline of `model` (the other one is freed first). Called under the lock. vdm: FlashVDM volume decoding
    (hierarchical: ~1 s instead of ~3 min of dense chunks at octree 380 on a shared 3080 Ti)."""
    import torch
    from hy3dgen.shapegen import Hunyuan3DDiTFlowMatchingPipeline
    key = f"{model}:{int(bool(vdm))}"
    if STATE["model"] == key and STATE["pipe"] is not None:
        return 0.0
    if key in STATE["pipes"]:                             # --keep 2: both models resident, switching costs nothing
        STATE["pipe"], STATE["model"] = STATE["pipes"][key], key
        return 0.0
    if STATE["pipe"] is not None and len(STATE["pipes"]) >= STATE["keep"]:
        unload()
    t = time.time()
    kw = {"dtype": torch.float32} if STATE["dtype"] == "fp32" else {}
    p = Hunyuan3DDiTFlowMatchingPipeline.from_pretrained("tencent/Hunyuan3D-2mini", subfolder=MODELS[model][0],
                                                         use_safetensors=True, variant="fp16", device="cuda", **kw)
    if vdm:
        p.enable_flashvdm()
    STATE["pipe"], STATE["model"] = p, key
    STATE["pipes"][key] = p
    STATE["loads"] += 1
    return round(time.time() - t, 1)


def _comfy_free():
    """Ask the box's ComfyUI to drop its cached models, only when its queue is empty (never under a render)."""
    try:
        q = json.loads(urllib.request.urlopen(STATE["comfy"] + "/queue", timeout=4).read())
        if q.get("queue_running") or q.get("queue_pending"):
            return "comfy busy"
        req = urllib.request.Request(STATE["comfy"] + "/free", data=b'{"unload_models": true, "free_memory": true}',
                                     headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=6).read()
        time.sleep(2)
        return "comfy freed"
    except Exception as exc:                              # noqa: BLE001
        return f"comfy: {exc}"[:80]


def _key_background(im):
    """No rembg on this box: the picture's border colour keyed out (the intake sends cut-outs / plain backdrops)."""
    import numpy as np
    from PIL import Image
    a = np.asarray(im.convert("RGB")).astype(np.int16)
    border = np.concatenate([a[0], a[-1], a[:, 0], a[:, -1]])
    bg = np.median(border, 0)
    d = np.abs(a - bg).sum(-1)
    alpha = np.clip((d - 24) * 8, 0, 255).astype(np.uint8)
    out = Image.fromarray(np.dstack([a.astype(np.uint8), alpha]), "RGBA")
    return out


def _prep(data):
    from PIL import Image
    im = Image.open(io.BytesIO(data))
    im.load()
    if im.mode == "RGBA" and im.getchannel("A").getextrema()[0] < 250:
        return im
    if STATE["rembg"] is None:
        try:
            from hy3dgen.rembg import BackgroundRemover
            STATE["rembg"] = BackgroundRemover()
        except Exception:                                 # noqa: BLE001 - rembg is not on every box
            STATE["rembg"] = _key_background
    return STATE["rembg"](im.convert("RGB"))


def _reduce(mesh, faces):
    """pyfqmr (fast quadric) when installed, else open3d, else pymeshlab (hy3dgen FaceReducer)."""
    import numpy as np
    import trimesh
    try:
        import pyfqmr
        s = pyfqmr.Simplify()
        s.setMesh(np.asarray(mesh.vertices, np.float64), np.asarray(mesh.faces, np.int32))
        s.simplify_mesh(target_count=int(faces), aggressiveness=7, preserve_border=True, verbose=0)
        v, f, _n = s.getMesh()
        return trimesh.Trimesh(v, f, process=False), "pyfqmr"
    except ImportError:
        pass
    try:
        import open3d as o3d
        m = o3d.geometry.TriangleMesh(o3d.utility.Vector3dVector(np.asarray(mesh.vertices, np.float64)),
                                      o3d.utility.Vector3iVector(np.asarray(mesh.faces, np.int32)))
        m = m.simplify_quadric_decimation(int(faces))
        return trimesh.Trimesh(np.asarray(m.vertices), np.asarray(m.triangles), process=False), "open3d"
    except ImportError:
        pass
    from hy3dgen.shapegen import FaceReducer
    return FaceReducer()(mesh, max_facenum=int(faces)), "pymeshlab"


def _opts(q):
    model = q.get("model") or "hunyuan_mini30"
    if model not in MODELS:
        raise ValueError(f"model must be one of {sorted(MODELS)}")
    o = {"model": model, "steps": int(q.get("steps") or MODELS[model][1]), "octree": int(q.get("octree") or 380),
         "faces": int(q.get("faces") if q.get("faces") not in (None, "") else 200000), "seed": int(q.get("seed") or 12345),
         "floaters": str(q.get("floaters", "1")) != "0", "vdm": str(q.get("vdm", "1")) != "0"}
    if not (1 <= o["steps"] <= 100 and 64 <= o["octree"] <= 512 and 0 <= o["faces"] <= 5_000_000):
        raise ValueError("steps 1..100, octree 64..512, faces 0..5000000")
    return o


_OWNER = {"at": 0.0, "apps": []}
_OWN_OK = ("python", "pythonw", "dwm", "claude", "chrome", "msedge", "explorer", "comfyui", "nvcontainer", "obs64")


def owner_3d_apps():
    """--yield-3d: the processes driving the card's 3D engine above --yield-3d-pct (a game on the owner's own GPU,
    worker-4090 2026-10-17: cs2 at 96 % made a mini30 job 83 s instead of 4.5 s and cost the owner his frame rate).
    Cached 10 s; Windows performance counters; [] when not Windows or off."""
    if not STATE.get("yield_3d") or os.name != "nt":
        return []
    if time.time() - _OWNER["at"] < 10:
        return _OWNER["apps"]
    apps = []
    try:
        import subprocess
        ps = ("$c=Get-Counter '\\GPU Engine(*engtype_3D)\\Utilization Percentage' -ErrorAction SilentlyContinue;"
              "$c.CounterSamples | ? { $_.CookedValue -gt %d } | %% { $id=[regex]::Match($_.InstanceName,'pid_(\\d+)').Groups[1].Value;"
              "$p=Get-Process -Id $id -ErrorAction SilentlyContinue; if ($p) { $p.ProcessName + ':' + [int]$_.CookedValue } }"
              % int(STATE.get("yield_3d_pct") or 40))
        out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True, timeout=15).stdout
        for line in out.split():
            name = line.split(":")[0].strip().lower()
            if name and not name.startswith(_OWN_OK):
                apps.append(line.strip())
    except Exception:                                     # noqa: BLE001
        apps = []
    _OWNER.update(at=time.time(), apps=apps)
    return apps


def generate(datas, q):
    """One forward over len(datas) pictures -> ([glb bytes | None], [row], batch row)."""
    import torch
    o = _opts(q)
    n = len(datas)
    if not (1 <= n <= STATE["max_batch"]):
        raise ValueError(f"batch 1..{STATE['max_batch']}")
    owner = owner_3d_apps()
    if owner:
        raise MemoryError("owner's GPU in use: " + ", ".join(owner)[:120])
    t0 = time.time()
    row = {**{k: o[k] for k in ("model", "steps", "octree", "vdm")}, "faces_target": o["faces"], "box": STATE["box"],
           "batch": n}
    resident = STATE["pipe"] is not None
    free = _vram_free_mb()
    need = STATE["min_free_mb"] + STATE["batch_mb"] * (n - 1) - (1800 if resident else 0)
    if free is not None and free < need:
        row["comfy"] = _comfy_free()
        free = _vram_free_mb()
        if free is not None and free < need:
            raise MemoryError(f"vram {free} MB free < {need} MB")
    row["vram_free_mb"] = free
    tp = time.time()
    ims = [_prep(d) for d in datas]
    row["prep_s"] = round(time.time() - tp, 2)
    row["load_s"] = load(o["model"], o["vdm"])
    torch.cuda.reset_peak_memory_stats()
    tg = time.time()
    pipe = STATE["pipe"]
    if n == 1:
        meshes = pipe(image=ims[0], num_inference_steps=o["steps"], octree_resolution=o["octree"], num_chunks=20000,
                      generator=torch.manual_seed(o["seed"]), output_type="trimesh")
    else:
        # the DiT runs the whole batch in one forward; the FlashVDM decoder takes one latent at a time (batched it
        # fails: einops «p c -> b p c» with b=1, 2026-10-17)
        lat = pipe(image=ims, num_inference_steps=o["steps"], octree_resolution=o["octree"], num_chunks=20000,
                   generator=torch.manual_seed(o["seed"]), output_type="latent")
        row["dit_s"] = round(time.time() - tg, 2)
        meshes = []
        for i in range(n):
            meshes += pipe._export(lat[i:i + 1], "trimesh", box_v=1.01, mc_level=0.0, num_chunks=20000,
                                   octree_resolution=o["octree"], mc_algo=None, enable_pbar=False)
    row["gen_s"] = round(time.time() - tg, 2)
    row["peak_gb"] = round(torch.cuda.max_memory_allocated() / 1e9, 2)
    glbs, rows = [], []
    from hy3dgen.shapegen import DegenerateFaceRemover, FloaterRemover
    for i, mesh in enumerate(meshes[:n]):
        r = {"i": i}
        if mesh is None:
            glbs.append(None)
            rows.append({**r, "error": "no surface"})
            continue
        r["faces_raw"] = int(len(mesh.faces))
        tr = time.time()
        if o["floaters"]:
            mesh = FloaterRemover()(mesh)
            mesh = DegenerateFaceRemover()(mesh)
        r["floaters_s"] = round(time.time() - tr, 2)
        if o["faces"] and len(mesh.faces) > o["faces"]:
            mesh, r["reducer"] = _reduce(mesh, o["faces"])
        r["post_s"] = round(time.time() - tr, 2)
        r["faces"] = int(len(mesh.faces))
        g = mesh.export(file_type="glb")
        r["bytes"] = len(g)
        glbs.append(g)
        rows.append(r)
    row["seconds"] = round(time.time() - t0, 2)
    row["seconds_per_picture"] = round(row["seconds"] / n, 2)
    return glbs, rows, row


def _idle_watch():
    while True:
        time.sleep(15)
        lim = STATE["idle_unload_s"]
        if lim and STATE["pipe"] is not None and not STATE["busy"] and time.time() - STATE["last_used"] > lim:
            with STATE["lock"]:
                if STATE["pipe"] is not None and time.time() - STATE["last_used"] > lim:
                    unload()
                    _log({"unloaded": True, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


def _free_cuda():
    try:
        import gc
        import torch
        gc.collect()
        torch.cuda.empty_cache()
    except Exception:                                     # noqa: BLE001
        pass


def _run(datas, q):
    """Under the GPU lock: (status, glbs, rows, row | error doc)."""
    if not STATE["lock"].acquire(timeout=float(q.get("wait") or 0)):
        return 503, None, None, {"error": "busy", "loaded": STATE["model"]}
    STATE["busy"] = True
    try:
        glbs, rows, row = generate(datas, q)
        STATE["jobs"] += 1
        STATE["pictures"] += len(datas)
        sec = STATE["seconds"].setdefault(row["model"], [])
        sec.append(row["seconds_per_picture"])
        del sec[:-50]
        row["at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        row["items"] = rows
        STATE["last"] = row
        _log(row)
        return 200, glbs, rows, row
    except MemoryError as exc:
        return 503, None, None, {"error": "vram", "detail": str(exc)}
    except ValueError as exc:
        if STATE["pipe"] is None:                         # a failed load: free what it left on the card
            _free_cuda()
        return 400, None, None, {"error": str(exc)}
    except Exception as exc:                              # noqa: BLE001
        STATE["errors"] += 1
        _log({"error": repr(exc)[:400], "trace": traceback.format_exc()[-1500:]})
        _free_cuda()
        return 500, None, None, {"error": repr(exc)[:300]}
    finally:
        STATE["busy"] = False
        STATE["last_used"] = time.time()
        STATE["lock"].release()


class H(BaseHTTPRequestHandler):
    def _send(self, code, doc=None, body=None, ctype="application/json", extra=None):
        body = body if body is not None else json.dumps(doc, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if urlsplit(self.path).path != "/health":
            return self._send(404, {"error": "not found"})
        med = {m: round(statistics.median(v), 1) for m, v in STATE["seconds"].items() if v}
        owner = owner_3d_apps()
        self._send(200, {"ok": True, "service": "fast3d", "box": STATE["box"], "models_array": sorted(MODELS),
                         "loaded": STATE["model"], "busy": STATE["busy"] or bool(owner), "owner_3d_apps": owner,
                         "jobs": STATE["jobs"],
                         "pictures": STATE["pictures"], "errors": STATE["errors"], "loads": STATE["loads"],
                         "seconds_median": med, "last": STATE["last"], "vram_free_mb": _vram_free_mb(),
                         "idle_unload_s": STATE["idle_unload_s"], "min_free_mb": STATE["min_free_mb"],
                         "max_batch": STATE["max_batch"], "batch_mb": STATE["batch_mb"], "dtype": STATE["dtype"],
                         "resident": sorted(STATE["pipes"]),
                         "queue_depth": STATE["waiting"] + (1 if STATE["busy"] else 0)})

    def do_POST(self):
        u = urlsplit(self.path)
        q = {k: v[-1] for k, v in parse_qs(u.query).items()}
        n = int(self.headers.get("Content-Length") or 0)
        if not (0 < n <= 200_000_000):
            return self._send(400, {"error": "empty or too large body"})
        data = self.rfile.read(n)
        STATE["waiting"] += 1
        try:
            if u.path == "/generate":
                code, glbs, _rows, row = _run([data], q)
                if code != 200:
                    return self._send(code, row)
                if glbs[0] is None:
                    return self._send(500, {"error": "no surface", "row": row})
                hdr = json.dumps({**row, **{k: v for k, v in row["items"][0].items() if k != "i"}})
                return self._send(200, body=glbs[0], ctype="model/gltf-binary", extra={"X-Fast3d": hdr, "X-Mini3d": hdr})
            if u.path == "/generate_batch":
                try:
                    doc = json.loads(data)
                    datas = [base64.b64decode(x) for x in doc.get("images") or []]
                except (ValueError, TypeError):
                    return self._send(400, {"error": "JSON {model, images: [base64, ...]}"})
                if not datas:
                    return self._send(400, {"error": "no images"})
                qq = {**q, **{k: v for k, v in doc.items() if k != "images"}}
                code, glbs, rows, row = _run(datas, qq)
                if code != 200:
                    return self._send(code, row)
                res = [{"glb_b64": base64.b64encode(g).decode(), "row": r} if g is not None else {"error": r.get("error"), "row": r}
                       for g, r in zip(glbs, rows)]
                return self._send(200, {"results": res, "row": {k: v for k, v in row.items() if k != "items"}})
            return self._send(404, {"error": "not found"})
        finally:
            STATE["waiting"] -= 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8990)
    ap.add_argument("--box", default=os.environ.get("COMPUTERNAME", "box").lower())
    ap.add_argument("--root", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--hunyuan", default=r"D:\AI\HY3D2\Hunyuan3D2_WinPortable\Hunyuan3D-2",
                    help="the Hunyuan3D-2 checkout (hy3dgen)")
    ap.add_argument("--pylib", default="", help="an extra site dir (trimesh, pymeshlab, pyfqmr) prepended to sys.path")
    ap.add_argument("--hf", default=os.environ.get("HF_HOME", r"X:\orbit\nvs\hf"), help="HF_HOME (fallback weights)")
    ap.add_argument("--idle-unload", type=float, default=900.0)
    ap.add_argument("--min-free-mb", type=int, default=5200)
    ap.add_argument("--batch-mb", type=int, default=700, help="extra VRAM per extra picture of a batch")
    ap.add_argument("--max-batch", type=int, default=4)
    ap.add_argument("--dtype", choices=("fp16", "fp32"), default="fp16", help="fp32 for Pascal (GTX 10xx) cards")
    ap.add_argument("--comfy", default="http://127.0.0.1:8289")
    ap.add_argument("--yield-3d", type=int, default=0, help="1: busy while another app drives the 3D engine (the "
                    "owner's games on worker-4090)")
    ap.add_argument("--yield-3d-pct", type=int, default=40)
    ap.add_argument("--keep", type=int, default=1, help="pipelines kept resident (2 on a 24 GB card: no unload+load "
                    "switch; torch 2.11 on worker-4090 crashed in torch_cpu.dll loading after an unload, 2026-10-17)")
    a = ap.parse_args()
    os.environ.setdefault("HF_HOME", a.hf)
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("HY3DGEN_MODELS", os.path.join(a.root, "models"))
    if a.pylib:
        sys.path.insert(0, a.pylib)
    sys.path.insert(0, a.hunyuan)
    os.chdir(a.hunyuan)
    os.makedirs(os.path.join(a.root, "logs"), exist_ok=True)
    STATE.update(box=a.box, root=a.root, idle_unload_s=max(0.0, a.idle_unload), min_free_mb=a.min_free_mb,
                 batch_mb=a.batch_mb, max_batch=max(1, a.max_batch), comfy=a.comfy, dtype=a.dtype, keep=max(1, a.keep),
                 yield_3d=bool(a.yield_3d), yield_3d_pct=a.yield_3d_pct)
    try:                                                  # the last 50 seconds per model survive a restart
        with open(os.path.join(a.root, "logs", "jobs.jsonl"), encoding="utf-8") as fh:
            for line in fh.readlines()[-500:]:
                r = json.loads(line)
                if r.get("model") and (r.get("seconds_per_picture") or r.get("seconds")):
                    STATE["seconds"].setdefault(r["model"], []).append(r.get("seconds_per_picture") or r["seconds"])
    except (OSError, ValueError):
        pass
    threading.Thread(target=_idle_watch, daemon=True).start()
    _log({"start": True, "box": a.box, "port": a.port, "models": sorted(MODELS), "dtype": a.dtype,
          "max_batch": a.max_batch})
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    main()
