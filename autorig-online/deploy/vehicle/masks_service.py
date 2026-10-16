r"""GroundingDINO-tiny + SAM2.1-tiny mask service on a GPU box (owner 2026-10-15: «сразу делай чтобы мы могли
сегментировать быстро»). The same logic as mt/vehicle_masks_worker.py, kept loaded on CUDA behind HTTP, so a vehicle
picture is segmented in about a second instead of minutes on the VPS CPU.

    python masks_service.py --root R:\autorig\.runtime\vehicle-seg --port 8989 [--device cuda]

Runs under the ComfyUI python on worker-4090 (torch CUDA, transformers, sam2, hydra are there). The VPS reaches it
through a reverse tunnel (`ssh -R 19411:127.0.0.1:8989 autorig-vps`); mt/vehicle_v3.masks_detect tries
VEHICLE_MASKS_URL first and falls back to the CPU worker.

    POST /masks   {"image_b64": <png/jpg>, "queries": {"wheel": "wheel,tire", ...}, "box_threshold": 0.3,
                   "wheel_threshold": 0.4, "text_threshold": 0.25, "nms_iou": 0.5, "max_share": 0.35,
                   "boxes": [{"kind": "hood", "box": [x0, y0, x1, y1]}, ...]}      (boxes: optional)
              ->  the masks.json document (objects with box, scores, pixels, centre, ellipse) plus
                  "masks": {"<kind>_<i>.png": <png b64, 0/255>} and "seconds" {dino, sam, total}
              `boxes` (2026-10-16): one SAM2 mask per box hint, no DINO; objects with "source": "box",
              "prompt_box", masks "<kind>_box_<i>.png"; "boxes_done" counts them. With `boxes` and no `queries`
              DINO does not run at all. Without `boxes` the request is the text path as before.
    GET  /health  {"ok": true, "device": "cuda", "gpu": "...", "loaded": true, "box": "f12", "requests": 3,
                   "inflight": 0, "last_seconds": 1.8, "vram_mb": 2210, "idle_unload_s": 600}

Farm boxes (2026-10-15, «разнести на флот»): `--idle-unload S` frees the GPU after S idle seconds (renders and Hunyuan
share the card) and the next /masks loads again (a few seconds); 0 (the default, worker-4090) keeps them loaded.
`<root>/pylib` (hydra, iopath, portalocker for a python that lacks them) is appended to sys.path.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cv2
import numpy as np

STATE = {"loaded": False, "device": "cpu", "gpu": "", "lock": threading.Lock(), "root": "", "want_device": "cuda",
         "box": os.environ.get("VEHICLE_MASKS_BOX") or os.environ.get("COMPUTERNAME", ""), "requests": 0, "errors": 0,
         "inflight": 0, "last_seconds": None, "last_used": 0.0, "idle_unload_s": 0, "loads": 0, "load_error": ""}


def _iou(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    return inter / max(1e-9, (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter)


def load(root, device):
    import torch
    from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor
    if root not in sys.path:
        sys.path.insert(0, root)
    pylib = os.path.join(root, "pylib")
    if os.path.isdir(pylib) and pylib not in sys.path:
        sys.path.append(pylib)
    from sam2.build_sam import build_sam2
    from sam2.sam2_image_predictor import SAM2ImagePredictor
    t0 = time.perf_counter()
    STATE["proc"] = AutoProcessor.from_pretrained(os.path.join(root, "grounding-dino"), local_files_only=True)
    STATE["gd"] = AutoModelForZeroShotObjectDetection.from_pretrained(os.path.join(root, "grounding-dino"), local_files_only=True,
                                                                      use_safetensors=True).eval().to(device)
    STATE["sam"] = SAM2ImagePredictor(build_sam2("configs/sam2.1/sam2.1_hiera_t.yaml", os.path.join(root, "checkpoints", "sam2.1_hiera_tiny.pt"),
                                                 device=device, apply_postprocessing=False), max_hole_area=0, max_sprinkle_area=0)
    STATE["device"] = device
    STATE["gpu"] = torch.cuda.get_device_name(0) if device == "cuda" and torch.cuda.is_available() else ""
    STATE["loaded"] = True
    STATE["loads"] += 1
    STATE["last_used"] = time.time()
    print(json.dumps({"loaded_s": round(time.perf_counter() - t0, 2), "device": device, "gpu": STATE["gpu"]}), flush=True)


def _load_safe():
    with STATE["lock"]:
        if STATE["loaded"]:
            return
        try:
            load(STATE["root"], STATE["want_device"])
            STATE["load_error"] = ""
        except Exception as exc:                   # noqa: BLE001
            STATE["load_error"] = f"{type(exc).__name__}: {exc}"[:300]
            print(json.dumps({"load_error": STATE["load_error"]}), flush=True)


def unload():
    """Free the GPU (called under STATE['lock'] when idle)."""
    for k in ("proc", "gd", "sam"):
        STATE.pop(k, None)
    STATE["loaded"] = False
    try:
        import gc
        gc.collect()
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:                              # noqa: BLE001
        pass
    print(json.dumps({"unloaded": True, "idle_s": round(time.time() - STATE["last_used"], 1)}), flush=True)


def _idle_watch():
    while True:
        time.sleep(15)
        lim = STATE["idle_unload_s"]
        if lim > 0 and STATE["loaded"] and not STATE["inflight"] and time.time() - STATE["last_used"] > lim:
            with STATE["lock"]:
                if STATE["loaded"] and not STATE["inflight"] and time.time() - STATE["last_used"] > lim:
                    unload()


def _vram_mb():
    try:
        import torch
        if STATE["device"] == "cuda" and torch.cuda.is_available():
            return round(torch.cuda.memory_reserved() / 2 ** 20)
    except Exception:                              # noqa: BLE001
        pass
    return None


def segment(img_bgr, queries, box_threshold=0.3, wheel_threshold=0.4, text_threshold=0.25, nms_iou=0.5, max_share=0.35,
            boxes=None):
    import torch
    from PIL import Image
    proc, gd, sam, device = STATE["proc"], STATE["gd"], STATE["sam"], STATE["device"]
    pil = Image.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))
    W, H = pil.size
    img = np.array(pil)
    t0 = time.perf_counter()
    raw = []
    qlist = []
    for kind, words in queries.items():
        ws = [w.strip() for w in str(words).split(",") if w.strip()]
        qlist.append([kind, ws])
        for word in ws:
            inp = proc(images=pil, text=word + ".", return_tensors="pt").to(device)
            with torch.inference_mode():
                o = gd(**inp)
            try:
                r = proc.post_process_grounded_object_detection(o, inp.input_ids, threshold=box_threshold, text_threshold=text_threshold, target_sizes=[(H, W)])[0]
            except TypeError:
                r = proc.post_process_grounded_object_detection(o, inp.input_ids, box_threshold=box_threshold, text_threshold=text_threshold, target_sizes=[(H, W)])[0]
            for b, s in zip(r["boxes"].tolist(), r["scores"].tolist()):
                box = [max(0.0, b[0]), max(0.0, b[1]), min(float(W), b[2]), min(float(H), b[3])]
                w_, h_ = box[2] - box[0], box[3] - box[1]
                if w_ < 3 or h_ < 3 or w_ * h_ / (W * H) > max_share:
                    continue
                if kind == "wheel" and (not 0.35 <= w_ / h_ <= 2.8 or float(s) < wheel_threshold):
                    continue
                raw.append({"kind": kind, "word": word, "score": round(float(s), 3), "box": [round(v, 1) for v in box]})
    dino_s = round(time.perf_counter() - t0, 3)
    kept = []
    for r_ in sorted(raw, key=lambda x: -x["score"]):
        if all(_iou(r_["box"], k["box"]) < nms_iou for k in kept):
            kept.append(r_)
    t = time.perf_counter()
    objects, masks, counts = [], {}, {}
    with torch.inference_mode():
        sam.set_image(img)
        for k in kept:
            box = np.array(k["box"], np.float32)
            ctr = np.array([[(box[0] + box[2]) / 2, (box[1] + box[3]) / 2]], np.float32)
            ms, scores, _ = sam.predict(point_coords=ctr, point_labels=np.array([1], np.int32), box=box[None, :], multimask_output=True)
            best = None
            for m, s in zip(ms.astype(bool), scores):
                ys, xs = np.nonzero(m)
                if not len(xs):
                    continue
                inside = float(((xs >= box[0]) & (xs <= box[2]) & (ys >= box[1]) & (ys <= box[3])).mean())
                if inside >= 0.55 and (best is None or s > best[1]):
                    best = (m, float(s), inside)
            if best is None:
                continue
            m = best[0]
            i = counts.get(k["kind"], 0)
            counts[k["kind"]] = i + 1
            name = f"{k['kind']}_{i}.png"
            ok, png = cv2.imencode(".png", m.astype(np.uint8) * 255)
            masks[name] = base64.b64encode(png.tobytes()).decode("ascii")
            ell = None
            cs, _ = cv2.findContours(m.astype(np.uint8), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)
            if cs:
                c = max(cs, key=cv2.contourArea)
                if len(c) >= 5:
                    e = cv2.fitEllipse(c)
                    ell = {"centre": [round(e[0][0], 1), round(e[0][1], 1)], "axes": [round(e[1][0], 1), round(e[1][1], 1)],
                           "angle": round(e[2], 1), "fill": round(float(m.sum()) / max(1e-6, np.pi * e[1][0] * e[1][1] / 4), 3)}
            ys, xs = np.nonzero(m)
            objects.append({"id": len(objects), "kind": k["kind"], "word": k["word"], "dino_score": k["score"],
                            "sam_score": round(best[1], 3), "box": [round(v) for v in k["box"]], "mask": name,
                            "pixels": int(m.sum()), "centre": [round(float(xs.mean()), 1), round(float(ys.mean()), 1)],
                            "ellipse": ell})
    boxes_done = 0
    with torch.inference_mode():                    # the box hints (no DINO): one SAM2 mask per box
        for bx in boxes or []:
            try:
                box = np.array([float(v) for v in bx["box"]][:4], np.float32)
            except (KeyError, TypeError, ValueError):
                continue
            box = np.clip(box, 0, [W, H, W, H]).astype(np.float32)
            if box[2] - box[0] < 3 or box[3] - box[1] < 3:
                continue
            kind = str(bx.get("kind") or "part")
            ctr = np.array([[(box[0] + box[2]) / 2, (box[1] + box[3]) / 2]], np.float32)
            ms, scores, _ = sam.predict(point_coords=ctr, point_labels=np.array([1], np.int32), box=box[None, :], multimask_output=True)
            best = None
            for m, s in zip(ms.astype(bool), scores):
                ys, xs = np.nonzero(m)
                if not len(xs):
                    continue
                inside = float(((xs >= box[0]) & (xs <= box[2]) & (ys >= box[1]) & (ys <= box[3])).mean())
                if inside >= 0.55 and (best is None or s > best[1]):
                    best = (m, float(s), inside)
            if best is None:
                continue
            m = best[0]
            i = counts.get(kind + "_box", 0)
            counts[kind + "_box"] = i + 1
            name = f"{kind}_box_{i}.png"
            ok, png = cv2.imencode(".png", m.astype(np.uint8) * 255)
            masks[name] = base64.b64encode(png.tobytes()).decode("ascii")
            ys, xs = np.nonzero(m)
            objects.append({"id": len(objects), "kind": kind, "word": None, "dino_score": None, "source": "box",
                            "prompt_box": [round(float(v)) for v in box], "tag": bx.get("tag"),
                            "sam_score": round(best[1], 3), "box": [int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())],
                            "mask": name, "pixels": int(m.sum()),
                            "centre": [round(float(xs.mean()), 1), round(float(ys.mean()), 1)], "ellipse": None})
            boxes_done += 1
    sam_s = round(time.perf_counter() - t, 3)
    return {"schema": "autorig.vehicle.masks/1", "size": [W, H], "queries": qlist, "boxes_done": boxes_done,
            "thresholds": {"box": box_threshold, "text": text_threshold, "nms_iou": nms_iou},
            "raw": raw, "objects": objects, "masks": masks,
            "seconds": {"dino": dino_s, "sam": sam_s, "total": round(time.perf_counter() - t0, 3)},
            "device": STATE["device"], "gpu": STATE["gpu"]}


class H(BaseHTTPRequestHandler):
    def _send(self, code, doc):
        data = json.dumps(doc).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):            # one line per request, no client noise
        if args and "/health" in str(args[0]):    # the fleet API and the client poll it; keep the log for /masks
            return
        print(f"[masks] {self.address_string()} {fmt % args}", flush=True)

    def do_GET(self):
        if self.path.startswith("/health"):
            return self._send(200, {"ok": not STATE["load_error"], "loaded": STATE["loaded"], "device": STATE["device"],
                                    "gpu": STATE["gpu"], "box": STATE["box"], "requests": STATE["requests"],
                                    "errors": STATE["errors"], "inflight": STATE["inflight"],
                                    "last_seconds": STATE["last_seconds"], "vram_mb": _vram_mb(),
                                    "idle_unload_s": STATE["idle_unload_s"], "loads": STATE["loads"],
                                    "load_error": STATE["load_error"]})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/masks"):
            return self._send(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            data = base64.b64decode(body["image_b64"])
            img = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
            if img is None:
                return self._send(400, {"error": "not an image"})
            STATE["inflight"] += 1
            STATE["requests"] += 1
            try:
                t0 = time.perf_counter()
                if not STATE["loaded"]:
                    _load_safe()                   # idle-unloaded (or still loading): load now, a few seconds
                if not STATE["loaded"]:
                    STATE["errors"] += 1
                    return self._send(503, {"error": "models not loaded: " + (STATE["load_error"] or "loading")})
                with STATE["lock"]:
                    if not STATE["loaded"]:
                        STATE["errors"] += 1
                        return self._send(503, {"error": "models unloaded"})
                    queries = (body.get("queries") or {}) if body.get("boxes") else \
                        (body.get("queries") or {"wheel": "wheel,tire"})      # the text path: as before
                    doc = segment(img, queries,
                                  float(body.get("box_threshold", 0.3)), float(body.get("wheel_threshold", 0.4)),
                                  float(body.get("text_threshold", 0.25)), float(body.get("nms_iou", 0.5)), float(body.get("max_share", 0.35)),
                                  boxes=body.get("boxes") or None)
                    STATE["last_used"] = time.time()
                doc["box_name"] = STATE["box"]
                doc["seconds"]["request"] = round(time.perf_counter() - t0, 3)
                STATE["last_seconds"] = doc["seconds"]["request"]
            finally:
                STATE["inflight"] -= 1
            self._send(200, doc)
        except Exception as exc:                   # noqa: BLE001
            STATE["errors"] += 1
            self._send(500, {"error": str(exc)[:400]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("VEHICLE_SEG_ROOT", r"R:\autorig\.runtime\vehicle-seg"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("VEHICLE_MASKS_PORT", "8989")))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--idle-unload", type=float, default=float(os.environ.get("VEHICLE_MASKS_IDLE_UNLOAD", "0")),
                    help="free the GPU after this many idle seconds (0 = keep loaded)")
    ap.add_argument("--box", default="", help="box name reported in /health and masks.json")
    a = ap.parse_args()
    STATE.update(root=a.root, want_device=a.device, idle_unload_s=max(0.0, a.idle_unload))
    if a.box:
        STATE["box"] = a.box
    srv = ThreadingHTTPServer((a.host, a.port), H)
    threading.Thread(target=_load_safe, daemon=True).start()
    threading.Thread(target=_idle_watch, daemon=True).start()
    print(json.dumps({"listening": f"http://{a.host}:{a.port}", "root": a.root}), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
