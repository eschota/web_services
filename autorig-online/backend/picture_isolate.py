"""Isolate the subject of an uploaded picture (owner 2026-10-16: «изолировать её и отправлять на 3д генерацию»).

A generation task starts with a picture. Before Hunyuan 3D sees it, the subject is cut out here by the fleet's mask
service (GroundingDINO + SAM2, ``deploy/vehicle/masks_service.py``, HTTP ``/masks`` on each GPU box's tunnel):

* ``cutout.png`` (RGBA: the mask feathered 2 px as alpha, cropped to the bbox + 4 % margin, at most 1536 px) is written
  atomically next to the source, ``/srv/autorig/data/var/uploads/<token>/cutout.png`` (nginx serves it as
  ``/u/<token>/cutout.png``); ``isolate.json`` beside it records the box, the seconds and the failure, if any;
* the task page's picture viewer polls the cutout URL and fades it in; the generation hands Hunyuan the cutout when it
  exists, the original otherwise. A failure is silent for the person (``isolate.json`` keeps the reason) and the
  generation goes on with the original picture.

The mask boxes come from the fleet API (``services_object.masks``: url, health, loaded, GPU), a warm box first, the
owner's PC last, then the next box on an error: 5 s connect, 25 s per box. The request shape is
``mt/vehicle_v3.py::_masks_via_service`` (``image_b64`` JPEG, ``queries``, ``max_share``, optional ``boxes``).
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

UPLOAD_ROOT = Path(os.getenv("AUTORIG_PICTURE_UPLOAD_ROOT", os.getenv("UPLOAD_DIR", "/var/autorig/uploads")))
FLEET_URL = os.getenv("AUTORIG_FLEET_URL", "http://127.0.0.1:8255/api/fleet")
# The fallback list of the boxes' tunnel ports when the fleet API does not answer (mt/vehicle_v3.py MASKS_BOXES).
FALLBACK_BOXES = {"http://127.0.0.1:19802": "f2", "http://127.0.0.1:19801": "f1", "http://127.0.0.1:19818": "raptor",
                  "http://127.0.0.1:19812": "f12", "http://127.0.0.1:19813": "f13", "http://127.0.0.1:19805": "f5",
                  "http://127.0.0.1:19411": "worker-4090"}
# Every word is one GroundingDINO pass (~0.5 s warm on a 3080 Ti): the first word alone usually finds the subject
# (0.8 s), the rest of the list is the second attempt, the central box (SAM only, 0.2 s) the third.
QUERY_FIRST = os.getenv("AUTORIG_ISOLATE_QUERY_FIRST", "character")
QUERY_WORDS = os.getenv("AUTORIG_ISOLATE_QUERY", "person,animal,creature,figure,object")
BOX_THRESHOLD = float(os.getenv("AUTORIG_ISOLATE_BOX_THRESHOLD", "0.3"))
SEND_MAX_SIDE = int(os.getenv("AUTORIG_ISOLATE_SEND_SIDE", "896"))     # the picture sent to the mask box (the mask is scaled back)
CUTOUT_MAX_SIDE = 1536
MARGIN = 0.04
FEATHER_PX = 2
MIN_SHARE = float(os.getenv("AUTORIG_ISOLATE_MIN_SHARE", "0.03"))      # a mask under 3 % of the picture is not the subject
MAX_SHARE = 0.96                                                       # a mask over 96 % is the background
MIN_SAM = float(os.getenv("AUTORIG_ISOLATE_MIN_SAM", "0.5"))
CONNECT_S, TOTAL_S, MAX_BOXES = 5.0, 25.0, 3
DEADLINE_S = float(os.getenv("AUTORIG_ISOLATE_DEADLINE", "50"))
FLEET_TTL = 20.0
_TOKEN_URL = re.compile(r"/u/([0-9a-fA-F-]{36})/source\.(?:png|jpe?g|webp)$")

_FLEET: Dict[str, Any] = {"t": 0.0, "rows": []}
_INFLIGHT: Dict[str, int] = {}
_SECONDS: Dict[str, float] = {}                                         # url -> EWMA of the request seconds seen here
_JOBS: Dict[str, "asyncio.Task[Dict[str, Any]]"] = {}                   # token -> the running isolation


def token_of(url: str) -> Optional[str]:
    m = _TOKEN_URL.search(str(url or "").split("?", 1)[0])
    return m.group(1) if m else None


def source_path(token: str) -> Optional[Path]:
    d = UPLOAD_ROOT / token
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        p = d / f"source{ext}"
        if p.is_file():
            return p
    return None


def cutout_path(token: str) -> Path:
    return UPLOAD_ROOT / token / "cutout.png"


def report_path(token: str) -> Path:
    return UPLOAD_ROOT / token / "isolate.json"


def cutout_ready(token: str) -> bool:
    try:
        return cutout_path(token).stat().st_size > 0
    except OSError:
        return False


def report(token: str) -> Optional[Dict[str, Any]]:
    try:
        doc = json.loads(report_path(token).read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError):
        return None


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name("." + path.name + f".{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


# ----------------------------------------------------------------------------------------------- the mask boxes
async def mask_boxes(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """The fleet API's mask services: [{url, box, ok, loaded, gpu, last_s}], cached FLEET_TTL s."""
    now = time.time()
    if _FLEET["rows"] and now - _FLEET["t"] < FLEET_TTL:
        return _FLEET["rows"]
    rows: List[Dict[str, Any]] = []
    try:
        r = await client.get(FLEET_URL, timeout=httpx.Timeout(3.0))
        doc = r.json() if r.status_code == 200 else {}
        for b in doc.get("boxes_array") or []:
            m = ((b.get("services_object") or {}).get("masks")) or {}
            url = str(m.get("url_string") or "").rstrip("/")
            if not url:
                continue
            rows.append({"url": url, "box": str(b.get("id") or b.get("box") or url.rsplit(":", 1)[-1]),
                         "ok": bool(m.get("ok_bool")), "loaded": bool(m.get("loaded_bool")),
                         "gpu": str(m.get("gpu_string") or ""), "last_s": m.get("last_seconds_float")})
    except Exception:                                              # noqa: BLE001 - the static list below
        rows = []
    if not rows:
        rows = [{"url": u, "box": n, "ok": True, "loaded": False, "gpu": "", "last_s": None}
                for u, n in FALLBACK_BOXES.items()]
    _FLEET.update(t=now, rows=rows)
    return rows


def order_boxes(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Healthy boxes only: a warm box first, the least busy, the faster one; the owner's PC last."""
    def expected(r):
        if r["url"] in _SECONDS:
            return _SECONDS[r["url"]]
        if isinstance(r.get("last_s"), (int, float)):
            return float(r["last_s"])
        return 14.0 if "1080" in r.get("gpu", "") else 5.0

    live = [r for r in rows if r.get("ok")]
    return sorted(live, key=lambda r: (r["box"] == "worker-4090", not r.get("loaded"), _INFLIGHT.get(r["url"], 0),
                                       expected(r)))


# ------------------------------------------------------------------------------------------------- the isolation
def _encode_jpeg(img) -> Tuple[bytes, Tuple[int, int]]:
    import cv2

    h, w = img.shape[:2]
    scale = min(1.0, SEND_MAX_SIDE / max(h, w))
    small = cv2.resize(img, (max(1, round(w * scale)), max(1, round(h * scale))), interpolation=cv2.INTER_AREA) \
        if scale < 1.0 else img
    ok, jpg = cv2.imencode(".jpg", small, [cv2.IMWRITE_JPEG_QUALITY, 90])
    if not ok:
        raise RuntimeError("the picture cannot be encoded")
    return jpg.tobytes(), (small.shape[1], small.shape[0])


async def _ask(client: httpx.AsyncClient, row: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
    url = row["url"]
    _INFLIGHT[url] = _INFLIGHT.get(url, 0) + 1
    t0 = time.time()
    try:
        r = await asyncio.wait_for(client.post(url + "/masks", json=body,
                                               timeout=httpx.Timeout(TOTAL_S, connect=CONNECT_S)), TOTAL_S)
        if r.status_code != 200:
            raise RuntimeError(f"/masks answered {r.status_code}: {r.text[:120]}")
        doc = r.json()
    finally:
        _INFLIGHT[url] = max(0, _INFLIGHT.get(url, 1) - 1)
    took = time.time() - t0
    _SECONDS[url] = took if url not in _SECONDS else 0.6 * _SECONDS[url] + 0.4 * took
    return doc


def _pick(doc: Dict[str, Any], size: Tuple[int, int]) -> Optional[Dict[str, Any]]:
    """The largest plausible mask of the answer: not the background, not a speck, SAM sure enough."""
    area = float(size[0] * size[1])
    best = None
    for ob in doc.get("objects") or []:
        share = float(ob.get("pixels") or 0) / area
        if not (MIN_SHARE <= share <= MAX_SHARE) or float(ob.get("sam_score") or 0) < MIN_SAM:
            continue
        if ob.get("mask") not in (doc.get("masks") or {}):
            continue
        if best is None or ob["pixels"] > best["pixels"]:
            best = ob
    return best


def _build_cutout(img, mask_png: bytes) -> Tuple[bytes, Dict[str, Any]]:
    """RGBA cutout: the mask (scaled back to the picture) feathered 2 px as alpha, cropped to the bbox + 4 %."""
    import cv2
    import numpy as np

    h, w = img.shape[:2]
    m = cv2.imdecode(np.frombuffer(mask_png, np.uint8), cv2.IMREAD_GRAYSCALE)
    if m is None:
        raise RuntimeError("the mask cannot be decoded")
    if m.shape != (h, w):
        m = cv2.resize(m, (w, h), interpolation=cv2.INTER_LINEAR)
    m = (m > 127).astype(np.uint8) * 255
    ys, xs = np.nonzero(m)
    if not len(xs):
        raise RuntimeError("the mask is empty")
    x0, x1, y0, y1 = int(xs.min()), int(xs.max()) + 1, int(ys.min()), int(ys.max()) + 1
    mx, my = round((x1 - x0) * MARGIN), round((y1 - y0) * MARGIN)
    x0, y0, x1, y1 = max(0, x0 - mx), max(0, y0 - my), min(w, x1 + mx), min(h, y1 + my)
    alpha = cv2.GaussianBlur(m, (0, 0), FEATHER_PX / 2.0)[y0:y1, x0:x1]
    rgba = np.dstack([img[y0:y1, x0:x1], alpha])
    scale = CUTOUT_MAX_SIDE / max(rgba.shape[0], rgba.shape[1])
    if scale < 1.0:
        rgba = cv2.resize(rgba, (round(rgba.shape[1] * scale), round(rgba.shape[0] * scale)), interpolation=cv2.INTER_AREA)
    ok, png = cv2.imencode(".png", rgba)
    if not ok:
        raise RuntimeError("the cutout cannot be encoded")
    return png.tobytes(), {"bbox": [x0, y0, x1, y1], "size": [int(rgba.shape[1]), int(rgba.shape[0])],
                           "mask_share": round(float((m > 0).sum()) / (w * h), 3)}


async def isolate_file(src: Path, token: str) -> Dict[str, Any]:
    """Cut the subject out of ``src`` into ``<token>/cutout.png``; always returns the report (also written as
    isolate.json). Never raises: a failure is the report's ``error``."""
    import cv2
    import numpy as np

    t0 = time.time()
    rep: Dict[str, Any] = {"schema": "autorig.picture-isolate/1", "ok": False, "token": token, "started_at": t0}
    try:
        img = await asyncio.to_thread(lambda: cv2.imdecode(np.frombuffer(src.read_bytes(), np.uint8), cv2.IMREAD_COLOR))
        if img is None:
            raise RuntimeError("the picture is unreadable")
        jpg, (sw, sh) = await asyncio.to_thread(_encode_jpeg, img)
        b64 = base64.b64encode(jpg).decode("ascii")
        central = [{"kind": "subject", "box": [sw * 0.05, sh * 0.05, sw * 0.95, sh * 0.95]}]
        attempts = [("text-first", {"image_b64": b64, "queries": {"subject": QUERY_FIRST}, "box_threshold": BOX_THRESHOLD,
                                    "max_share": 1.0}),
                    ("text", {"image_b64": b64, "queries": {"subject": QUERY_WORDS}, "box_threshold": BOX_THRESHOLD,
                              "max_share": 1.0}),
                    ("central-box", {"image_b64": b64, "queries": {}, "boxes": central, "max_share": 1.0})]
        tried: List[str] = []
        picked, doc, row, used = None, None, None, ""
        async with httpx.AsyncClient() as client:
            rows = order_boxes(await mask_boxes(client))[:MAX_BOXES]
            if not rows:
                raise RuntimeError("no mask service answers")
            for mode, body in attempts:
                for row in rows:
                    if time.time() - t0 > DEADLINE_S:
                        break
                    try:
                        doc = await _ask(client, row, body)
                    except Exception as exc:                       # noqa: BLE001 - the next box
                        tried.append(f"{row['box']} {mode}: {type(exc).__name__}: {str(exc)[:100]}")
                        _SECONDS[row["url"]] = 99.0
                        continue
                    picked = _pick(doc, (sw, sh))
                    used = mode
                    if picked is not None:
                        break
                    tried.append(f"{row['box']} {mode}: no plausible mask ({len(doc.get('objects') or [])} objects)")
                    break                                           # the box answered; the next mode, not the next box
                if picked is not None:
                    break
        if picked is None or doc is None or row is None:
            raise RuntimeError("no subject mask: " + "; ".join(tried)[:300])
        png_mask = base64.b64decode(doc["masks"][picked["mask"]])
        png, info = await asyncio.to_thread(_build_cutout, img, png_mask)
        cutout_path(token).parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(cutout_path(token), png)
        rep.update(ok=True, box=row["box"], mode=used, sam_score=picked.get("sam_score"), dino_score=picked.get("dino_score"),
                   mask_seconds=(doc.get("seconds") or {}), fallbacks=tried or None, **info)
    except Exception as exc:                                       # noqa: BLE001
        rep["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    rep["seconds"] = round(time.time() - t0, 2)
    try:
        _atomic_write(report_path(token), json.dumps(rep, ensure_ascii=False).encode("utf-8"))
    except OSError:
        pass
    return rep


def kick(token: str) -> Optional["asyncio.Task[Dict[str, Any]]"]:
    """Start the isolation of ``token`` once (idempotent in this process); None when there is no source or a report."""
    job = _JOBS.get(token)
    if job is not None:
        return job
    if report(token) is not None:
        return None
    src = source_path(token)
    if src is None:
        return None
    job = asyncio.create_task(isolate_file(src, token))
    _JOBS[token] = job
    job.add_done_callback(lambda _t, _k=token: _JOBS.pop(_k, None))
    return job


async def ensure(token: str, wait_s: float) -> Optional[Dict[str, Any]]:
    """Start the isolation (if not started) and wait up to ``wait_s`` for it; the report, or None when it is still
    running (it goes on and writes the cutout when it ends)."""
    job = kick(token)
    if job is None:
        return report(token)
    try:
        return await asyncio.wait_for(asyncio.shield(job), max(0.1, wait_s))
    except asyncio.TimeoutError:
        return None
    except Exception as exc:                                       # noqa: BLE001
        return report(token) or {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
