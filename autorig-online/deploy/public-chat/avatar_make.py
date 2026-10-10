"""Make a round profile avatar from a person's 3D model (Public chat · V3, 2026-10-10).

Owner, 2026-10-10: «сделай всем юзерам автоматически аватарку из их залитой 3д модели последней чтобы был кроп
превью круглый, кроп который только модель охватывает в круг ровно чётко».

    PYTHONPATH=/srv/autorig/data/motion_transfer python3 -P avatar_make.py --task <uuid> --out-dir <dir>
        (--proj <lit.png> <mask.png> | --glb <model.glb> | --poster <url or file>)

Sources, best first (the caller tries them in this order):
    proj    a Motion Transfer run's proj/front_lit.png + proj/front_mask.png (the run already knows the front)
    glb     the task's GLB rendered with the MT CPU rasterizer (mt.render, read-only use); the facing is checked
            by silhouette width and toe direction, because prepared models do not always face +Z
    poster  the task's poster with the studio background cut away (GrabCut), the last resort
The model is then cropped to its minimal enclosing circle with an even margin, laid on a dark disc and written as
<task>-256.webp, <task>-128.webp and <task>-64.webp with a transparent outside. NudeNet checks the result; an
exposed body never becomes an avatar. Prints one JSON line: {"state": "ok"|"nsfw"|"failed", ...}.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.request

import cv2
import numpy as np

SIZES = (256, 128, 64)
MASTER = 512
MARGIN = 0.07
EXPLICIT = {"FEMALE_GENITALIA_EXPOSED", "MALE_GENITALIA_EXPOSED", "ANUS_EXPOSED"}
SUGGESTIVE = {"FEMALE_BREAST_EXPOSED", "BUTTOCKS_EXPOSED"}


def emit(doc: dict) -> int:
    print(json.dumps(doc, ensure_ascii=False))
    return 0 if doc.get("state") == "ok" else 3


# ------------------------------------------------------------------ sources -> RGBA

def from_proj(lit_path: str, mask_path: str) -> np.ndarray:
    lit = cv2.imread(lit_path, cv2.IMREAD_COLOR)
    mask = cv2.imread(mask_path, cv2.IMREAD_GRAYSCALE)
    if lit is None or mask is None:
        raise RuntimeError("projection files unreadable")
    if mask.shape[:2] != lit.shape[:2]:
        mask = cv2.resize(mask, (lit.shape[1], lit.shape[0]), interpolation=cv2.INTER_NEAREST)
    rgb = cv2.cvtColor(lit, cv2.COLOR_BGR2RGB)
    return np.dstack([rgb, mask])


def _mask_of(R, mesh, yaw: float, size: int) -> np.ndarray:
    cam = R.cameras_for(mesh.positions, size=size, names=("front",), yaw=yaw, frame_points=mesh.positions)["front"]
    m = np.asarray(R.mask_png(R.render_view(mesh, cam)))
    return (m[..., 0] if m.ndim == 3 else m) > 127


def _width(m: np.ndarray) -> int:
    cols = np.nonzero(m.any(axis=0))[0]
    return int(cols.max() - cols.min() + 1) if len(cols) else 0


def toe_direction(m: np.ndarray) -> int:
    """+1 when the feet point to the image's right, -1 to its left, 0 when unclear. In a side view the toes reach
    much further in front of the ankle than the heel (even a high heel) reaches behind it."""
    rows = np.nonzero(m.any(axis=1))[0]
    if not len(rows):
        return 0
    top, bottom = int(rows.min()), int(rows.max())
    h = bottom - top + 1
    if h < 60:
        return 0
    soles = m[bottom - max(2, int(0.035 * h)):bottom + 1]
    ankles = m[bottom - int(0.17 * h):bottom - int(0.10 * h) + 1]
    fx = np.nonzero(soles.any(axis=0))[0]
    ax = np.nonzero(ankles)[1]
    if not len(fx) or not len(ax):
        return 0
    ankle = float(np.median(ax))
    ahead, behind = float(fx.max()) - ankle, ankle - float(fx.min())
    if abs(ahead - behind) < 0.15 * (fx.max() - fx.min() + 1):
        return 0
    return 1 if ahead > behind else -1


def facing_yaw(R, mesh) -> tuple:
    """Yaw that shows the model's front. Prepared models should face +Z, but not all do. A much wider silhouette
    from the side means the model faces +X or -X; the toes then say which (in the +Z view the image's right is +X).
    Otherwise the toes in the side view tell +Z from -Z (there the model's +Z points to the image's left)."""
    front = _mask_of(R, mesh, R.FORWARD_YAW["+z"], 192)
    side = _mask_of(R, mesh, R.FORWARD_YAW["+x"], 192)
    wf, ws = _width(front), _width(side)
    if wf and ws > 1.3 * wf:
        fwd = "-x" if toe_direction(front) < 0 else "+x"
    else:
        fwd = "-z" if toe_direction(side) > 0 else "+z"
    return R.FORWARD_YAW[fwd], fwd


def from_glb(path: str) -> tuple:
    from mt import render as R
    from mt.glb import load_glb

    with open(path, "rb") as handle:
        mesh = load_glb(handle.read())
    yaw, fwd = facing_yaw(R, mesh)
    cam = R.cameras_for(mesh.positions, size=MASTER, names=("front",), yaw=yaw, frame_points=mesh.positions)["front"]
    view = R.render_view(mesh, cam)
    lit = np.asarray(R.lit_png(view), dtype=np.uint8)[..., :3]
    mask = np.asarray(R.mask_png(view), dtype=np.uint8)
    if mask.ndim == 3:
        mask = mask[..., 0]
    return np.dstack([lit, mask]), fwd


def from_poster(src: str) -> np.ndarray:
    if src.startswith("http"):
        req = urllib.request.Request(src, headers={"User-Agent": "autorig-public-chat-avatar/1"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = np.frombuffer(r.read(), np.uint8)
        img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    else:
        img = cv2.imread(src, cv2.IMREAD_COLOR)
    if img is None:
        raise RuntimeError("poster unreadable")
    scale = 480.0 / max(img.shape[:2])
    if scale < 1:
        img = cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)), interpolation=cv2.INTER_AREA)
    h, w = img.shape[:2]
    border = np.concatenate([img[:6].reshape(-1, 3), img[-6:].reshape(-1, 3), img[:, :6].reshape(-1, 3),
                             img[:, -6:].reshape(-1, 3)]).astype(np.float32)
    bg = np.median(border, axis=0)
    diff = np.linalg.norm(img.astype(np.float32) - bg, axis=2)
    fg = diff > max(18.0, float(np.percentile(np.linalg.norm(border - bg, axis=1), 98)) * 1.6)
    ys, xs = np.nonzero(fg)
    if len(xs) < 200:
        raise RuntimeError("no subject on the poster")
    x0, x1 = np.percentile(xs, [0.5, 99.5]).astype(int)
    y0, y1 = np.percentile(ys, [0.5, 99.5]).astype(int)
    pad = 6
    rect = (max(1, x0 - pad), max(1, y0 - pad), min(w - 2, x1 + pad) - max(1, x0 - pad),
            min(h - 2, y1 + pad) - max(1, y0 - pad))
    mask = np.where(fg, cv2.GC_PR_FGD, cv2.GC_PR_BGD).astype(np.uint8)
    outside = np.ones((h, w), bool)
    outside[rect[1]:rect[1] + rect[3], rect[0]:rect[0] + rect[2]] = False
    mask[outside] = cv2.GC_BGD
    bgd, fgd = np.zeros((1, 65), np.float64), np.zeros((1, 65), np.float64)
    cv2.grabCut(img, mask, rect, bgd, fgd, 4, cv2.GC_INIT_WITH_MASK)
    alpha = np.where((mask == cv2.GC_FGD) | (mask == cv2.GC_PR_FGD), 255, 0).astype(np.uint8)
    count, labels, stats, _ = cv2.connectedComponentsWithStats(alpha, 8)
    if count > 1:
        keep = 1 + int(np.argmax(stats[1:, cv2.CC_STAT_AREA]))
        alpha = np.where(labels == keep, 255, 0).astype(np.uint8)
    alpha = cv2.GaussianBlur(alpha, (3, 3), 0)
    return np.dstack([cv2.cvtColor(img, cv2.COLOR_BGR2RGB), alpha])


# ------------------------------------------------------------------ circle crop

def disc_background(size: int) -> np.ndarray:
    yy, xx = np.mgrid[0:size, 0:size].astype(np.float32)
    c = (size - 1) / 2.0
    d = np.sqrt((xx - c) ** 2 + ((yy - c * 0.82) ** 2)) / (size * 0.62)
    d = np.clip(d, 0, 1)[..., None]
    inner = np.array([46, 56, 88], np.float32)
    outer = np.array([13, 16, 26], np.float32)
    return inner * (1 - d) + outer * d


def circle_avatar(rgba: np.ndarray) -> tuple:
    alpha = rgba[..., 3]
    ys, xs = np.nonzero(alpha > 40)
    if len(xs) < 60:
        raise RuntimeError("empty silhouette")
    pts = np.column_stack([xs, ys]).astype(np.float32)
    hull = cv2.convexHull(pts)
    (cx, cy), r = cv2.minEnclosingCircle(hull)
    radius = r * (1 + MARGIN) + 1.0
    scale = (MASTER / 2.0) / radius
    m = np.float32([[scale, 0, MASTER / 2.0 - cx * scale], [0, scale, MASTER / 2.0 - cy * scale]])
    warped = cv2.warpAffine(rgba, m, (MASTER, MASTER), flags=cv2.INTER_AREA if scale < 1 else cv2.INTER_CUBIC,
                            borderMode=cv2.BORDER_CONSTANT, borderValue=(0, 0, 0, 0))
    a = (warped[..., 3:4].astype(np.float32) / 255.0)
    bg = disc_background(MASTER)
    rgb = warped[..., :3].astype(np.float32) * a + bg * (1 - a)
    yy, xx = np.mgrid[0:MASTER, 0:MASTER].astype(np.float32)
    c = (MASTER - 1) / 2.0
    dist = np.sqrt((xx - c) ** 2 + (yy - c) ** 2)
    disc = np.clip(MASTER / 2.0 - dist, 0.0, 1.0)            # one-pixel anti-aliased edge
    out = np.dstack([np.clip(rgb, 0, 255), disc * 255]).astype(np.uint8)
    fill = float((alpha > 40).sum()) / float(math.pi * r * r) if r > 0 else 0.0
    return out, {"circle_px": [round(cx, 1), round(cy, 1), round(r, 1)], "fill": round(fill, 3)}


def nudity(rgb: np.ndarray, lenient: bool = False) -> tuple:
    """(flagged, score). `lenient` is for tasks the site already rated safe: an untextured clay figure often makes
    the detector see skin, so only a clear explicit hit blocks the avatar there."""
    try:
        from nudenet import NudeDetector
    except Exception as exc:  # noqa: BLE001 - no detector: the caller only accepts tasks rated safe
        return None, f"nudenet unavailable: {type(exc).__name__}"
    ok, buf = cv2.imencode(".jpg", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR), [cv2.IMWRITE_JPEG_QUALITY, 92])
    found = NudeDetector().detect(buf.tobytes()) or []
    explicit = max((d["score"] for d in found if d.get("class") in EXPLICIT), default=0.0)
    suggestive = max((d["score"] for d in found if d.get("class") in SUGGESTIVE), default=0.0)
    flagged = explicit >= 0.5 if lenient else (explicit >= 0.2 or suggestive >= 0.35)
    return flagged, round(float(max(explicit, suggestive)), 3)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--proj", nargs=2)
    ap.add_argument("--glb")
    ap.add_argument("--poster")
    ap.add_argument("--require-nudenet", action="store_true")
    ap.add_argument("--rating", default="unknown", help="the site's content_rating of the task (safe|unknown|...)")
    args = ap.parse_args()
    t0 = time.time()
    info = {"task": args.task}
    try:
        if args.proj:
            rgba, info["source"] = from_proj(*args.proj), "proj"
        elif args.glb:
            rgba, info["facing"] = from_glb(args.glb)
            info["source"] = "glb"
        elif args.poster:
            rgba, info["source"] = from_poster(args.poster), "poster"
        else:
            return emit({"state": "failed", "reason": "no source", **info})
        avatar, geo = circle_avatar(rgba)
        info.update(geo)
    except Exception as exc:  # noqa: BLE001
        return emit({"state": "failed", "reason": f"{type(exc).__name__}: {exc}"[:300], **info})
    safe = args.rating == "safe"
    flagged, score = nudity(avatar[..., :3], lenient=safe)
    info["nudity"] = score
    info["rating"] = args.rating
    if flagged or (flagged is None and (args.require_nudenet or not safe)):
        return emit({"state": "nsfw", **info, "seconds": round(time.time() - t0, 2)})
    os.makedirs(args.out_dir, exist_ok=True)
    bgra = cv2.cvtColor(avatar, cv2.COLOR_RGBA2BGRA)
    for size in SIZES:
        img = cv2.resize(bgra, (size, size), interpolation=cv2.INTER_AREA)
        tmp = os.path.join(args.out_dir, f".{args.task}-{size}.{os.getpid()}.webp")
        cv2.imwrite(tmp, img, [cv2.IMWRITE_WEBP_QUALITY, 90])
        os.replace(tmp, os.path.join(args.out_dir, f"{args.task}-{size}.webp"))
    info["seconds"] = round(time.time() - t0, 2)
    return emit({"state": "ok", **info})


if __name__ == "__main__":
    sys.exit(main())
