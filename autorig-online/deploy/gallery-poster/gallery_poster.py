"""Gallery posters from the V3 side: one consistent 9:16 capture per task (Gallery · V3, 2026-10-11).

Owner, 2026-10-11, looking at «Recent Rigged Models» on the homepage: «а что тогда с галереей на главной? там должны
быть рендеры с вьювера V3». The old posters are a mix: grey studio frames of the classic converter, a blank white
silhouette, a stretched arm caught in an animation frame. Every card (homepage, /gallery, author pages, chat cards,
og:image) loads `/thumb/<task>`; the backend serves `posters-v3/<task>.jpg` first when it exists (main.py
`v3_poster_path`), so one file per task fixes every place at once.

What a capture is: the task's viewer GLB (the one the V3 viewer opens, not the animation frame of a video) rendered with
the Motion Transfer rasterizer (mt.render, read-only use) from the viewer's three-quarter camera, framed the same way for
every model, standing on the ground of one of the viewer's environments (static/env/backdrops/source) with a contact
shadow. The Unity viewer itself is not driven headless on the VPS: that is a 2 GB WebGL page per task on a CPU box.

    python3 -P gallery_poster.py loop            service: render the missing posters, newest and V3 tasks first
    python3 -P gallery_poster.py once [--task T] one pass (or one task) and exit
    python3 -P gallery_poster.py render --task T --glb G --out O.jpg      the child: one render, one JSON line
    python3 -P gallery_poster.py status          counts by state

A capture is never blank: the child checks the coverage, the model/background contrast (it tries the next environment
when a white model would vanish into a bright backdrop) and the colour spread. A task without any usable source is
recorded as `no_source` / `failed` in posters.sqlite3 (the triage list reads that: mt/triage.py `poster_*` defects) and
keeps whatever poster it had.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

W, H = 720, 1280
RENDER = 1024
ENVS = ("ancient_ruins", "mediterranean_courtyard", "pine_forest_trail", "studio_white_softbox", "sci_fi_hangar",
        "savanna_acacia_plain")
OUT_DIR = Path(os.getenv("GP_OUT", "/srv/autorig/data/static/posters-v3"))
SITE_DB = os.getenv("GP_SITE_DB", "/srv/autorig/data/db/autorig.db")
MT_ROOT = Path(os.getenv("GP_MT_ROOT", "/srv/autorig/data/motion_transfer"))
GLB_CACHE = Path(os.getenv("GP_GLB_CACHE", "/srv/autorig/data/static/glb_cache"))
TASK_CACHE = Path(os.getenv("GP_TASK_CACHE", "/srv/autorig/data/static/tasks"))
BACKDROPS = [Path(p) for p in os.getenv(
    "GP_BACKDROPS", "/srv/autorig/live/static/env/backdrops/source:"
                    "/srv/autorig/current/autorig-online/static/env/backdrops/source").split(":") if p]
BACKEND = os.getenv("GP_BACKEND", "http://127.0.0.1:8200").rstrip("/")
RENDER_TIMEOUT_S = 150
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
RETRY_FAILED_S = 6 * 3600
VERSION = 3        # bump when the look changes: every poster is rebuilt


# ====================================================================================================== the render
def _mask_of(R, mesh, yaw, size):
    import numpy as np
    cam = R.cameras_for(mesh.positions, size=size, names=("front",), yaw=yaw, frame_points=mesh.positions)["front"]
    m = np.asarray(R.mask_png(R.render_view(mesh, cam)))
    return (m[..., 0] if m.ndim == 3 else m) > 127


def _width(m):
    import numpy as np
    cols = np.nonzero(m.any(axis=0))[0]
    return int(cols.max() - cols.min() + 1) if len(cols) else 0


def toe_direction(m):
    """+1 when the feet point to the image's right in a side view, -1 left, 0 unclear."""
    import numpy as np
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


def facing_yaw(R, mesh):
    front = _mask_of(R, mesh, R.FORWARD_YAW["+z"], 192)
    side = _mask_of(R, mesh, R.FORWARD_YAW["+x"], 192)
    wf, ws = _width(front), _width(side)
    if wf and ws > 1.3 * wf:
        fwd = "-x" if toe_direction(front) < 0 else "+x"
    else:
        fwd = "-z" if toe_direction(side) > 0 else "+z"
    return R.FORWARD_YAW[fwd], fwd


def render_model(glb_path: str):
    """The model from the viewer's three-quarter camera as RGBA (transparent background), plus facing and size."""
    import cv2
    import numpy as np
    from mt import render as R
    from mt.glb import load_glb

    with open(glb_path, "rb") as handle:
        mesh = load_glb(handle.read())
    if len(mesh.faces) < 20:
        raise RuntimeError("no geometry")
    yaw, fwd = facing_yaw(R, mesh)
    cam = R.persp_camera(mesh.positions, size=RENDER, yaw=yaw, name="persp")
    view = R.render_view(mesh, cam)
    lit = np.asarray(R.lit_png(view), dtype=np.uint8)[..., :3]
    mask = np.asarray(R.mask_png(view), dtype=np.uint8)
    if mask.ndim == 3:
        mask = mask[..., 0]
    return np.dstack([lit, mask]), {"facing": fwd, "tris": int(len(mesh.faces))}


def load_backdrop(name: str):
    import cv2
    for root in BACKDROPS:
        path = root / f"{name}.jpg"
        if path.is_file():
            img = cv2.imread(str(path), cv2.IMREAD_COLOR)
            if img is not None:
                return img
    raise RuntimeError(f"backdrop {name} not found")


def luminance(rgb):
    return float((0.2126 * rgb[..., 0] + 0.7152 * rgb[..., 1] + 0.0722 * rgb[..., 2]).mean() / 255.0)


def compose(rgba, env: str, seed: int):
    """Stand the model on the environment: tight crop of its alpha, one framing rule for every model, a contact
    shadow, a faint tint from the environment. Returns (BGR image, info)."""
    import cv2
    import numpy as np

    alpha = rgba[..., 3]
    ys, xs = np.nonzero(alpha > 40)
    if len(xs) < 400:
        raise RuntimeError("empty silhouette")
    x0, x1, y0, y1 = int(xs.min()), int(xs.max()) + 1, int(ys.min()), int(ys.max()) + 1
    crop = rgba[y0:y1, x0:x1].copy()
    ch, cw = crop.shape[:2]
    if not (0.2 < ch / cw < 7.0):
        raise RuntimeError(f"odd silhouette {cw}x{ch}")
    back = load_backdrop(env)
    sh, sw = back.shape[:2]
    bw = int(sh * W / H)                                   # the middle 9:16 of the 16:9 environment
    left = max(0, (sw - bw) // 2)
    canvas = cv2.resize(back[:, left:left + bw], (W, H), interpolation=cv2.INTER_CUBIC).astype(np.float32)
    yy, xx = np.mgrid[0:H, 0:W].astype(np.float32)
    vig = 1.0 - 0.22 * np.clip(((xx - W / 2) / (W * 0.75)) ** 2 + ((yy - H * 0.5) / (H * 0.75)) ** 2, 0, 1)
    canvas *= vig[..., None]
    # framing: the same for every model (a standing figure fills ~64 % of the height, a wide one is limited by width)
    scale = min(0.64 * H / ch, 0.90 * W / cw)
    nw, nh = max(2, int(round(cw * scale))), max(2, int(round(ch * scale)))
    small = cv2.resize(crop, (nw, nh), interpolation=cv2.INTER_AREA)
    feet_y = int(H * 0.835)
    top = feet_y - nh
    left_x = (W - nw) // 2
    top = max(int(H * 0.04), top)
    feet_y = top + nh
    # contact shadow
    shadow = np.zeros((H, W), np.float32)
    cv2.ellipse(shadow, (W // 2, feet_y - 6), (int(nw * 0.36), max(8, int(H * 0.018))), 0, 0, 360, 1.0, -1)
    shadow = cv2.GaussianBlur(shadow, (0, 0), 16)
    canvas *= (1.0 - 0.55 * shadow)[..., None]
    region = canvas[top:top + nh, left_x:left_x + nw]
    bg_lum = luminance(region[..., ::-1])
    model_rgb = small[..., :3].astype(np.float32)                        # lit_png is RGB
    a = (small[..., 3:4].astype(np.float32) / 255.0)
    model_lum = float((model_rgb * a).sum() / max(1.0, a.sum() * 3) / 255.0)
    tint = region.reshape(-1, 3).mean(0)[::-1]
    model_rgb = model_rgb * 0.95 + tint * 0.05
    contrast = abs(model_lum - bg_lum)
    spread = float(model_rgb[a[..., 0] > 0.5].std()) / 255.0 if (a > 0.5).any() else 0.0
    region[:] = region * (1 - a) + model_rgb[..., ::-1] * a
    out = np.clip(canvas, 0, 255).astype(np.uint8)
    coverage = float((small[..., 3] > 127).sum()) / (W * H)
    return out, {"env": env, "contrast": round(contrast, 3), "spread": round(spread, 3), "coverage": round(coverage, 3),
                 "size": [nw, nh]}


def cmd_render(args) -> int:
    import cv2
    t0 = time.time()
    info = {"task": args.task, "version": VERSION}
    try:
        rgba, geo = render_model(args.glb)
        info.update(geo)
        seed = int(hashlib.sha256(args.task.encode()).hexdigest()[:8], 16)
        start = seed % len(ENVS)
        best = None
        for k in range(len(ENVS)):
            env = ENVS[(start + k) % len(ENVS)]
            img, meta = compose(rgba, env, seed)
            if best is None or meta["contrast"] > best[1]["contrast"]:
                best = (img, meta)
            if meta["contrast"] >= 0.14:
                break                                    # the model reads against this environment
        img, meta = best
        info.update(meta)
        if meta["coverage"] < 0.03:
            raise RuntimeError(f"model too small in frame ({meta['coverage']})")
        if meta["spread"] < 0.02 and meta["contrast"] < 0.06:
            raise RuntimeError("blank capture")
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"state": "failed", "reason": f"{type(exc).__name__}: {exc}"[:300], **info}))
        return 3
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.stem}.{os.getpid()}.jpg")
    cv2.imwrite(str(tmp), img, [cv2.IMWRITE_JPEG_QUALITY, 88, cv2.IMWRITE_JPEG_PROGRESSIVE, 1])
    if tmp.stat().st_size < 8000:
        tmp.unlink()
        print(json.dumps({"state": "failed", "reason": "image too small", **info}))
        return 3
    os.replace(tmp, out)
    info["seconds"] = round(time.time() - t0, 2)
    print(json.dumps({"state": "ok", **info}))
    return 0


# ================================================================================================== the orchestrator
def db() -> sqlite3.Connection:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(OUT_DIR / "posters.sqlite3"), timeout=20)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS posters (task_id TEXT PRIMARY KEY, state TEXT NOT NULL, reason TEXT, "
                "source TEXT, sig TEXT, version INTEGER, env TEXT, ts REAL NOT NULL, kind TEXT)")
    return con


def site() -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{SITE_DB}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def glb_from_url(url: str):
    m = re.search(r"/api/mt/files/([0-9a-f]{20})/([A-Za-z0-9_./-]+\.glb)$", str(url or ""))
    if not m or ".." in m.group(2):
        return None
    path = MT_ROOT / "runs" / m.group(1) / m.group(2)
    return path if path.is_file() else None


def source_glb(task_id: str, viewer_url: str):
    """The model the V3 viewer opens: the run's rigged GLB for V3 tasks, else the viewer-prepared GLB."""
    for path in (glb_from_url(viewer_url), GLB_CACHE / f"{task_id}_prepared_viewer.glb",
                 GLB_CACHE / f"{task_id}_prepared.glb", TASK_CACHE / task_id / "model_prepared.glb",
                 GLB_CACHE / f"{task_id}_animations.glb", TASK_CACHE / task_id / "all_animations.glb"):
        if path is not None and path.is_file() and path.stat().st_size > 2000:
            return path
    return None


def candidates(con_site, limit: int):
    rows = con_site.execute(
        "SELECT id, pipeline_kind, viewer_prepared_glb_url, content_rating, created_at FROM tasks "
        "WHERE status='done' AND is_public=1 AND (content_rating IS NULL OR content_rating != 'adult') "
        "ORDER BY (pipeline_kind='v3') DESC, created_at DESC LIMIT ?", (limit,)).fetchall()
    return rows


def poster_path(task_id: str) -> Path:
    return OUT_DIR / f"{task_id}.jpg"


def record(con, task_id, state, reason="", source="", sig="", env="", kind=""):
    con.execute("INSERT OR REPLACE INTO posters(task_id, state, reason, source, sig, version, env, ts, kind) "
                "VALUES (?,?,?,?,?,?,?,?,?)", (task_id, state, reason[:300], source, sig, VERSION, env, time.time(), kind))
    con.commit()


def process(con, row) -> str:
    tid = row["id"]
    glb = source_glb(tid, row["viewer_prepared_glb_url"] or "")
    prev = con.execute("SELECT * FROM posters WHERE task_id=?", (tid,)).fetchone()
    if glb is None:
        if prev is None or prev["state"] != "no_source":
            record(con, tid, "no_source", "no viewer GLB on disk", kind=row["pipeline_kind"] or "")
        return "no_source"
    st = glb.stat()
    sig = f"{glb.name}:{st.st_size}:{int(st.st_mtime)}"
    if prev is not None and prev["version"] == VERSION and prev["sig"] == sig:
        if prev["state"] == "ok" and poster_path(tid).is_file():
            return "skip"
        if prev["state"] == "failed" and time.time() - float(prev["ts"]) < RETRY_FAILED_S:
            return "skip"
    cmd = ["nice", "-n", "15", sys.executable, "-P", str(Path(__file__).resolve()), "render", "--task", tid,
           "--glb", str(glb), "--out", str(poster_path(tid))]
    env = dict(os.environ, PYTHONPATH=str(MT_ROOT), OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=RENDER_TIMEOUT_S, env=env)
        lines = proc.stdout.strip().splitlines()
        res = json.loads(lines[-1]) if lines else {"state": "failed", "reason": proc.stderr[-200:] or "no output"}
    except subprocess.TimeoutExpired:
        res = {"state": "failed", "reason": "timeout"}
    except (ValueError, OSError) as exc:
        res = {"state": "failed", "reason": f"{type(exc).__name__}: {exc}"}
    state = "ok" if res.get("state") == "ok" else "failed"
    record(con, tid, state, str(res.get("reason") or ""), glb.name, sig, str(res.get("env") or ""),
           row["pipeline_kind"] or "")
    if state == "failed" and poster_path(tid).is_file():
        with contextlib.suppress(OSError):
            poster_path(tid).unlink()                    # an older capture of a model that now fails: do not keep it
    return state


def cmd_once(args) -> int:
    con = db()
    counts: dict = {}
    with contextlib.closing(site()) as cs:
        if args.task:
            rows = cs.execute("SELECT id, pipeline_kind, viewer_prepared_glb_url, content_rating, created_at FROM tasks "
                              "WHERE id=?", (args.task,)).fetchall()
        else:
            rows = candidates(cs, args.limit)
    for row in rows:
        result = process(con, row)
        counts[result] = counts.get(result, 0) + 1
        if result not in ("skip", "no_source"):
            print(row["id"], result, flush=True)
            time.sleep(args.pause)
    print(json.dumps(counts))
    return 0


def cmd_loop(args) -> int:
    while True:
        try:
            cmd_once(argparse.Namespace(task=None, limit=args.limit, pause=args.pause))
        except Exception as exc:  # noqa: BLE001
            print(f"loop error: {type(exc).__name__}: {exc}", flush=True)
        time.sleep(args.interval)


def cmd_status(args) -> int:
    con = db()
    print(json.dumps({r[0]: r[1] for r in con.execute("SELECT state, COUNT(*) FROM posters GROUP BY state")}))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("render")
    r.add_argument("--task", required=True)
    r.add_argument("--glb", required=True)
    r.add_argument("--out", required=True)
    o = sub.add_parser("once")
    o.add_argument("--task")
    o.add_argument("--limit", type=int, default=3000)
    o.add_argument("--pause", type=float, default=0.5)
    lo = sub.add_parser("loop")
    lo.add_argument("--limit", type=int, default=3000)
    lo.add_argument("--pause", type=float, default=1.0)
    lo.add_argument("--interval", type=float, default=90.0)
    sub.add_parser("status")
    args = ap.parse_args()
    return {"render": cmd_render, "once": cmd_once, "loop": cmd_loop, "status": cmd_status}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
