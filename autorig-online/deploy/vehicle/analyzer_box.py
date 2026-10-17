r"""The common GPU analyser, box side (contract V3.2 §1, owner 2026-10-17: «сделать анализатор в конвейере общим, который
сразу и воксели подкрасит, и на 3д модели обведёт объекты, материалы»). Loaded by masks_service.py for `POST /analyze`
on the same models it keeps for `/masks` (GroundingDINO-tiny + SAM2.1-tiny), under the same lock, on the box's GPU.

    POST /analyze {"kind": "mesh", "mesh_npz_b64": <npz P (V,3) f32 canonical frame, F (M,3) i32, C (V,3) u8>,
                   "views": 6, "size": 448, "pitch_deg": 12, "vocab": {"head": "head", "foot": "foot,shoe,boot", ...},
                   "materials": {"skin": "skin", ...} | null, "box_threshold": 0.25, "text_threshold": 0.2,
                   "min_score": 0.15, "max_share": 0.9, "batch": 4, "return_maps": false}
               -> {"votes_b64" (M x L f16), "seen_b64" (M f16), "labels", "mat_votes_b64", "mat_labels",
                   "views": [{name, objects: [{label, phrase, score, sam, box, pixels}]}], "seconds": {...}}
    POST /analyze {"kind": "picture", "image_b64": ..., "vocab": ..., "materials": ...}
               -> {"objects": [... "mask": "<label>_<i>.png"], "masks": {name: png b64}, "object_map_b64",
                   "material_map_b64", "labels", "mat_labels", "seconds"}

Mesh path: the views (orthographic, `views` yaws around the canonical model that faces +Z, looking down `pitch_deg`)
are rasterised on the GPU in torch (a barycentric splat with a 64-bit depth|face z-buffer, the pixel -> face map),
shaded flat from the vertex colours (the textures sampled by the caller); GroundingDINO gets ONE caption with every
phrase of the vocabulary for a batch of views (one pass instead of one pass per word); SAM2 takes every box of a view
in one call; in a pixel the smallest mask wins (a hand inside an arm); every pixel votes for its face with DINO x SAM
x |n . view|. The same for the material phrases (the Material-ID). Nothing is written on the box.
"""
from __future__ import annotations

import base64
import io
import math
import time

import cv2
import numpy as np

VERSION = "analyzer-box/3 2026-10-17"


def _b64(a: np.ndarray) -> str:
    return base64.b64encode(np.ascontiguousarray(a).tobytes()).decode("ascii")


def _png_b64(img) -> str:
    ok, png = cv2.imencode(".png", img)
    return base64.b64encode(png.tobytes()).decode("ascii")


def cameras(P, n, size, pitch_deg, margin=0.05):
    lo, hi = P.min(0), P.max(0)
    center = (lo + hi) / 2
    radius = float(np.linalg.norm(hi - lo)) / 2
    px = 2 * radius / (size * (1 - 2 * margin))
    p = math.radians(pitch_deg)
    names = {0: "front", 45: "front_left", 90: "left", 135: "back_left", 180: "back", 225: "back_right",
             270: "right", 315: "front_right", 60: "left60", 120: "back_left120", 240: "back_right240", 300: "right300"}
    out = []
    for k in range(n):
        yaw = 360.0 * k / n
        a = math.radians(yaw)
        back = np.array([math.sin(a) * math.cos(p), math.sin(p), math.cos(a) * math.cos(p)])
        fwd = -back
        right = np.cross(fwd, np.array([0.0, 1.0, 0.0]))
        right /= np.linalg.norm(right)
        up = np.cross(right, fwd)
        origin = center - fwd * radius - right * (size / 2) * px + up * (size / 2) * px
        out.append({"name": names.get(int(round(yaw)), f"yaw{int(round(yaw))}"), "forward": fwd, "up": up,
                    "right": right, "origin": origin, "px": px, "size": size})
    return out


def raster(torch, Pt, Ft, cam, dev):
    """-> face id per pixel (size x size, -1 = background) on the GPU."""
    size = cam["size"]
    o = torch.tensor(cam["origin"], dtype=torch.float32, device=dev)
    r = torch.tensor(cam["right"], dtype=torch.float32, device=dev)
    u_ = torch.tensor(cam["up"], dtype=torch.float32, device=dev)
    f_ = torch.tensor(cam["forward"], dtype=torch.float32, device=dev)
    d = Pt - o
    u = (d @ r) / cam["px"]
    v = -(d @ u_) / cam["px"]
    z = d @ f_
    zmin, zmax = float(z.min()), float(z.max())
    zq = ((z - zmin) / max(zmax - zmin, 1e-9) * (2 ** 36 - 1))
    a, b, c = Ft[:, 0], Ft[:, 1], Ft[:, 2]
    ua, ub, uc, va, vb, vc = u[a], u[b], u[c], v[a], v[b], v[c]
    edge = torch.maximum(torch.maximum(torch.hypot(ub - ua, vb - va), torch.hypot(uc - ub, vc - vb)), torch.hypot(ua - uc, va - vc))
    steps = torch.clamp(torch.ceil(edge * 1.6).long() + 1, 1, 64)
    big = torch.iinfo(torch.int64).max
    zbuf = torch.full((size * size,), big, dtype=torch.int64, device=dev)
    fid_all = torch.arange(len(Ft), device=dev, dtype=torch.int64)
    for k in torch.unique(steps).tolist():
        sel = torch.nonzero(steps == k).squeeze(1)
        i, j = torch.meshgrid(torch.arange(k + 1, device=dev), torch.arange(k + 1, device=dev), indexing="ij")
        m = (i + j) <= k
        w1 = (i[m].float() / k)[None, :]
        w2 = (j[m].float() / k)[None, :]
        w0 = 1.0 - w1 - w2
        per = int(w0.shape[1])
        chunk = max(1, 6_000_000 // per)
        for s0 in range(0, len(sel), chunk):
            fs = sel[s0:s0 + chunk]
            fa, fb, fc = a[fs][:, None], b[fs][:, None], c[fs][:, None]
            px = u[fa] * w0 + u[fb] * w1 + u[fc] * w2
            py = v[fa] * w0 + v[fb] * w1 + v[fc] * w2
            dz = zq[fa] * w0 + zq[fb] * w1 + zq[fc] * w2
            ix, iy = torch.floor(px).long(), torch.floor(py).long()
            ok = (ix >= 0) & (iy >= 0) & (ix < size) & (iy < size)
            key = (dz.long() << 24) | fid_all[fs][:, None].expand_as(dz)
            zbuf.scatter_reduce_(0, (iy * size + ix)[ok], key[ok], reduce="amin")
    tri = torch.where(zbuf == big, torch.full_like(zbuf, -1), zbuf & ((1 << 24) - 1))
    return tri.view(size, size)


def shade(torch, tri, fn, fcol, cam, dev, bg=0.92):
    """Flat lit picture (BGR uint8) from the face normals and colours, the light of mt.render.lit_png."""
    on = tri >= 0
    t = tri.clamp(min=0)
    n = fn[t]
    f_ = torch.tensor(cam["forward"], dtype=torch.float32, device=dev)
    up = torch.tensor(cam["up"], dtype=torch.float32, device=dev)
    rt = torch.tensor(cam["right"], dtype=torch.float32, device=dev)
    n = torch.where(((n @ f_) > 0)[..., None], -n, n)
    key = -f_ * 0.75 + up * 0.55 - rt * 0.35
    key = key / key.norm()
    lam = (n @ key).clamp(0, 1) * 0.75 + (n @ up).clamp(0, 1) * 0.2 + 0.28
    rim = (1 - (n @ f_).abs()).clamp(0, 1) ** 3 * 0.12
    alb = fcol[t]
    lin = alb ** 2.2 * lam[..., None] + rim[..., None]
    rgb = lin.clamp(0, 1) ** (1 / 2.2)
    rgb = torch.where(on[..., None], rgb, torch.full_like(rgb, bg))
    return (rgb * 255 + 0.5).byte().cpu().numpy()[..., ::-1].copy()


def _phrases(vocab):
    """{label: "w1,w2"} -> the caption and phrase -> label."""
    pairs = []
    for lab, words in (vocab or {}).items():
        for w in str(words).split(","):
            w = w.strip().lower()
            if w:
                pairs.append((w, lab))
    caption = " . ".join(w for w, _ in pairs) + " ."
    return caption, pairs


def _label_of(text, pairs):
    text = (text or "").lower().strip()
    best = None
    for w, lab in pairs:
        if w == text:
            return lab
        if w in text and (best is None or len(w) > len(best[0])):
            best = (w, lab)
    return best[1] if best else None


def detect(STATE, imgs_bgr, vocab, box_threshold, text_threshold, max_share, batch):
    """GroundingDINO on a batch of pictures with one caption -> per picture [(label, phrase, score, box)]."""
    import torch
    from PIL import Image
    proc, gd, device = STATE["proc"], STATE["gd"], STATE["device"]
    caption, pairs = _phrases(vocab)
    out = []
    i = 0
    while i < len(imgs_bgr):
        chunk = imgs_bgr[i:i + batch]
        pils = [Image.fromarray(cv2.cvtColor(x, cv2.COLOR_BGR2RGB)) for x in chunk]
        try:
            inp = proc(images=pils, text=[caption] * len(pils), return_tensors="pt", padding=True).to(device)
            with torch.inference_mode():
                o = gd(**inp)
        except RuntimeError as exc:                          # CUDA OOM on a shared card: one by one
            if batch > 1 and "out of memory" in str(exc).lower():
                torch.cuda.empty_cache()
                batch = 1
                continue
            raise
        sizes = [(x.shape[0], x.shape[1]) for x in chunk]
        try:
            res = proc.post_process_grounded_object_detection(o, inp.input_ids, threshold=box_threshold,
                                                              text_threshold=text_threshold, target_sizes=sizes)
        except TypeError:
            res = proc.post_process_grounded_object_detection(o, inp.input_ids, box_threshold=box_threshold,
                                                              text_threshold=text_threshold, target_sizes=sizes)
        for r, (H, W) in zip(res, sizes):
            texts = r.get("text_labels") or r.get("labels") or []
            dets = []
            for b, s, t in zip(r["boxes"].tolist(), r["scores"].tolist(), texts):
                lab = _label_of(t if isinstance(t, str) else str(t), pairs)
                if lab is None:
                    continue
                box = [max(0.0, b[0]), max(0.0, b[1]), min(float(W), b[2]), min(float(H), b[3])]
                w_, h_ = box[2] - box[0], box[3] - box[1]
                if w_ < 3 or h_ < 3 or w_ * h_ / (W * H) > max_share:
                    continue
                dets.append((lab, str(t), float(s), box))
            out.append(dets)
        i += len(chunk)
    return out


def segment_boxes(STATE, img_bgr, dets):
    """SAM2 masks of every box of one picture in one call -> [(label, phrase, score, sam, box, mask bool)]."""
    import torch
    sam = STATE["sam"]
    if not dets:
        return []
    img = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
    boxes = np.array([d[3] for d in dets], np.float32)
    with torch.inference_mode():
        sam.set_image(img)
        ms, scores, _ = sam.predict(box=boxes, multimask_output=False)
    ms = np.asarray(ms)
    scores = np.asarray(scores)
    if ms.ndim == 3:                                         # one box: (1, H, W)
        ms = ms[None]
        scores = scores[None]
    out = []
    for k, d in enumerate(dets):
        m = ms[k, 0] > 0.5 if ms.ndim == 4 else ms[k] > 0.5
        if not m.any():
            continue
        x0, y0, x1, y1 = d[3]
        ys, xs = np.nonzero(m)
        inside = float(((xs >= x0) & (xs <= x1) & (ys >= y0) & (ys <= y1)).mean())
        if inside < 0.55:
            continue
        sc = float(np.ravel(scores[k])[0])
        out.append((d[0], d[1], d[2], sc, d[3], m))
    return out


# a mask over this share of the figure's silhouette is a whole-figure detection, not the part (a «staff» box around
# the whole Santa); the big garments may cover more
SIL_CAP = {"torso": 0.75, "cloak": 0.8, "cape": 0.8, "coat": 0.85, "skirt": 0.6, "prop": 0.3, "body": 0.95,
           "_": 0.5}
# rare labels need a surer detection (the hat's tip is not a tail, a pompom is not a horn)
MIN_DINO = {"tail": 0.35, "horn": 0.4, "wing": 0.3, "ear": 0.3, "prop": 0.3}


def _paint(objs, shape, lid, min_score, sil=None):
    """Pixel label map (-1 none) and score map: big masks first, the smaller ones overwrite."""
    lab = np.full(shape, -1, np.int32)
    sc = np.zeros(shape, np.float32)
    for o in sorted(objs, key=lambda x: -int(x[5].sum())):
        s = o[2] * o[3]
        if s < min_score or o[0] not in lid or o[2] < MIN_DINO.get(o[0], 0.0):
            continue
        if sil and int(o[5].sum()) > SIL_CAP.get(o[0], SIL_CAP["_"]) * sil:
            continue
        lab[o[5]] = lid[o[0]]
        sc[o[5]] = s
    return lab, sc


def analyze(body, STATE):
    import torch
    t0 = time.perf_counter()
    sec = {}
    groups = body.get("vocab_groups") or [body.get("vocab") or {}]   # one DINO caption per group
    vocab = {}
    for g in groups:
        vocab.update(g)
    mats = body.get("materials") or None
    labels = list(vocab)
    lid = {l: i for i, l in enumerate(labels)}
    mlabels = list(mats or {})
    mid = {l: i for i, l in enumerate(mlabels)}
    bt, tt = float(body.get("box_threshold", 0.25)), float(body.get("text_threshold", 0.2))
    ms_, mx = float(body.get("min_score", 0.15)), float(body.get("max_share", 0.9))
    batch = int(body.get("batch", 4))
    dev = STATE["device"]
    if body.get("kind") == "picture":
        img = cv2.imdecode(np.frombuffer(base64.b64decode(body["image_b64"]), np.uint8), cv2.IMREAD_COLOR)
        t = time.perf_counter()
        dets = [d for g in groups for d in detect(STATE, [img], g, bt, tt, mx, 1)[0]]
        mdets = detect(STATE, [img], mats, bt, tt, mx, 1)[0] if mats else []
        sec["dino"] = round(time.perf_counter() - t, 3)
        t = time.perf_counter()
        objs = segment_boxes(STATE, img, dets)
        mobjs = segment_boxes(STATE, img, mdets) if mdets else []
        sec["sam"] = round(time.perf_counter() - t, 3)
        lab, _ = _paint(objs, img.shape[:2], lid, ms_)
        mlab, _ = _paint(mobjs, img.shape[:2], mid, ms_) if mobjs else (np.full(img.shape[:2], -1, np.int32), None)
        masks, rows, cnt = {}, [], {}
        for o in objs:
            i = cnt.get(o[0], 0)
            cnt[o[0]] = i + 1
            name = f"{o[0]}_{i}.png"
            masks[name] = _png_b64(o[5].astype(np.uint8) * 255)
            rows.append({"label": o[0], "phrase": o[1], "score": round(o[2], 3), "sam": round(o[3], 3),
                         "box": [round(v) for v in o[4]], "pixels": int(o[5].sum()), "mask": name})
        sec["total"] = round(time.perf_counter() - t0, 3)
        return {"version": VERSION, "kind": "picture", "labels": labels, "mat_labels": mlabels, "objects": rows,
                "masks": masks, "object_map_b64": _png_b64((lab + 1).astype(np.uint8)),
                "material_map_b64": _png_b64((mlab + 1).astype(np.uint8)), "seconds": sec}
    # ---- mesh
    t = time.perf_counter()
    z = np.load(io.BytesIO(base64.b64decode(body["mesh_npz_b64"])), allow_pickle=False)
    P, F = z["P"].astype(np.float32), z["F"].astype(np.int64)
    C = z["C"].astype(np.float32) / 255.0 if "C" in z.files else np.full((len(P), 3), 0.7, np.float32)
    Pt = torch.tensor(P, device=dev)
    Ft = torch.tensor(F, device=dev)
    fa, fb, fc = Pt[Ft[:, 0]], Pt[Ft[:, 1]], Pt[Ft[:, 2]]
    fn = torch.linalg.cross(fb - fa, fc - fa)
    fn = fn / fn.norm(dim=1, keepdim=True).clamp(min=1e-12)
    Ct = torch.tensor(C, device=dev)
    fcol = (Ct[Ft[:, 0]] + Ct[Ft[:, 1]] + Ct[Ft[:, 2]]) / 3
    cams = cameras(P.astype(np.float64), int(body.get("views", 6)), int(body.get("size", 448)),
                   float(body.get("pitch_deg", 12.0)))
    tris, imgs = [], []
    for cam in cams:
        tri = raster(torch, Pt, Ft, cam, dev)
        imgs.append(shade(torch, tri, fn, fcol, cam, dev))
        tris.append(tri)
    if dev == "cuda":
        torch.cuda.synchronize()
    sec["render"] = round(time.perf_counter() - t, 3)
    t = time.perf_counter()
    dets = [[] for _ in imgs]
    for g in groups:
        for k, dd in enumerate(detect(STATE, imgs, g, bt, tt, mx, batch)):
            dets[k].extend(dd)
    mdets = detect(STATE, imgs, mats, bt, tt, mx, batch) if mats else [[] for _ in imgs]
    sec["dino"] = round(time.perf_counter() - t, 3)
    M, L, ML = len(F), len(labels), len(mlabels)
    votes = torch.zeros(M * max(L, 1), device=dev)
    mvotes = torch.zeros(M * max(ML, 1), device=dev)
    seen = torch.zeros(M, device=dev)
    rows, maps, tri_png = [], {}, {}
    t_sam = 0.0
    for cam, tri, img, d, md in zip(cams, tris, imgs, dets, mdets):
        ts = time.perf_counter()
        objs = segment_boxes(STATE, img, d)
        mobjs = segment_boxes(STATE, img, md) if md else []
        t_sam += time.perf_counter() - ts
        on = tri >= 0
        f = tri[on]
        f_ = torch.tensor(cam["forward"], dtype=torch.float32, device=dev)
        w = (fn[f] @ f_).abs() + 0.05
        seen.index_add_(0, f, w)
        for objs_, nlab, lidx, acc in ((objs, L, lid, votes), (mobjs, ML, mid, mvotes)):
            if not objs_ or not nlab:
                continue
            lab, sc = _paint(objs_, tri.shape, lidx, ms_, sil=int(on.sum().item()) if acc is votes else None)
            labt = torch.tensor(lab, device=dev)[on]
            sct = torch.tensor(sc, device=dev)[on]
            hit = labt >= 0
            acc.index_add_(0, f[hit] * nlab + labt[hit].long(), (w * sct)[hit])
            if body.get("return_maps") and acc is votes:
                maps[cam["name"]] = _png_b64((lab + 1).astype(np.uint8))
        if body.get("return_maps") and cam["name"] in ("front", "left", "back", "right"):
            t1 = (tri + 1).cpu().numpy().astype(np.int64)
            tri_png[cam["name"]] = _png_b64(np.stack([t1 & 255, (t1 >> 8) & 255, (t1 >> 16) & 255], -1).astype(np.uint8))
        rows.append({"name": cam["name"], "objects": [{"label": o[0], "phrase": o[1], "score": round(o[2], 3),
                                                       "sam": round(o[3], 3), "box": [round(v) for v in o[4]],
                                                       "pixels": int(o[5].sum())} for o in objs],
                     "materials": [{"label": o[0], "score": round(o[2], 3), "pixels": int(o[5].sum())} for o in mobjs]})
    sec["sam"] = round(t_sam, 3)
    out = {"version": VERSION, "kind": "mesh", "faces": M, "labels": labels, "mat_labels": mlabels,
           "votes_b64": _b64(votes.view(M, max(L, 1)).half().cpu().numpy()),
           "mat_votes_b64": _b64(mvotes.view(M, max(ML, 1)).half().cpu().numpy()),
           "seen_b64": _b64(seen.half().cpu().numpy()), "views": rows,
           "cameras": [{k: (v.tolist() if hasattr(v, "tolist") else v) for k, v in c.items()} for c in cams]}
    if body.get("return_maps"):
        out["maps"] = maps
        out["tri"] = tri_png
        out["renders"] = {c["name"]: base64.b64encode(cv2.imencode(".jpg", im, [cv2.IMWRITE_JPEG_QUALITY, 85])[1].tobytes()).decode()
                          for c, im in zip(cams, imgs)}
    sec["total"] = round(time.perf_counter() - t0, 3)
    out["seconds"] = sec
    return out
