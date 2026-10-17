#!/usr/bin/env python3
"""Hand-labelled part ground truth for the V3.1 part-entity corpus (docs/v31-part-entities-spec-20261017.md B.5).

A model's ground truth is written by a person (here: the agent, by hand) as ordered region rules in the model's
canonical frame (mt.render: faces +Z, +Y up, its left +X), every coordinate normalised to the model's bounding box
(0..1 per axis: x 0 = its right side, 1 = its left; y 0 = the floor, 1 = the top; z 0 = its back, 1 = its front),
checked on the four-view sheet this tool draws (labels + a 0.1 grid) and corrected until the sheet is right. The rules
never read a voter's output: they are the reference the voters are measured against.

    parts_gt.py sheet <spec.json> [--out sheet.png]           apply the rules, draw front / left / back / right
    parts_gt.py write <spec.json> --out <id>.parts_gt.json    the ground truth file (per-face labels, zlib + base64)
    parts_gt.py eval <corpus.json> [--analyse] [--out report.json]   IoU per label of every voter and of the merge

Spec: {"id", "glb" (path), "forward" (+z|-z|+x|-x), "category", "base": "torso", "rules": [rule, ...]}
Rule: {"label": "arm", "side": true, <conditions>} - every condition must hold for a face (its centroid):
    "x" | "y" | "z": [lo, hi]            normalised coordinate range
    "within": ["arm.L", ...]             only faces that already carry one of these labels
    "not": ["hand.L", ...]               never faces that carry these
    "plane": {"p": [x, y, z], "n": [nx, ny, nz]}    the side of a plane the normal points to (normalised frame)
    "planes": [plane, ...]               all of them (a V, a wedge)
    "ball": [[x, y, z], r]               within r of a point (r in units of the bbox height)
    "capsule": [[x, y, z], [x, y, z], r] within r of a segment
    "nx" | "ny" | "nz": [lo, hi]          the face normal's component (outer surfaces: hair on top, a cloak's back)
"side": true appends .L / .R by the face's side of the mid-plane (x 0.5 by default, "mid": 0.47 to move it).
"""
from __future__ import annotations

import argparse
import base64
import json
import pathlib
import sys
import time
import zlib

import numpy as np

MT_ROOT = "/srv/autorig/data/motion_transfer"


def _mt():
    root = __import__("os").environ.get("PARTS_GT_MT_ROOT") or MT_ROOT
    if root not in sys.path:
        sys.path.insert(0, root)
    from mt import parts_seg as PS, render as R, analyzer as A, parts_entities as PE
    from mt.glb import load_glb
    return PS, R, A, PE, load_glb


def load_model(spec):
    PS, R, A, PE, load_glb = _mt()
    data = pathlib.Path(spec["glb"]).read_bytes()
    mesh0 = load_glb(data)
    mesh, _ = PS.canonical(mesh0, spec.get("forward", "+z"))
    return mesh0, mesh, data


def apply_rules(spec, mesh):
    PS, R, A, PE, load_glb = _mt()
    P = mesh.positions.astype(np.float64)
    F = mesh.faces
    lo, hi = P.min(0), P.max(0)
    ext = np.maximum(hi - lo, 1e-9)
    H = float(ext[1])
    C = (P[F].mean(1) - lo) / ext
    n = np.cross(P[F[:, 1]] - P[F[:, 0]], P[F[:, 2]] - P[F[:, 0]])
    n /= np.maximum(np.linalg.norm(n, axis=1, keepdims=True), 1e-12)
    lab = np.array([spec.get("base", "torso")] * len(F), dtype=object)
    Cw = P[F].mean(1)
    for r in spec["rules"]:
        m = np.ones(len(F), bool)
        for k, ax in (("x", 0), ("y", 1), ("z", 2)):
            if k in r:
                m &= (C[:, ax] >= r[k][0]) & (C[:, ax] <= r[k][1])
        for k, ax in (("nx", 0), ("ny", 1), ("nz", 2)):
            if k in r:
                m &= (n[:, ax] >= r[k][0]) & (n[:, ax] <= r[k][1])
        if "within" in r:
            m &= np.isin(lab, r["within"])
        if "not" in r:
            m &= ~np.isin(lab, r["not"])
        for pl in ([r["plane"]] if "plane" in r else []) + list(r.get("planes") or []):
            p0 = np.array(pl["p"], float)
            nn = np.array(pl["n"], float)
            m &= ((C - p0) @ nn) >= 0
        if "ball" in r:
            c0 = lo + np.array(r["ball"][0], float) * ext
            m &= np.linalg.norm(Cw - c0, axis=1) <= r["ball"][1] * H
        if "capsule" in r:
            a = lo + np.array(r["capsule"][0], float) * ext
            b = lo + np.array(r["capsule"][1], float) * ext
            ab = b - a
            t = np.clip(((Cw - a) @ ab) / max(ab @ ab, 1e-12), 0, 1)
            m &= np.linalg.norm(Cw - (a + t[:, None] * ab), axis=1) <= r["capsule"][2] * H
        name = r["label"]
        if r.get("side"):
            mid = r.get("mid", spec.get("mid", 0.5))
            lab[m & (C[:, 0] >= mid)] = name + ".L"
            lab[m & (C[:, 0] < mid)] = name + ".R"
        else:
            lab[m] = name
    return lab


def _colour(PS, l):
    return PS.COLOURS.get(l, PS.COLOURS.get(str(l).split(".")[0], (128, 128, 128)))


def sheet(spec, out, size=420, views=("front", "left", "back", "right")):
    import cv2
    PS, R, A, PE, load_glb = _mt()
    mesh0, mesh, _ = load_model(spec)
    lab = apply_rules(spec, mesh)
    P = mesh.positions.astype(np.float64)
    F = mesh.faces
    lo, hi = P.min(0), P.max(0)
    # draw on a proxy when the mesh is big (the labels follow the vertex clusters)
    if len(F) > 120000:
        Pp, Fp, _, vmap, _ = A.proxy(P, F, None, 60000)
        labs = sorted({str(x) for x in np.unique(lab)})
        li = {l: i for i, l in enumerate(labs)}
        fl = np.array([li[str(x)] for x in lab])
        vv = np.zeros((len(P), len(labs)), np.float32)
        for k in range(3):
            np.add.at(vv, (F[:, k], fl), 1)
        pv = np.zeros((len(Pp), len(labs)), np.float32)
        np.add.at(pv, vmap, vv)
        pvl = pv.argmax(1)
        flp = pvl[Fp[:, 0]]
        lab_draw = np.array(labs, dtype=object)[flp]
        dm = type(mesh)(Pp, np.zeros_like(Pp), np.zeros((len(Pp), 2), np.float32), Fp.astype(np.int64),
                        np.zeros(len(Fp), np.int32), [])
    else:
        lab_draw, dm = lab, mesh
    cams = R.cameras_for(dm.positions.astype(np.float64), size=size, names=tuple(views), frame_points=P)
    tiles = []
    for name in views:
        cam = cams[name]
        v = R.render_view(dm, cam) if dm is mesh else _flat_view(R, dm, cam)
        img = np.full((size, size, 3), 235, np.float32)
        on = v.tri >= 0
        shade = np.clip(np.abs(v.normal[..., 2] if False else (v.normal @ (-cam.forward))), 0.25, 1.0)
        labs_px = lab_draw[v.tri[on]]
        col = np.zeros((int(on.sum()), 3), np.float32)
        for l in np.unique(labs_px):
            col[labs_px == l] = np.array(_colour(PS, str(l))[::-1], np.float32)
        img[on] = col * (0.45 + 0.55 * shade[on][:, None])
        # the 0.1 grid of the normalised frame
        hax = 0 if name in ("front", "back") else 2
        for k in range(11):
            t = k / 10
            pt = lo.copy() + (hi - lo) * 0.5
            pt[hax] = lo[hax] + t * (hi[hax] - lo[hax])
            u, _, _ = cam.project(pt[None])
            pt2 = lo.copy() + (hi - lo) * 0.5
            pt2[1] = lo[1] + t * (hi[1] - lo[1])
            _, vv_, _ = cam.project(pt2[None])
            u, vv_ = int(round(float(u[0]))), int(round(float(vv_[0])))
            c = (90, 90, 90) if k % 5 else (0, 0, 200)
            if 0 <= u < size:
                cv2.line(img, (u, 0), (u, size - 1), c, 1)
                cv2.putText(img, f"{t:.1f}", (u + 1, size - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.3, c, 1)
            if 0 <= vv_ < size:
                cv2.line(img, (0, vv_), (size - 1, vv_), c, 1)
                cv2.putText(img, f"{t:.1f}", (1, vv_ - 2), cv2.FONT_HERSHEY_SIMPLEX, 0.3, c, 1)
        axis = {"front": "x: 1 (its left) is on the right", "back": "x: 1 (its left) on the left",
                "left": "z: front", "right": "z: front"}[name]
        cv2.putText(img, f"{name}  {axis}", (4, 12), cv2.FONT_HERSHEY_SIMPLEX, 0.38, (0, 0, 0), 1)
        tiles.append(img.astype(np.uint8))
    present = sorted({str(x) for x in np.unique(lab)})
    tot = len(lab)
    W = size * len(tiles)
    legend = np.full((22 * ((len(present) + 5) // 6) + 8, W, 3), 30, np.uint8)
    for i, l in enumerate(present):
        x, y = 8 + (i % 6) * (W // 6), 16 + (i // 6) * 22
        cv2.rectangle(legend, (x, y - 11), (x + 14, y + 3), _colour(PS, l)[::-1], -1)
        cv2.putText(legend, f"{l} {100 * (lab == l).sum() / tot:.1f}%", (x + 20, y), cv2.FONT_HERSHEY_SIMPLEX, 0.42,
                    (230, 230, 230), 1)
    cv2.imwrite(str(out), np.vstack([np.hstack(tiles), legend]))
    return {"labels": {l: int((lab == l).sum()) for l in present}, "faces": tot}


def _flat_view(R, dm, cam):
    tri, bary, zbuf = R.rasterize(np.stack(cam.project(dm.positions.astype(np.float64))[:2], 1),
                                  cam.project(dm.positions.astype(np.float64))[2], dm.faces, cam.size)
    P = dm.positions.astype(np.float64)
    F = dm.faces
    fn = np.cross(P[F[:, 1]] - P[F[:, 0]], P[F[:, 2]] - P[F[:, 0]])
    fn /= np.maximum(np.linalg.norm(fn, axis=1, keepdims=True), 1e-12)
    normal = np.zeros((cam.size, cam.size, 3), np.float32)
    on = tri >= 0
    normal[on] = fn[tri[on]]
    normal[on] *= np.where((normal[on] @ cam.forward) > 0, -1, 1)[:, None]
    return R.View(cam, tri, zbuf, normal, np.zeros_like(normal), on)


def write(spec, out):
    import hashlib
    mesh0, mesh, data = load_model(spec)
    lab = apply_rules(spec, mesh)
    labels = sorted({str(x) for x in np.unique(lab)})
    li = {l: i for i, l in enumerate(labels)}
    arr = np.array([li[str(x)] for x in lab], np.uint8)
    doc = {"schema": "autorig.autotests.parts-gt/1", "id": spec["id"], "glb": pathlib.Path(spec["glb"]).name,
           "glb_sha256": hashlib.sha256(data).hexdigest(), "faces": int(len(lab)), "forward": spec.get("forward", "+z"),
           "category": spec.get("category"), "labels": labels,
           "face_label_zlib_b64": base64.b64encode(zlib.compress(arr.tobytes(), 9)).decode("ascii"),
           "area_share": {l: round(float((lab == l).mean()), 4) for l in labels},
           "author": "hand-labelled by the agent (Opus, 2026-10-17) with region rules in the normalised canonical "
                     "frame, checked and corrected on the four-view sheet; never derived from a voter's output",
           "spec": spec, "at": round(time.time(), 1)}
    pathlib.Path(out).write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    return doc


def read_gt(path):
    doc = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    arr = np.frombuffer(zlib.decompress(base64.b64decode(doc["face_label_zlib_b64"])), np.uint8)
    return doc, np.array(doc["labels"], dtype=object)[arr]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s1 = sub.add_parser("sheet")
    s1.add_argument("spec")
    s1.add_argument("--out")
    s1.add_argument("--size", type=int, default=420)
    s1.add_argument("--views", default="front,left,back,right")
    s2 = sub.add_parser("write")
    s2.add_argument("spec")
    s2.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    spec = json.loads(pathlib.Path(a.spec).read_text(encoding="utf-8"))
    if a.cmd == "sheet":
        print(json.dumps(sheet(spec, a.out or str(pathlib.Path(a.spec).with_suffix(".png")), a.size,
                               tuple(a.views.split(",")))))
    elif a.cmd == "write":
        d = write(spec, a.out)
        print(json.dumps({"id": d["id"], "faces": d["faces"], "area_share": d["area_share"]}))


if __name__ == "__main__":
    main()
