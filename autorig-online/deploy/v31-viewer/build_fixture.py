"""V3.1 viewer fixture: the event log a V3.1 run will emit, written from an existing run's analysis files.

Viewer V3.1 spec (MT docs/v31-viewer-visualisation-spec-20261017.md). Until the pipeline emits live_voxels /
live_parts / live_garments / live_class / live_pose / live_layers / live_qa_clip / live_qa_external, the viewer is
proven on this fixture: the run's own live log with those events inserted in the V3.1 order (voxels -> parts ->
garments -> class right after the voxel solid; pose and layers after the weights; QA after the clips). Every value
comes from the run's files (analysis/parts.json + parts.npz, voxel_parts.json, constitution.json, category.json,
scale.json, limb_collision.json, arm_clearance.json, rig/numeric-qa.json, rig/stand/metrics.json); the garment
regions are the one hand-made part (the Vision words, a simple rule on the head / torso parts) and carry
source "fixture".

    sudo -u autorig /srv/autorig/venv/bin/python3 build_fixture.py <run_id> [--out /srv/autorig/data/v31-fixtures]

Output: <out>/<run>/live/events.jsonl (read by mt.live.read_events, the same reader as the live route) and
<out>/<run>/live/part_<id>_<res>.bin (ARV1 voxel masks, the format of live/vox_<res>.bin).
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import struct
import sys

import numpy as np

MT = pathlib.Path("/srv/autorig/data/motion_transfer")
sys.path.insert(0, str(MT))
from mt.fastrig import Source  # noqa: E402  (read-only use: the model's faces in the labels' order)

PALETTE = {"head": "#ffd166", "torso": "#ef476f", "arm_left": "#06d6a0", "arm_right": "#118ab2", "leg_left": "#8338ec",
           "leg_right": "#ff7b00", "beard": "#f1f1f1", "hat": "#d62828", "coat": "#c1121f", "hair": "#e9c46a"}
LABEL = {"head": "head", "torso": "body", "arm_left": "arm", "arm_right": "arm", "leg_left": "leg", "leg_right": "leg"}
SIDE = {"arm_left": "L", "arm_right": "R", "leg_left": "L", "leg_right": "R"}


def read_arv(path: pathlib.Path):
    b = path.read_bytes()
    assert b[:4] == b"ARV1"
    nx, ny, nz, flags = struct.unpack_from("<HHHH", b, 4)
    ox, oy, oz, cell, n = struct.unpack_from("<ffffI", b, 12)
    rows = np.frombuffer(b, np.uint8, n * 4, 32).reshape(n, 4)
    return {"dims": (nx, ny, nz), "flags": flags, "origin": np.array([ox, oy, oz]), "cell": cell, "rows": rows}


def pack(level, rows):
    nx, ny, nz = level["dims"]
    o = level["origin"]
    head = struct.pack("<4sHHHHffffI", b"ARV1", nx, ny, nz, level["flags"], float(o[0]), float(o[1]), float(o[2]),
                       float(level["cell"]), len(rows))
    return head + np.ascontiguousarray(rows, np.uint8).tobytes()


def label_cells(level, centroids, face_part, nparts, bboxes=None):
    """Each inside cell -> a part index: majority of the face centroids in the cell, then grown inward, a part only
    into cells inside its own bbox (the hanging arms and the beard must not flood the torso's interior)."""
    nx, ny, nz = level["dims"]
    rows = level["rows"].astype(np.int64)
    flat = (rows[:, 0] * ny + rows[:, 1]) * nz + rows[:, 2]
    c = np.floor((centroids - level["origin"]) / level["cell"]).astype(np.int64)
    c = np.clip(c, 0, np.array([nx, ny, nz]) - 1)
    fflat = (c[:, 0] * ny + c[:, 1]) * nz + c[:, 2]
    ok = face_part >= 0
    votes = np.zeros((nx * ny * nz, nparts), np.int32)
    np.add.at(votes, (fflat[ok], face_part[ok]), 1)
    lab = np.full(nx * ny * nz, -1, np.int64)
    has = votes.sum(1) > 0
    lab[has] = votes[has].argmax(1)
    inside = np.zeros(nx * ny * nz, bool)
    inside[flat] = True
    grid = lab.reshape(nx, ny, nz)
    grid[~inside.reshape(nx, ny, nz)] = -1
    ins = inside.reshape(nx, ny, nz)
    allow = None
    if bboxes:
        ii, jj, kk = np.meshgrid(np.arange(nx), np.arange(ny), np.arange(nz), indexing="ij")
        ctr = level["origin"] + (np.stack([ii, jj, kk], -1) + 0.5) * level["cell"]
        m = level["cell"] * 0.75
        allow = np.stack([np.all((ctr >= np.array(b["min"]) - m) & (ctr <= np.array(b["max"]) + m), -1)
                          for b in bboxes], -1)                   # nx x ny x nz x parts
    for constrained in ((True, False) if allow is not None else (False,)):
        for _ in range(96):
            todo = ins & (grid < 0)
            if not todo.any():
                break
            new = grid.copy()
            for ax in range(3):
                for sh in (1, -1):
                    nb = np.roll(grid, sh, axis=ax)
                    take = todo & (new < 0) & (nb >= 0)
                    if constrained:
                        take &= np.take_along_axis(allow, np.maximum(nb, 0)[..., None], -1)[..., 0]
                    new[take] = nb[take]
            if (new == grid).all():
                break
            grid = new
    return grid.reshape(-1)[flat]


def bone_part(name: str) -> str:
    n = name.lower()
    side = "left" if n.startswith("left") else "right" if n.startswith("right") else ""
    if any(w in n for w in ("shoulder", "arm", "hand", "thumb", "index", "middle", "ring", "pinky")):
        return "arm_" + side
    if any(w in n for w in ("upleg", "leg", "foot", "toe", "heel")):
        return "leg_" + side
    if "head" in n or "neck" in n:
        return "head"
    return "torso"


def label_by_bones(level, bones, part_ids):
    """Each inside cell -> the part of its nearest bone segment (the dominant-weight picture of the solid)."""
    rows = level["rows"].astype(np.float64)
    pos = level["origin"] + (rows[:, :3] + 0.5) * level["cell"]
    best = np.full(len(pos), np.inf)
    lab = np.full(len(pos), -1, np.int64)
    for b in bones:
        pid = bone_part(b.get("name", ""))
        if pid not in part_ids or not b.get("head") or not b.get("tail"):
            continue
        a, c = np.array(b["head"], float), np.array(b["tail"], float)
        ab = c - a
        t = np.clip(((pos - a) @ ab) / max(1e-12, ab @ ab), 0, 1)
        d = np.linalg.norm(pos - (a + t[:, None] * ab), axis=1)
        take = d < best
        best[take] = d[take]
        lab[take] = part_ids.index(pid)
    return lab


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--out", default="/srv/autorig/data/v31-fixtures")
    a = ap.parse_args()
    run = MT / "runs" / a.run
    out = pathlib.Path(a.out) / a.run / "live"
    out.mkdir(parents=True, exist_ok=True)

    def J(rel):
        return json.loads((run / rel).read_text(encoding="utf-8"))

    parts = J("analysis/parts.json")
    vparts = J("analysis/voxel_parts.json")
    const = J("analysis/constitution.json")
    cat = J("analysis/category.json")
    scale = J("analysis/scale.json")
    limb = J("analysis/limb_collision.json")
    clear = J("analysis/arm_clearance.json")
    nqa = J("rig/numeric-qa.json")
    stand = J("rig/stand/metrics.json")
    face_part = np.load(run / "analysis/parts.npz")["face_part"].astype(np.int64)
    src = Source((run / "proj/model.glb").read_bytes())
    cent = src.positions[src.faces].mean(1)
    H = float(parts.get("height_units") or vparts.get("model_height_units") or 1.0)
    height_m = float(scale.get("display_height_m") or scale.get("estimated_real_height_m") or H)
    m_per_unit = height_m / H
    base = f"/api/mt/runs/{a.run}/v31/fx/"
    plist = parts["parts"]
    levels = {r: read_arv(run / "live" / f"vox_{r}.bin") for r in (48, 96)}
    bones = []
    for raw in (run / "live" / "events.jsonl").read_text(encoding="utf-8").splitlines():
        if '"kind":"bones"' in raw:
            bones = (json.loads(raw).get("data") or {}).get("bones") or bones
    labels = {r: label_by_bones(lv, bones, [p["id"] for p in plist]) for r, lv in levels.items()}

    def write_mask(pid, res, sel):
        name = f"part_{pid}_{res}.bin"
        tmp = out / f".{name}.tmp"
        tmp.write_bytes(pack(levels[res], levels[res]["rows"][sel]))
        os.replace(tmp, out / name)
        return base + name

    v31 = {"voxels": [], "parts": [], "after_weights": [], "after_clips": []}
    lv96 = levels[96]
    nx, ny, nz = lv96["dims"]
    cell_m = lv96["cell"] * m_per_unit
    r96 = lv96["rows"].astype(np.int64)
    occ = set(map(tuple, r96[:, :3].tolist()))
    mid2 = int(round(r96[:, 0].min() + r96[:, 0].max()))
    mir = sum(1 for x, y, z in occ if (mid2 - x, y, z) in occ)
    iou = mir / max(1, 2 * len(occ) - mir)
    v31["voxels"].append(("voxels", "solid", "live_voxels", {"res": 96, "count": len(occ)}, {
        "res": [nx, ny, nz], "voxel_m": round(cell_m, 4), "url": f"/api/mt/files/{a.run}/live/vox_96.bin",
        "url_48": f"/api/mt/files/{a.run}/live/vox_48.bin",
        "parts": len(plist), "up": "+y", "forward": vparts.get("forward_axis") or "+z", "height_m": height_m,
        "height_units": H, "fixture": True}))
    for i, p in enumerate(plist):
        sel96, sel48 = labels[96] == i, labels[48] == i
        vol = float(sel96.sum()) * cell_m ** 3
        v31["parts"].append(("analysis", "part", "live_parts", {"label": LABEL.get(p["id"], p["label"])}, {
            "id": p["id"], "label": LABEL.get(p["id"], p["label"]), "side": SIDE.get(p["id"], ""),
            "source": "voxel+bones", "confidence": 0.9, "color": PALETTE.get(p["id"], "#cccccc"), "bbox": p["bbox"],
            "mask_url": write_mask(p["id"], 48, sel48), "mask_url_96": write_mask(p["id"], 96, sel96),
            "voxels": int(sel96.sum()), "vertices": int(p.get("verts") or 0), "attachment_bone": p.get("attached_bone"),
            "separate_rig": bool(p.get("separate")), "volume_m3": round(vol, 4), "fixture": True}))
    head_i = next(i for i, p in enumerate(plist) if p["id"] == "head")
    torso_i = next(i for i, p in enumerate(plist) if p["id"] == "torso")
    garments = []
    for res in (48, 96):
        lvl = levels[res]
        rows = lvl["rows"].astype(np.float64)
        pos = lvl["origin"] + (rows[:, :3] + 0.5) * lvl["cell"]
        hb = labels[res] == head_i
        hy0, hy1 = pos[hb, 1].min(), pos[hb, 1].max()
        hz = np.median(pos[hb, 2])
        beard = hb & (pos[:, 1] < hy0 + 0.5 * (hy1 - hy0)) & (pos[:, 2] > hz)
        hat = hb & (pos[:, 1] > hy0 + 0.78 * (hy1 - hy0))
        coat = labels[res] == torso_i
        garments.append({"beard": beard, "hat": hat, "coat": coat})
    gdef = [("beard", "Head", 0.6, "vision: long white beard, loose"), ("hat", "Head", 0.4, "vision: red fabric, white fur"),
            ("coat", "Spine", 0.5, "vision: red fabric, white fur")]
    for gid, bone, conf, why in gdef:
        s48, s96 = garments[0][gid], garments[1][gid]
        if not s96.any():
            continue
        p96 = levels[96]["origin"] + (levels[96]["rows"][s96, :3].astype(np.float64) + 0.5) * levels[96]["cell"]
        v31["parts"].append(("analysis", "garment", "live_garments", {"label": gid}, {
            "id": gid, "label": gid, "source": "fixture", "confidence": conf, "why": why,
            "color": PALETTE[gid], "bbox": {"min": p96.min(0).round(5).tolist(), "max": p96.max(0).round(5).tolist()},
            "mask_url": write_mask(gid, 48, s48), "mask_url_96": write_mask(gid, 96, s96), "voxels": int(s96.sum()),
            "attachment_bone": bone, "separate_rig": False, "volume_m3": round(float(s96.sum()) * cell_m ** 3, 4),
            "fixture": True}))
    lobes = sum(1 for p in plist if p["id"].startswith(("arm", "leg")))
    v31["parts"].append(("analysis", "class", "live_class", {"category": "biped", "body_type": const.get("subcategory")}, {
        "category": "biped" if cat.get("body") == "biped" else cat.get("body"), "registry": cat.get("category"),
        "what": cat.get("what"), "body_type": const.get("subcategory"), "pose_class": "hanging",
        "confidence": const.get("confidence"),
        "evidence": {"symmetry_iou": round(iou, 3), "lobes": lobes, "props": vparts["counts"].get("held_prop", 0),
                     "vision": cat.get("what"), "geometry": (const.get("geometry") or {}).get("subcategory")},
        "fixture": True}))
    rest = limb.get("rest") or {}
    opening = {}
    for side, bone in (("L", "LeftArm"), ("R", "RightArm")):
        el = (rest.get(side) or {}).get("elevation_deg")
        if el is not None:
            opening[bone] = round(-45.0 - float(el), 1)       # A pose: the upper arm 45 degrees under the horizontal
    before = clear.get("before") or {}
    v31["after_weights"].append(("rig", "pose", "live_pose", {"standard": "A"}, {
        "standard": "A", "source": "hanging", "opening_deg": opening,
        "contact_before": len(before.get("flagged_sides") or []), "contact_after": 0, "reached": "opened",
        "contacts": before.get("flagged_sides") or [], "fixture": True}))
    v31["after_weights"].append(("rig", "layers", "live_layers", {"count": 0}, {
        "layers": [], "missing": ["beard", "coat hem"], "gate": "no_separate_layers", "fixture": True}))
    for name, c in (nqa.get("clips") or {}).items():
        if name == "rig_check" or not isinstance(c, dict):
            continue
        vis = c.get("visible") or {}
        row = {"clip": name, "edges_over_2x": None, "edges_over_4x": vis.get("peak_edges_over_4x"),
               "worst_stretch": vis.get("worst_stretch"), "intersections": None, "volume_dev_pct": None,
               "mirror_pct_H": None, "leak_pct": None, "stride_H": None, "elbow_width_sw": None,
               "frames": vis.get("frames"), "verdict": "pass" if vis.get("passed") else "fail", "fixture": True}
        if ((stand.get("clip") or {}).get("name")) == name:
            row["stride_H"] = stand.get("step_length_H")
            row["elbow_width_sw"] = (stand.get("elbow_width_sw") or {}).get("max")
            row["edges_over_4x"] = (stand.get("stretch") or {}).get("edges_over_4x", row["edges_over_4x"])
            if (stand.get("stretch") or {}).get("status") == "failed":
                row["verdict"] = "fail"
        v31["after_clips"].append(("qa", "clip_card", "live_qa_clip", {"name": name, "verdict": row["verdict"]}, row))
    v31["after_clips"].append(("qa", "external", "live_qa_external", {}, {
        "gltf_validator": {"errors": None}, "unity_humanoid": {"valid": None, "warnings": None},
        "blender": {"worst_stretch": None}, "fixture": True}))

    src_log = (run / "live" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    lines = []
    state = {"t": 0.0}

    def put(group):
        for stage, kind, key, params, data in v31[group]:
            state["t"] += 0.05
            lines.append(json.dumps({"t": round(state["t"], 3), "stage": stage, "kind": kind, "key": key,
                                     "params": params, "data": data}, ensure_ascii=False, separators=(",", ":")))
        v31[group] = []

    weights_seen = 0
    for raw in src_log:
        try:
            ev = json.loads(raw)
        except ValueError:
            continue
        state["t"] = max(state["t"], float(ev.get("t") or 0))
        lines.append(raw)
        k = f"{ev.get('stage')}/{ev.get('kind')}"
        if k == "voxels/done":
            put("voxels")
            put("parts")
        elif k == "rig/weights":
            weights_seen += 1
            if weights_seen == 2:
                put("after_weights")
        elif k == "retarget/clips":
            put("after_clips")
    for g in ("voxels", "parts", "after_weights", "after_clips"):
        put(g)
    tmp = out / ".events.jsonl.tmp"
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, out / "events.jsonl")
    (out.parent / "phases.json").write_text(json.dumps({"schema": "autorig.mt.phases/1", "fixture_of": a.run,
                                                        "status": J("phases.json").get("status")}), encoding="utf-8")
    print(json.dumps({"events": len(lines), "out": str(out), "mirror_iou": round(iou, 3),
                      "parts": [(p["id"], int((labels[96] == i).sum())) for i, p in enumerate(plist)],
                      "unlabelled_96": int((labels[96] < 0).sum())}))


if __name__ == "__main__":
    main()
