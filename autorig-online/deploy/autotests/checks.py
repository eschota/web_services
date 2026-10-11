"""AutoRig regression autotests: the checks (one case per process, run by autotests.py).

Every check returns metrics only; autotests.py compares them with the thresholds of corpus.json, so the verdicts live
in one place. The checks reuse the production code of the tree under test (MT: mt.rig_first, mt.fast_analysis,
mt.parts_cluster, mt.v3_numeric_qa, mt.live_voxels + mt.live_classic = the live judge's centring, mt.skin_postvalidate;
backend: fbx_ascii and the unit tests of the release) and Astra's rest-relative escape probe (vendor/).

    python3 -P checks.py --case <json> --work <dir>     -> one JSON line per check on stdout (autotests.py reads them)

Environment (set by autotests.py): MT_TREE (the MT tree under test, also MT_ROOT and on sys.path), AUTOTESTS_RELEASE
(the backend release under test), AUTOTESTS_CORPUS (the read-only corpus).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
import traceback

import numpy as np

HERE = pathlib.Path(__file__).resolve().parent
MT_TREE = pathlib.Path(os.environ.get("MT_TREE", "/srv/autorig/data/motion_transfer"))
MT_PROD = pathlib.Path("/srv/autorig/data/motion_transfer")
CORPUS = pathlib.Path(os.environ.get("AUTOTESTS_CORPUS", "/srv/autorig/data/autotests/corpus"))
RELEASE = pathlib.Path(os.environ.get("AUTOTESTS_RELEASE", "/srv/autorig/current"))
DB = pathlib.Path(os.environ.get("AUTORIG_DB", "/srv/autorig/data/db/autorig.db"))
PY = os.environ.get("AUTOTESTS_PY", sys.executable)
for p in (str(HERE / "vendor"), str(MT_TREE)):
    if p not in sys.path:
        sys.path.insert(0, p)

HAND_RX = re.compile(r"(?i)(hand|palm|thumb|index|middle|ring|pinky|finger|^c_(thumb|index|middle|ring|pinky))")
TORSO_RX = re.compile(r"(?i)^(mixamorig:)?(root|spine|chest|hips|pelvis|torso|abdomen|belly|c_root|c_spine|c_traj)")


def _sha(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _input(name: str) -> pathlib.Path:
    path = CORPUS / name
    if not path.is_file():
        raise FileNotFoundError(f"corpus file missing: {name} (run `autotests sync`)")
    return path


def _env():
    env = dict(os.environ)
    env.update(MT_ROOT=str(MT_TREE), PYTHONPATH=str(MT_TREE), PYTHONDONTWRITEBYTECODE="1")
    return env


def _child(args, cwd=None, timeout=600):
    t = time.time()
    p = subprocess.run(args, cwd=str(cwd or MT_TREE), env=_env(), capture_output=True, text=True, timeout=timeout)
    return p, round(time.time() - t, 3)


# ------------------------------------------------------------------------------------------------ skinned GLB model
def _renormalized(doc, binary):
    """A copy of the binary chunk with every float WEIGHTS_0 row scaled to sum 1 (source skins exported with more
    than four influences per vertex lose the rest); the GLB itself is never written."""
    buf = bytearray(binary)
    for mesh in doc.get("meshes", []):
        for prim in mesh.get("primitives", []):
            idx = (prim.get("attributes") or {}).get("WEIGHTS_0")
            if idx is None:
                continue
            acc = doc["accessors"][idx]
            if acc.get("componentType") != 5126 or acc.get("type") != "VEC4":
                continue
            view = doc["bufferViews"][acc["bufferView"]]
            start = int(view.get("byteOffset", 0)) + int(acc.get("byteOffset", 0))
            stride = int(view.get("byteStride", 16))
            arr = np.ndarray((acc["count"], 4), np.float32, buffer=buf, offset=start, strides=(stride, 4))
            tot = arr.sum(1, keepdims=True)
            arr[:] = np.where(tot > 1e-8, arr / np.maximum(tot, 1e-8), np.array([1, 0, 0, 0], np.float32))
    return bytes(buf)


class Skin:
    """A skinned GLB at rest, the way the viewer draws it: LBS-posed vertices, the joints' world positions."""

    def __init__(self, path):
        from mt import skin_postvalidate as SP
        t = time.time()
        self.path = pathlib.Path(path)
        doc, binary = SP.read_glb(self.path.read_bytes())
        try:
            rows, scale, topo = SP.prepare_primitives(doc, binary)
        except SP.InvalidArtifact as exc:                 # a source skin (assimp: >4 influences cut, sums < 1)
            if str(exc) != "malformed_skin_weights":
                raise
            binary = _renormalized(doc, binary)
            rows, scale, topo = SP.prepare_primitives(doc, binary)
            self.renormalized = True
        nodes = doc.get("nodes", [])
        W = SP.world_matrices(nodes, {})
        jnodes = sorted({int(j) for r in rows for j in r["joint_nodes"]})
        gidx = {j: i for i, j in enumerate(jnodes)}
        self.names = [str(nodes[j].get("name") or f"node_{j}") for j in jnodes]
        self.heads = np.array([W[j][:3, 3] for j in jnodes], np.float64)
        parent = {}
        for i, n in enumerate(nodes):
            for c in n.get("children") or []:
                parent[int(c)] = i
        jset = set(jnodes)
        self.parent = []
        for j in jnodes:
            p = parent.get(j)
            while p is not None and p not in jset:
                p = parent.get(p)
            self.parent.append(gidx[p] if p is not None else -1)
        P, F, J, Wt, off = [], [], [], [], 0
        for r in rows:
            P.append(SP.posed_world(r, W))
            F.append(r["triangles"] + off)
            m = np.array([gidx[int(j)] for j in r["joint_nodes"]], np.int64)
            J.append(m[r["joints"]])
            Wt.append(r["weights"])
            off += len(r["rest"])
        self.P, self.F = np.concatenate(P), np.concatenate(F)
        self.J, self.W = np.concatenate(J), np.concatenate(Wt)
        self.rows, self.scale, self.topology = rows, scale, topo
        self.H = float(np.ptp(self.P[:, 1])) or 1.0
        self.animations = [a.get("name") for a in doc.get("animations", [])]
        self.load_s = round(time.time() - t, 3)


def skeleton_fit(path, template_fingerprints=()):
    """Does the skeleton sit on this mesh at all? (d76f84c3 / af874411 / 7831327b: the converter shipped an unfitted
    67-joint template, 1.47-1.63x the mesh height or 2-5x its width)."""
    s = Skin(path)
    lo, hi = s.P.min(0), s.P.max(0)
    tol = 0.02 * s.H
    leaf_end = np.array([n.endswith(("_End", "_end")) for n in s.names])      # the live judge skips them too
    outside = np.any((s.heads < lo - tol) | (s.heads > hi + tol), axis=1) & ~leaf_end
    fp = hashlib.sha256(np.round(np.sort(s.heads, axis=0), 3).tobytes()).hexdigest()[:16]
    lat = int(np.argmax(np.ptp(s.P[:, [0, 2]], 0)) * 2)          # the wider horizontal axis (x or z)
    torso = [i for i, n in enumerate(s.names) if TORSO_RX.search(n) and not re.search(r"(?i)root|traj", n)]
    centre = (lo[lat] + hi[lat]) / 2
    spine_off = (round(float(np.abs(s.heads[torso, lat] - centre).max()) / s.H * 100, 2) if torso else None)
    return {"spine_lateral_offset_pct_H": spine_off,
            "joints": len(s.names), "vertices": int(len(s.P)), "mesh_height": round(s.H, 4),
            "height_ratio": round(float(np.ptp(s.heads[:, 1])) / s.H, 3),
            "width_ratio": round(float(np.ptp(s.heads[:, lat])) / max(float(np.ptp(s.P[:, lat])), 1e-9), 3),
            "joints_outside_bbox": int(outside.sum()),
            "joints_outside_bbox_names": [n for n, o in zip(s.names, outside) if o][:12],
            "skeleton_fingerprint": fp, "unfitted_template": int(fp in set(template_fingerprints)),
            "tail_joints": sum(1 for n in s.names if re.search(r"(?i)tail", n)),
            "clips": len(s.animations), "load_s": s.load_s}


def hand_weights(path):
    """Hand / finger influence where it does not belong (af874411: palm weights on torso vertices)."""
    s = Skin(path)
    hand = np.array([bool(HAND_RX.search(n)) for n in s.names])
    torso = np.array([bool(TORSO_RX.search(n)) for n in s.names])
    if not hand.any():
        return {"hand_joints": 0, "palm_on_torso": None, "far_hand_weight": None}
    hw = (s.W * hand[s.J]).sum(1)
    dom = s.J[np.arange(len(s.J)), s.W.argmax(1)]
    palm_on_torso = int(((hw >= 0.05) & torso[dom]).sum())
    hp = s.heads[hand]
    far = np.zeros(len(s.P), bool)
    cand = np.nonzero((hw >= 0.1) & (s.W.max(1) < 0.99))[0]      # a rigid prop on the hand (a sword) is not a leak
    for a in range(0, len(cand), 20000):
        idx = cand[a:a + 20000]
        d = np.linalg.norm(s.P[idx, None, :] - hp[None], axis=2).min(1)
        far[idx] = d > 0.15 * s.H
    return {"hand_joints": int(hand.sum()), "torso_joints": int(torso.sum()),
            "hand_weighted_vertices": int((hw >= 0.1).sum()), "palm_on_torso": palm_on_torso,
            "far_hand_weight": int(far.sum())}


def _nn_dist(A, B):
    """Distance of every point of A to its nearest point of B (OpenCV FLANN kd-trees, 128 checks)."""
    import cv2
    idx = cv2.flann_Index(np.ascontiguousarray(B, np.float32), {"algorithm": 1, "trees": 4})
    _, d2 = idx.knnSearch(np.ascontiguousarray(A, np.float32), 1, params={"checks": 128})
    return np.sqrt(np.maximum(d2.reshape(-1).astype(np.float64), 0.0))


def rest_drift(path, prepared):
    """The rig's rest geometry against the prepared source (7831327b: 1543 neck vertices moved up 0.79 in the rig
    rest; a valid bind cannot see it). Both are taken in their own glTF frames, normalised to height 1 at the feet."""
    from mt.fastrig import Source
    s = Skin(path)
    src = Source(pathlib.Path(prepared).read_bytes())
    A, B = s.P.copy(), np.asarray(src.positions, np.float64)

    def norm(X, ref):
        lo, hi = ref.min(0), ref.max(0)
        c = np.array([(lo[0] + hi[0]) / 2, lo[1], (lo[2] + hi[2]) / 2])
        return (X - c) / (float(hi[1] - lo[1]) or 1.0)

    # Two registrations, the better one counts: the source's frame for both (a displaced part grows the rig's own
    # bbox, so its own frame would rescale everything) and each in its own frame (an exporter that rescales the
    # whole model is not a drift).
    Bn = norm(B, B)
    best = None
    for An in (norm(A, B), norm(A, A)):
        d = _nn_dist(An, Bn)
        if best is None or (d > 0.02).sum() < (best[0] > 0.02).sum():
            best = (d, An)
    d, An = best
    back = _nn_dist(Bn, An)
    # The converter re-poses the mesh before the rig (arms straightened, legs corrected): a moved hand is normal
    # there, a taller rest is not (7831327b: the neck went up 0.79, the rest grew 69 %).
    return {"height_change_pct": round((float(np.ptp(A[:, 1])) / (float(np.ptp(B[:, 1])) or 1.0) - 1) * 100, 2),
            "vertices": int(len(A)), "source_vertices": int(len(B)),
            "moved_vertices": int((d > 0.02).sum()), "max_drift_pct_H": round(float(d.max()) * 100, 2),
            "p99_drift_pct_H": round(float(np.percentile(d, 99)) * 100, 3),
            "uncovered_source_vertices": int((back > 0.02).sum())}


def centring(path, work, res=96):
    """The live judge's centring (mt.live_classic.final_rig): every joint against the voxel erosion depth of the
    mesh -> ok / off / outside (d76f84c3: 66 of 67 red)."""
    from mt import live_voxels as LV
    from mt.live_classic import skeleton_from_glb
    run = pathlib.Path(work) / "centring"
    shutil.rmtree(run, ignore_errors=True)
    (run / "proj").mkdir(parents=True)
    (run / "live").mkdir()                                  # live.of() writes only into an existing live/ dir
    os.symlink(pathlib.Path(path).resolve(), run / "proj" / "model.glb")
    t = time.time()
    info = LV.run(run, res)
    bones = skeleton_from_glb(pathlib.Path(path))
    with np.load(run / "live" / "voxfield.npz") as f:
        height = float(f["dims"][1]) * float(f["cell"])
    marks = LV.joint_marks(run, bones, height)
    out = [m["bone"] for m in marks if m["verdict"] == "outside"]
    off = [m["bone"] for m in marks if m["verdict"] == "off"]
    return {"judged": len(marks), "ok": len(marks) - len(out) - len(off), "off": len(off), "outside": len(out),
            "outside_bones": out[:16], "off_bones": off[:16], "voxels_s": info.get("seconds"),
            "seconds": round(time.time() - t, 3)}


def prop_rigidity(path, work, words="", rig_json=None):
    """A held prop must move rigidly with one bone (66ba97ba: the converter skinned the warrior's sword into the body,
    BindCheck 445 hits, a retry). Props found by mt.parts_cluster on this very GLB."""
    from mt import parts_cluster as PC
    from mt.fastrig import Source
    s = Skin(path)
    run = pathlib.Path(work) / "props"
    shutil.rmtree(run, ignore_errors=True)
    run.mkdir(parents=True)
    rig = json.loads(pathlib.Path(rig_json).read_text(encoding="utf-8")) if rig_json else {}
    doc = PC.cluster(run, glb=pathlib.Path(path), rig=rig, words=words, export=False, write=True)
    face_part = np.load(run / "analysis" / "parts.npz")["face_part"]
    src = Source(pathlib.Path(path).read_bytes())
    key = {tuple(np.round(p, 6)): i for i, p in enumerate(s.P)}
    props = []
    for i, part in enumerate(doc["parts"]):
        if part.get("class") != "prop":
            continue
        vids = np.unique(np.asarray(src.faces)[face_part == i].reshape(-1))
        mine = [key.get(tuple(np.round(src.positions[v], 6))) for v in vids]
        mine = np.array([m for m in mine if m is not None], np.int64)
        if not len(mine):
            continue
        dom = s.J[mine, s.W[mine].argmax(1)]
        rigid = s.W[mine].max(1) >= 0.99
        names = sorted({s.names[d] for d in dom})
        top = np.bincount(dom).argmax()
        props.append({"id": part["id"], "kind": part.get("kind"), "verts": int(len(mine)),
                      "attached_bone": part.get("attached_bone"),
                      "rigid_fraction": round(float(rigid.mean()), 4),
                      "one_bone_fraction": round(float((dom == top).mean()), 4),
                      "dominant_bones": names[:8], "dominant_bone_count": len(names)})
    worst = min(props, key=lambda p: p["one_bone_fraction"] * p["rigid_fraction"]) if props else None
    return {"props_found": len(props), "props": props,
            "rigid_fraction": worst["rigid_fraction"] if worst else None,
            "one_bone_fraction": worst["one_bone_fraction"] if worst else None,
            "dominant_bone_count": worst["dominant_bone_count"] if worst else None}


def bone_outside(path, rig_json):
    """mt.bone_check on an MT rig (rig.json + rigged.glb): bone samples outside the mesh (16ce2f35: the heel and
    LeftToeBase outside the foot)."""
    from mt import bone_check as BC, joint_views as JV
    doc = json.loads(pathlib.Path(rig_json).read_text(encoding="utf-8"))
    m = JV.Model(pathlib.Path(path), doc)
    rep = BC.check(m)
    foot = {b: v["outside"] for b, v in rep["bones"].items() if re.search(r"(Foot|Toe|Heel)", b) and v["outside"]}
    return {"samples": rep["samples"], "outside_samples": rep["outside_samples"],
            "bones_outside": rep["bones_outside"][:16], "foot_outside_samples": int(sum(foot.values())),
            "foot_bones_outside": sorted(foot), "seconds": rep.get("seconds")}


def deformation_probe(path, clips=None, samples=12):
    """Astra's rest-relative escape probe (vendor/deformation_probe.py, staged by Astra 2026-10-10, too slow for
    every customer run): per clip, `samples` evenly spaced poses -> stretched edges, outlier vertices, detached
    components (7831327b: rigid detachment has zero stretched edges)."""
    from mt import skin_postvalidate as SP
    import deformation_probe as DP
    t0 = time.time()
    doc, binary = SP.read_glb(pathlib.Path(path).read_bytes())
    DP.validate_layout(doc)
    rows, scale, _ = SP.prepare_primitives(doc, binary)
    anims = SP.parse_animations(doc, binary)
    probe = DP.Probe(rows, scale)
    names = [c for c in (clips or list(anims)) if c in anims]
    per, worst = {}, {"stretched_edges": 0, "outlier_vertices": 0, "detached_components": 0}
    failed = 0
    checked = 0
    for name in names:
        channels, keys, times = anims[name]
        pick = [times[int(i)] for i in np.unique(np.linspace(0, len(times) - 1, min(samples, len(times))).astype(int))]
        bad = 0
        for t in pick:
            over = {}
            for node, pth, tt, vv, interp in channels:
                over.setdefault(node, {})[pth] = SP.sample_channel(tt, vv, t, pth, interp)
            W = SP.world_matrices(doc.get("nodes", []), over)
            r = probe.check(probe.pose(W), time=t, details=False)
            checked += 1
            bad += 0 if r["passed"] else 1
            worst["stretched_edges"] = max(worst["stretched_edges"], r["stretched_edges"])
            worst["outlier_vertices"] = max(worst["outlier_vertices"], r["outlier_vertices"])
            worst["detached_components"] = max(worst["detached_components"], len(r["detached_components"]))
        per[name] = bad
        failed += bad
    return {"clips": len(names), "poses": checked, "failed_poses": failed, "failed_by_clip": per, **worst,
            "probe_sha256": _sha(HERE / "vendor" / "deformation_probe.py")[:16], "seconds": round(time.time() - t0, 2)}


def limb_collision(path):
    """«Limb collision · V3» (mt/limb_collision.py, landed 2026-10-10): forearm / hand inside the body, sleeve smear
    and arm asymmetry on the posed mesh of every clip (98c1247c: the right forearm through the torso in Idle)."""
    try:
        from mt import limb_collision as L
    except ImportError:
        return {"pending": True, "why": "mt/limb_collision.py is not in the tree under test"}
    rep = L.detect(pathlib.Path(path).read_bytes())
    sm = L.summary(rep)
    rank = {"none": 0, "low": 1, "medium": 2, "high": 3}
    worst = sm.get("worst") or {}
    sides = sm.get("sides") or {}
    return {"severity": sm.get("severity"), "severity_rank": rank.get(sm.get("severity"), -1),
            "flagged_sides": sorted(sm.get("flagged_sides") or []),
            "right_rank": rank.get((sides.get("R") or {}).get("severity"), -1),
            "left_rank": rank.get((sides.get("L") or {}).get("severity"), -1),
            # 2ad5c0c8: arms not bound to their bones at all (0 forearm/hand vertices) - never a pass
            "min_arm_vertices": min([int((v or {}).get("arm_vertices") or 0) for v in sides.values()] or [0]),
            "worst_share": worst.get("share"), "worst_depth_pct_H": worst.get("max_depth_pct_H"),
            "frames_checked": sm.get("frames_checked"), "seconds": sm.get("seconds")}


def pose_stabilization(ctx):
    """«Limb stabilization · V3» (mt/limb_stabilize.py, 2026-10-10): a posed input (d1522b45: both arms over the head,
    a knee raised) read from its own skeleton, the chains turned to T / A before the fast rig; the fit check
    «skeleton not fitted» on the rig (weight mass on two bones, limb bones without vertices, hand off its vertices)."""
    run = ctx["run"]
    st = json.loads((run / "stab" / "stabilization.json").read_text()) if (run / "stab" / "stabilization.json").is_file() else {}
    rig = json.loads((run / "rig" / "rig.json").read_text())
    fc = rig.get("fit_check") or {}
    after = st.get("measure_after") or {}
    arms, legs = after.get("arms") or {}, after.get("legs") or {}
    applied = bool(st.get("applied"))
    return {"source_skin": int(st.get("source") == "source_skin"), "state_before": st.get("state"),
            "posed_before": int(st.get("state") == "posed"), "applied": int(applied),
            "state_after": st.get("state_after"), "target_pose": st.get("target_pose"),
            "arm_elevation_after_abs_max": max([abs(a["elevation_deg"]) for a in arms.values()] or [0]) if applied else None,
            "elbow_bend_after_max": max([a["elbow_bend_deg"] for a in arms.values()] or [0]) if applied else None,
            "knee_bend_after_max": max([l["knee_bend_deg"] for l in legs.values()] or [0]) if applied else None,
            "max_move_pct_H": st.get("max_move_pct_H"),
            "duplicated_vertices": (st.get("contacts_split") or {}).get("duplicated_vertices"),
            "stabilize_s": st.get("seconds"),
            "fit_ok": int(bool(fc.get("ok"))), "fit_reasons": fc.get("reasons"), "top2_share": fc.get("top2_share"),
            "min_limb_share": fc.get("min_limb_share"), "hand_gap_pct_H": fc.get("hand_gap_pct_H"),
            "clips": len(rig.get("clips") or []), "source_pose_clip": int("Source pose" in (rig.get("clips") or []))}


# Astra TODO · horse (a742491a): what the Unity viewer's glTFast 6.20 loads (no Draco / meshopt / KTX packages)
GLTFAST_REQUIRED_OK = {"KHR_mesh_quantization", "KHR_texture_transform", "KHR_materials_pbrSpecularGlossiness",
                       "KHR_materials_unlit", "KHR_materials_variants", "KHR_materials_transmission",
                       "EXT_mesh_gpu_instancing", "KHR_lights_punctual", "KHR_materials_clearcoat"}
LEG_BONE_RX = re.compile(r"^(Left|Right)(Front|Hind)(UpLeg|Leg|Foot|Toe)$")


def viewer_opens(path):
    """Astra TODO · horse: the viewer refuses a GLB that requires an extension glTFast lacks (EXT_texture_webp, Draco,
    meshopt, KTX2) and cannot decode WebP / KTX2 images. -> the counts the thresholds judge."""
    from mt import skin_postvalidate as SP
    doc = SP.read_glb(pathlib.Path(path).read_bytes())[0]
    req = list(doc.get("extensionsRequired") or [])
    bad = [e for e in req if e not in GLTFAST_REQUIRED_OK]
    imgs = []
    for im in doc.get("images") or []:
        mime = im.get("mimeType") or ("image/png" if str(im.get("uri", "")).lower().endswith(".png") else
                                      "image/jpeg" if re.search(r"\.jpe?g$", str(im.get("uri", "")).lower()) else "?")
        imgs.append(mime)
    bad_img = [m for m in imgs if m not in ("image/png", "image/jpeg")]
    return {"required": req, "unsupported_required": len(bad), "unsupported_list": bad, "images": len(imgs),
            "unsupported_images": len(bad_img), "image_types": sorted(set(imgs)),
            "bytes": pathlib.Path(path).stat().st_size}


def quadruped_legs(path, rig_json=None):
    """Astra TODO · horse: a quadruped rig's legs isolated by topology. From the GLB alone (bone names Left/Right +
    Front/Hind + UpLeg/Leg/Foot/Toe): vertices weighted (> 5 %) to a front and a hind leg, welded edges whose ends
    belong to two different legs (dominant), front|hind among them; from rig.json: the rig_check stretch."""
    s = Skin(path)
    keys = sorted({m.group(1) + m.group(2) for n in s.names for m in [LEG_BONE_RX.match(n)] if m})
    if len(keys) < 4:
        return {"legs": len(keys), "front_hind_vertices": None}
    G = np.zeros((len(s.J), len(keys)))
    for k, key in enumerate(keys):
        cols = [i for i, n in enumerate(s.names) if (m := LEG_BONE_RX.match(n)) and m.group(1) + m.group(2) == key]
        G[:, k] = (np.isin(s.J, cols) * s.W).sum(1)
    front = np.array(["Front" in k for k in keys])
    has = G > 0.05
    fh_v = int((has[:, front].any(1) & has[:, ~front].any(1)).sum())
    rest = np.concatenate([r["rest"] for r in s.rows])
    q = np.round((rest - rest.min(0)) / max(float(np.ptp(rest, 0).max()), 1e-12) * 1e5).astype(np.int64)
    _, wid = np.unique(q, axis=0, return_inverse=True)
    wid = wid.reshape(-1)
    dom = np.where(G.max(1) >= 0.5, G.argmax(1), -1)
    wdom = np.full(int(wid.max()) + 1, -1)
    wdom[wid] = dom
    e = np.concatenate([s.F[:, [0, 1]], s.F[:, [1, 2]], s.F[:, [2, 0]]])
    eu, ev = wid[e[:, 0]], wid[e[:, 1]]
    key = np.unique(np.sort(np.stack([eu, ev], 1), 1), axis=0)
    a, b = wdom[key[:, 0]], wdom[key[:, 1]]
    cross = (a >= 0) & (b >= 0) & (a != b)
    fh_e = cross & (front[np.maximum(a, 0)] != front[np.maximum(b, 0)])
    out = {"legs": len(keys), "front_hind_vertices": fh_v, "cross_leg_edges": int(cross.sum()),
           "front_hind_edges": int(fh_e.sum()), "vertices": int(len(s.J))}
    if rig_json and pathlib.Path(rig_json).is_file():
        rig = json.loads(pathlib.Path(rig_json).read_text(encoding="utf-8"))
        st = (rig.get("checks") or {}).get("rig_check_stretch") or {}
        frames = [v for k, v in st.items() if k.startswith("t=") and isinstance(v, dict)]
        out.update(rig_check_edges_over_2x=max([int(v.get("edges_over_2x") or 0) for v in frames] or [0]),
                   rig_check_edges_over_4x=max([int(v.get("edges_over_4x") or 0) for v in frames] or [0]),
                   rig_check_max_stretch=max([float(v.get("max_stretch") or 0) for v in frames] or [0]),
                   forward_axis=rig.get("forward_axis"), forward_check=rig.get("forward_check"))
    return out


def mt_quadruped_rig(ctx, input_name, what="", forward_axis="+z"):
    """Astra TODO · horse: the rig the V3 conveyor builds once Vision has answered (mt.rig_first.build on a run whose
    classify says quadruped and whose projections carry Vision's front), so a front across the body is exercised."""
    run = pathlib.Path(ctx["work"]) / "run"
    shutil.rmtree(run, ignore_errors=True)
    (run / "proj").mkdir(parents=True)
    shutil.copyfile(_input(input_name), run / "proj" / "model.glb")
    (run / "proj" / "manifest.json").write_text(json.dumps({"forward_axis": forward_axis}))
    (run / "phases.json").write_text(json.dumps({"phases": [{"id": "classify", "result": {
        "what": what, "body": "quadruped", "props": ""}}]}))
    code = ("import json, sys; from mt import rig_first as R; d = R.build(sys.argv[1], sys.argv[2]); "
            "print(json.dumps({k: d.get(k) for k in ('body_plan', 'forward_axis', 'forward_check', 'timings_s')} "
            "| {'bones': len(d.get('bones') or [])}, default=str))")
    p, wall = _child([PY, "-P", "-c", code, str(run), what], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"rig_first.build exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    out = json.loads(p.stdout.strip().splitlines()[-1])
    ctx["run"], ctx["words"] = run, what
    from mt import skin_postvalidate as SP
    doc, binary = SP.read_glb((run / "proj" / "model.glb").read_bytes())
    P = np.concatenate([r["rest"] for r in SP.prepare_primitives(doc, binary)[0]]) if doc.get("skins") else None
    if P is None:
        from mt import fastrig as F
        P = F.Source((run / "proj" / "model.glb").read_bytes()).positions
    ext = np.ptp(P, 0)
    long_ax = "z" if ext[2] >= ext[0] else "x"
    fc = out.get("forward_check") or {}
    return {"seconds": wall, "bones": out.get("bones"), "body_plan": out.get("body_plan"),
            "forward_axis": out.get("forward_axis"), "forward_given": forward_axis,
            "forward_axis_along_body": int(str(out.get("forward_axis") or "")[-1:] == long_ax),
            "forward_changed": int(bool(fc.get("changed"))), "rig_total_s": (out.get("timings_s") or {}).get("fastrig")}


def leg_joints(path, rig_json=None):
    """«Leg fit · V3» (2026-10-11, task 7018c8b8: the ankle at the top of a wide boot, 18.9 % H, the knee a stump):
    mt/leg_fit.rules on a rigged GLB (+ rig.json): ankle height and share of the foot length, the ankle behind the
    heel line, the knee's share of hip -> ankle and its centring, left / right ankle symmetry."""
    try:
        from mt import leg_fit as LF
    except ImportError:                                   # the module is not on the tree under test yet
        return {"pending": True, "why": "mt/leg_fit.py is not in the tree under test"}
    r = LF.rules_for_glb(path, rig_json)
    sides = r.get("sides") or {}
    out = {"failed": len(r.get("failed") or []), "codes": r.get("failed"),
           "ankle_max_pct_H": max([float(v.get("ankle_pct_H") or 0) for k, v in sides.items() if isinstance(v, dict)] or [0]),
           "ankle_diff_pct_H": sides.get("ankle_diff_pct_H", 0.0),
           "knee_offset_max": max([float(v.get("knee_offset_r") or 0) for k, v in sides.items() if isinstance(v, dict)] or [0]),
           "knee_t": {k: v.get("knee_t") for k, v in sides.items() if isinstance(v, dict)},
           "ankle_over_foot": {k: v.get("ankle_over_foot") for k, v in sides.items() if isinstance(v, dict)}}
    return out


def rig_stretch(ctx):
    """«Skinning · V3» (2026-10-11): does the fast rig tear its own mesh?  rig.json's rig_check stretch (fastrig's
    looping test clip at three frames: edges over 2x / 4x their rest length that also grew), the per-bone weight
    sanity, and skin_tools.measure on the GLB (synthetic joint poses + the first clip; the welded mesh, so a split
    seam counts there but not in rig_check)."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    c = rig.get("checks") or {}
    st = {k: v for k, v in (c.get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    out = {"sum_edges_over_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
           "sum_edges_over_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
           "max_stretch": max([float(v.get("max_stretch") or 0) for v in st.values()] or [0]),
           "worst_pairs": (c.get("rig_check_stretch") or {}).get("worst"),
           "limb_reach_moved": c.get("limb_reach_moved"), "contact_split_faces": c.get("limb_contact_split_faces"),
           "unweighted_vertices": c.get("unweighted_vertices"), "bones": len(rig.get("bones") or [])}
    try:
        from mt import skin_tools as ST
        r = ST.Rig((run / "rig" / "rigged.glb").read_bytes())
        m = ST.measure(r, r.Wg)
        out.update({f"measure_{k}": v for k, v in (m.get("total") or {}).items()})
        dom = r.Wg.argmax(1)
        cnt = np.bincount(dom, minlength=len(r.names))
        deform = [i for i, n in enumerate(r.names) if not n.endswith("_End") and not n.startswith(("Prop", "Hair_"))]
        limb = [i for i in deform if r.names[i].endswith(("Arm", "ForeArm", "UpLeg", "Leg"))]
        out["empty_limb_bones"] = int(sum(1 for i in limb if cnt[i] == 0))
        out["top_bone_share"] = round(float(cnt.max()) / max(int(cnt.sum()), 1), 4)
    except Exception as exc:                                   # noqa: BLE001 - the rig.json numbers still judge
        out["measure_error"] = f"{type(exc).__name__}: {exc}"[:160]
    return out


def hair_limbs(ctx):
    """Task 137bd37f (dreadlocks beside the shoulders): how much skin above the arm joints the arm bones own, per
    side; plus the absolute-size facts of the source (the rig path is scale-invariant: same numbers at 0.01x / 100x)."""
    from mt import fastrig as F, limb_stabilize as LS
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    data = (run / "rig" / "rigged.glb").read_bytes()
    js, b = F.read_glb(data)
    src = F.Source(data)
    r = LS.SourceRig(src, js, b)
    P = src.positions
    H = float(np.ptp(P[:, 1])) or 1.0
    y0 = float(P[:, 1].min())
    dom = r.vj[np.arange(len(r.vj)), r.vw.argmax(1)]
    names = r.names
    heads = {bn["name"]: bn["head"] for bn in rig.get("bones") or []}
    out = {"height_units": round(H, 4), "vertices": int(len(P))}
    total_above = 0
    for side in ("Left", "Right"):
        arm = [i for i, n in enumerate(names) if n.startswith(side) and n.endswith(("Shoulder", "Arm", "ForeArm", "Hand"))]
        if not arm or f"{side}Arm" not in heads:
            continue
        top = float(heads[f"{side}Arm"][1]) + 0.03 * H
        m = np.isin(dom, arm)
        above = int((m & (P[:, 1] > top)).sum())
        total_above += above
        out[f"{side.lower()}_arm_vertices"] = int(m.sum())
        out[f"{side.lower()}_arm_above_joint"] = above
    out["arm_vertices_above_joints"] = total_above
    out["neck_head_share"] = round(float(np.isin(dom, [i for i, n in enumerate(names) if n in ("Neck", "Head")]).mean()), 4)
    return out


def constitution(ctx):
    """Body constitution · V3 (mt/constitution.py): the humanoid subcategory by body constitution of the MT run's
    model (geometry only here; the vision judge runs in the conveyor), its confidence and proportions."""
    try:
        from mt import constitution as BC
    except ImportError:
        return {"pending": True, "why": "mt/constitution.py is not in the tree under test"}
    run = ctx["run"]
    doc = BC.classify(run, vision=False)
    p = doc.get("proportions") or {}
    sub = doc.get("subcategory")
    out = {"subcategory": sub, "confidence": doc.get("confidence"),
           "head": p.get("head"), "legs": p.get("legs"), "arms": p.get("arms"), "arm_pose": p.get("arm_pose"),
           "torso_width": p.get("torso_width"), "torso_depth": p.get("torso_depth"),
           "arm_stubs": p.get("arm_stubs"), "priors_applied": (doc.get("priors") or {}).get("applied"),
           "priors_normal": 1 if (doc.get("priors") or {}).get("applied") == "normal" else 0,
           "seconds": (doc.get("geometry") or {}).get("seconds")}
    for name in BC.SUBCATEGORIES:
        out["is_" + name] = 1 if sub == name else 0
    out["arm_pose_t"] = 1 if p.get("arm_pose") == "t" else 0
    return out


def rig_joints(ctx):
    """Where the fast rig put its joints, as shares of the height: the arm joints' distance from the mid-plane
    (an arm inside the torso reads near 0), the shoulder and hip heights, the hand's distance from the hips.
    The potato 583622f3: LeftArm / RightArm were the proportions guess 0.11 H from the axis, inside the belly."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    heads = {b["name"]: np.asarray(b["head"], float) for b in rig.get("bones") or []}
    H = float(rig.get("model_height_units") or 1.0)
    y0 = min(float(h[1]) for h in heads.values()) if heads else 0.0
    out = {"bones": len(heads), "body_plan": rig.get("body_plan"),
           "constitution": (rig.get("constitution") or {}).get("subcategory"),
           "constitution_confidence": (rig.get("constitution") or {}).get("confidence"),
           "arm_joints_from_constitution": len((rig.get("constitution") or {}).get("arm_joints_from") or [])}
    if "Hips" in heads:
        hips = heads["Hips"]
        out["hips_y_pct_H"] = round(float(hips[1] - y0) / H * 100, 2)
        for side in ("Left", "Right"):
            if f"{side}Arm" in heads:
                a = heads[f"{side}Arm"]
                out[f"{side.lower()}_arm_off_axis_pct_H"] = round(float(np.hypot(a[0] - hips[0], a[2] - hips[2])) / H * 100, 2)
                out[f"{side.lower()}_arm_y_pct_H"] = round(float(a[1] - y0) / H * 100, 2)
            if f"{side}Hand" in heads:
                hd = heads[f"{side}Hand"]
                out[f"{side.lower()}_hand_off_axis_pct_H"] = round(float(np.hypot(hd[0] - hips[0], hd[2] - hips[2])) / H * 100, 2)
        offs = [v for k, v in out.items() if k.endswith("_arm_off_axis_pct_H")]
        out["min_arm_off_axis_pct_H"] = min(offs) if offs else None
    return out


# ------------------------------------------------------------------------------------------------ MT pipeline steps
def mt_arm_clearance(ctx):
    """«V3 triage · rig quality» (mt/arm_clearance.py, 2026-10-10): what the V3 conveyor does after the retarget - the
    limb check, then, a side flagged, the clips keep the forearm / hand out of the body (a new rig version, only when
    the detector says better). The metrics are limb_collision's on the rig the customer then gets. Run it last in a
    case: it may replace the case run's rig/rigged.glb."""
    run = ctx["run"]
    try:
        from mt import arm_clearance  # noqa: F401
    except ImportError:
        return {"pending": True, "why": "mt/arm_clearance.py is not in the tree under test"}
    p, wall = _child([PY, "-P", "-m", "mt.arm_clearance", "--dir", str(run), "--check"], timeout=900)
    if p.returncode != 0:
        raise RuntimeError(f"arm_clearance exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    doc = json.loads(p.stdout)
    out = limb_collision(run / "rig" / "rigged.glb")
    out.update(clearance_seconds=wall, clearance_applied=bool(doc.get("version") and doc.get("better")),
               clearance_skipped=doc.get("skipped"))
    return out


def mt_rig_first(ctx, input_name, words="", forward_axis=""):
    """The rig the viewer gets first (classic mirror + V3 conveyor): python -m mt.rig_first on the upload, like
    mt/classic_mirror.py does at upload time. forward_axis: the front the conveyor's projections / Vision know
    (proj/forward.json), for a model whose symmetry alone cannot tell it (the potato 583622f3: guess 0.32)."""
    run = pathlib.Path(ctx["work"]) / "run"
    shutil.rmtree(run, ignore_errors=True)
    run.mkdir(parents=True)
    if forward_axis:                                     # as the conveyor's projections record it (rig_first then
        (run / "proj").mkdir(parents=True, exist_ok=True)   # skips its own guess)
        (run / "proj" / "manifest.json").write_text(json.dumps({"forward_axis": forward_axis, "source": "autotests"}))
    p, wall = _child([PY, "-P", "-m", "mt.rig_first", "--dir", str(run), "--glb", str(_input(input_name)),
                      "--words", words], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"rig_first exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    out = json.loads(p.stdout.strip().splitlines()[-1])
    ctx["run"], ctx["words"] = run, words
    parts = (json.loads((run / "analysis" / "parts.json").read_text()) if (run / "analysis" / "parts.json").is_file()
             else {"parts": []})
    props = [x for x in parts["parts"] if x.get("class") == "prop" and x.get("separate")]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    return {"seconds": wall, "rig_total_s": out["timings_s"].get("total"), "timings_s": out["timings_s"],
            "bones": out.get("bones"), "body_plan": out.get("body_plan"), "clips": len(out.get("clips") or []),
            "props": len(props), "props_in_hand": sum(1 for x in props if re.search(r"Hand$", str(x.get("attached_bone") or ""))),
            "prop_bones": [x.get("attached_bone") for x in props],
            "prop_rig_bones": [f'{b["name"]}<{b.get("parent")}' for b in rig.get("bones", [])
                               if str(b.get("name", "")).startswith("Prop")],
            "prop_bones_held": sum(1 for b in rig.get("bones", []) if str(b.get("name", "")).startswith("Prop")
                                   and re.search(r"(Hand|ForeArm)$", str(b.get("parent") or ""))),
            "prop_bones_n": sum(1 for b in rig.get("bones", []) if str(b.get("name", "")).startswith("Prop")),
            # V3 triage rig_stretch: the rig's own rig_check, edges over 2x / 4x summed over its check times
            "rig_check_2x": sum(int(v.get("edges_over_2x") or 0) for k, v in
                                ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items()
                                if str(k).startswith("t=") and isinstance(v, dict)),
            "rig_check_4x": sum(int(v.get("edges_over_4x") or 0) for k, v in
                                ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items()
                                if str(k).startswith("t=") and isinstance(v, dict))}


def hand_rig(ctx):
    """Hands rig · V3 (2026-10-11): the hands branch of rig_first on this case's MT run: the body plan, hands and
    their sides, fingers and thumbs, forearms, finger samples outside the mesh, finger weights leaking into a
    neighbour, the Fist clip's stretch (rig.json of mt/handrig.py)."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    hands = rig.get("hands") or []
    hc = (rig.get("checks") or {}).get("hand_checks") or {}
    st = {k: v for k, v in ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    return {"body_plan": rig.get("body_plan"), "hands": len(hands), "sides": "".join(h.get("side", "?")[0] for h in hands),
            "fingers_min": min([v.get("fingers", 0) for v in hc.values()] or [0]),
            "thumbs": sum(1 for h in hands if h.get("thumb_vertices")),
            "forearms": sum(1 for h in hands if h.get("forearm")),
            "fused": sum(1 for h in hands if h.get("fused")),
            "palm_confidence_min": min([h.get("palm_confidence", 0) for h in hands] or [0]),
            "bones": len(rig.get("bones") or []), "deform_bones": (rig.get("checks") or {}).get("deform_bones"),
            "joints_outside": len((rig.get("checks") or {}).get("joints_outside_mesh") or []),
            "finger_outside": sum(int(v.get("finger_samples_outside") or 0) for v in hc.values()),
            "finger_leak": sum(int(v.get("finger_leak_vertices") or 0) for v in hc.values()),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "stretch_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
            "unweighted": (rig.get("checks") or {}).get("unweighted_vertices"),
            "clips": len(rig.get("clips") or []), "rig_total_s": (rig.get("timings_s") or {}).get("total")}


def hand_detect(ctx, input_name, words=""):
    """The hands detector alone (mt.handrig --detect) on a corpus input: a control on bodies (hands must be false)
    and a detector case on hands-only models."""
    run = pathlib.Path(ctx["work"]) / "detect" / pathlib.Path(input_name).stem
    shutil.rmtree(run, ignore_errors=True)
    (run / "proj").mkdir(parents=True)
    shutil.copyfile(_input(input_name), run / "proj" / "model.glb")
    p, wall = _child([PY, "-P", "-m", "mt.handrig", "--dir", str(run), "--detect", "--words", words], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"handrig detect exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    d = json.loads(p.stdout)
    return {"seconds": wall, "hands": bool(d.get("hands")), "confidence": d.get("confidence"), "why": d.get("why"),
            "groups": len(d.get("groups") or []),
            "max_branches": max([g.get("branches") or 0 for g in d.get("groups") or []] or [0])}


def mt_fast_analysis(ctx):
    """The V3 fast analysis stage (mt.fast_analysis: bone_check, heel rules, symmetry, proportions, parts)."""
    run = ctx["run"]
    p, wall = _child([PY, "-P", "-m", "mt.fast_analysis", "--dir", str(run)], timeout=600)
    if p.returncode != 0:
        raise RuntimeError(f"fast_analysis exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    fa = json.loads((run / "analysis" / "fast.json").read_text())
    res = fa.get("results") or {}
    bo = res.get("bone_outside_check") or {}
    heel = res.get("foot_heel_rules") or {}
    foot_bad = sum(1 for s in ("left", "right") for v in ((heel.get(s) or {}).get("after") or {}).values() if v is False)
    errors = [k for k, v in res.items() if isinstance(v, dict) and v.get("error")]
    return {"seconds": wall, "category": (fa.get("category") or {}).get("category"),
            "outside_before": bo.get("outside_before"), "outside_after": bo.get("outside_after"),
            "bones_outside_before": ((bo.get("before") or {}).get("bones_outside") or [])[:16],
            "bones_outside_after": ((bo.get("after") or {}).get("bones_outside") or [])[:16],
            "foot_rules_failed_after": foot_bad, "op_errors": len(errors), "op_error_ids": errors,
            "leg_rules_failed": len((res.get("leg_joint_rules") or {}).get("failed") or []),   # Leg fit V3
            "leg_rule_codes": (res.get("leg_joint_rules") or {}).get("failed"),
            "warnings": len(fa.get("warnings") or []), "symmetry_worst_pct_H": (res.get("symmetry_check") or {}).get("worst_pct_H"),
            "ops_total_s": (fa.get("timings") or {}).get("total")}


def s_doc(path):
    from mt import skin_postvalidate as SP
    return SP.read_glb(pathlib.Path(path).read_bytes())[0]


def mt_numeric_qa(ctx, clips=None, cap=2000):
    """The V3 numeric deformation QA, chunked across workers (mt.v3_numeric_qa), on the MT rig."""
    run = ctx["run"]
    glb = run / "rig" / "rigged.glb"
    s = Skin(glb)
    names = [c for c in (clips or s.animations) if c in s.animations]
    if not names:
        return {"clips": 0, "complete": False, "status": "no_clips"}
    acc = [int(x.get("count") or 0) for x in s_doc(glb).get("accessors") or []]
    need = 1
    for an in s_doc(glb).get("animations") or []:                # the conveyor's own cap rule (v3_conveyor.py)
        if an.get("name") in names:
            need = max(need, max([acc[x["input"]] for x in an.get("samplers") or []] or [1]) * 2 + 1)
    cap = min(need, int(cap))
    cj = run / "rig" / ".autotests_clips.json"
    cj.write_text(json.dumps(names))
    out = run / "rig" / ".autotests_numeric.json"
    p, wall = _child([PY, "-P", "-m", "mt.v3_numeric_qa", str(glb), _sha(glb), str(cj), str(cap), str(out)], timeout=900)
    if p.returncode != 0 or not out.is_file():
        raise RuntimeError(f"v3_numeric_qa exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    rep = json.loads(out.read_text())
    clips_doc = rep.get("clips") or {}
    bad = sum(int(l.get("bad_frames") or 0) for c in clips_doc.values() for l in c.get("layers") or [])
    frames = sum(int(l.get("checked_frames") or 0) for c in clips_doc.values() for l in c.get("layers") or [])
    mx = max([int(l.get("max_stretched_edge_count") or 0) for c in clips_doc.values() for l in c.get("layers") or []] or [0])
    return {"seconds": wall, "clips": len(names), "status": rep.get("status"), "complete": bool(rep.get("complete")),
            "clips_passed": sum(1 for c in clips_doc.values() if c.get("numeric_deformation_passed")),
            "bad_frames": bad, "checked_frames": frames, "bad_frame_share": round(bad / max(frames, 1), 4),
            "max_stretched_edges": mx, "vertices": int(len(s.P)), "sample_cap": cap,
            "workers": (rep.get("parallel") or {}).get("workers")}


def numeric_qa_file(ctx, target, cap=2000):
    """QA calibration V3: the V3 numeric QA (visible defects, per-clip aggregates) and the pops check of anim_qa on a
    corpus GLB, in a work copy (the live log of the QA child lands there, never in a customer run)."""
    run = pathlib.Path(ctx["work"]) / "numeric" / pathlib.Path(target).stem / "rig"
    shutil.rmtree(run.parent, ignore_errors=True)
    run.mkdir(parents=True)
    glb = run / "rigged.glb"
    shutil.copyfile(target, glb)
    doc = s_doc(glb)
    names = [a.get("name") for a in doc.get("animations") or []]
    acc = [int(x.get("count") or 0) for x in doc.get("accessors") or []]
    need = 1
    for an in doc.get("animations") or []:
        need = max(need, max([acc[x["input"]] for x in an.get("samplers") or []] or [1]) * 2 + 1)
    cj, out = run / "clips.json", run / "numeric.json"
    cj.write_text(json.dumps(names))
    p, wall = _child([PY, "-P", "-m", "mt.v3_numeric_qa", str(glb), _sha(glb), str(cj), str(min(need, int(cap))),
                      str(out)], timeout=900)
    if p.returncode != 0 or not out.is_file():
        raise RuntimeError(f"v3_numeric_qa exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    rep = json.loads(out.read_text())
    clips = rep.get("clips") or {}
    vis = [c.get("visible") or {} for c in clips.values()]
    from mt import anim_qa
    aq = anim_qa.check(glb.read_bytes())
    return {"seconds": wall, "status": rep.get("status"), "rule": rep.get("verdict_rule"),
            "passed": bool(rep.get("numeric_deformation_passed")), "clips": len(clips),
            "clips_passed": sum(1 for c in clips.values() if c.get("numeric_deformation_passed")),
            "tear_clips": sum(1 for v in vis if "tear" in (v.get("why") or [])),
            "extent_clips": sum(1 for v in vis if "extent" in (v.get("why") or [])),
            "worst": max([float(v.get("worst_stretch") or 0) for v in vis] or [0.0]),
            "peak_share": max([float(v.get("peak_share") or 0) for v in vis] or [0.0]),
            "anim_flips": sum(len(r["flips"]) for r in aq.values()),
            "anim_spikes": sum(len(r["spikes"]) for r in aq.values())}


# ------------------------------------------------------------------------------------------------ backend / intake
def ascii_fbx_intake(input_name, work):
    """b5b2a520 / 63bf5d35: an ASCII FBX (AssetStudio export without the `a:` key) must become a GLB on the VPS
    (backend fbx_ascii.py, assimp), because the farm's Blender cannot read it (FBX_ASCII_UNSUPPORTED)."""
    backend = RELEASE / "autorig-online" / "backend"
    sys.path.insert(0, str(backend))
    import fbx_ascii
    src = pathlib.Path(work) / "ascii" / _input(input_name).name
    src.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_input(input_name), src)
    t = time.time()
    is_ascii = fbx_ascii.is_ascii_fbx(src)
    res = fbx_ascii.fbx_to_glb(src, src.with_suffix(".glb"))
    took = round(time.time() - t, 3)
    out = src.with_suffix(".glb")
    verts = None
    if out.is_file():
        from mt.fastrig import Source
        verts = int(len(Source(out.read_bytes()).positions))
    return {"is_ascii": bool(is_ascii), "ok": bool(res.get("ok", out.is_file())), "glb_vertices": verts or 0,
            "seconds": took, "result": {k: v for k, v in res.items() if k in ("ok", "error", "repaired", "keys_fixed")}}


def backend_unittests(modules):
    backend = RELEASE / "autorig-online" / "backend"
    p, wall = _child([PY, "-m", "unittest", *modules], cwd=backend, timeout=600)
    tail = (p.stderr or "")[-4000:]
    ran = re.search(r"Ran (\d+) test", tail)
    fails = re.search(r"failures=(\d+)", tail)
    errs = re.search(r"errors=(\d+)", tail)
    return {"seconds": wall, "ran": int(ran.group(1)) if ran else 0, "exit": p.returncode,
            "failures": int(fails.group(1)) if fails else 0, "errors": int(errs.group(1)) if errs else 0,
            "tail": tail[-600:] if p.returncode else ""}


def task_page_js(files):
    """The task page's JS must parse (node --check), the same gate live_static.py applies to an overlay put."""
    static = RELEASE / "autorig-online" / "static"
    bad, checked = [], 0
    for rel in files:
        for path in sorted(static.glob(rel)):
            checked += 1
            p = subprocess.run(["node", "--check", str(path)], capture_output=True, text=True, timeout=60)
            if p.returncode:
                bad.append({"file": str(path.relative_to(static)), "error": (p.stderr or "")[-300:]})
    return {"checked": checked, "failed": len(bad), "failures": bad}


def v3_export_fbx(input_name, timeout=300, worker=None, spec=None):
    """Downloads · V3: a corpus V3 rig through the real export queue and a Blender export worker -> a binary FBX
    whose re-import (inside the worker) finds one armature, the skinned mesh and every take. ``worker`` pins the
    job to one box (f1, f2, f7, f13) to prove that box; without it any free worker takes it. ``spec`` is a custom
    export (the task page's options: {"clips": [...], "mesh": false, "mixamo": true, "format": "fbx"|"glb"|"blend",
    "preset": …}); the file is then checked for its own format."""
    import importlib
    import pwd
    import time

    backend = str(RELEASE / "autorig-online" / "backend")
    if backend not in sys.path:
        sys.path.insert(0, backend)
    D = importlib.import_module("task_downloads_v3")
    data = _input(input_name).read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    tag = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:8] if spec else ""
    folder = D.QUEUE_DIR.parent / "autotests" / (sha[:16] + (f"-{worker}" if worker else "") + (f"-{tag}" if tag else ""))
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    (folder / "rigged.glb").write_bytes(data)
    try:                                                   # the backend (user autorig) writes the job's progress
        pw = pwd.getpwnam("autorig")
        for path in (folder.parent, folder, folder / "rigged.glb"):
            os.chown(path, pw.pw_uid, pw.pw_gid)
    except (KeyError, PermissionError):
        pass
    online = D.worker_online()
    t0 = time.time()
    if spec:
        ext = spec.get("format", "fbx")
        job_spec = {"id": f"x-{'0' * 12}.{ext}", "ext": ext, "clip": None, "clip_index": None, "by": "worker",
                    "worker_spec": spec}
    else:
        ext, job_spec = "fbx", D.parse_fmt("fbx", [])
    D.enqueue(folder, "autotests", {"sha16": sha[:16], "sha256": sha}, job_spec, pin=worker)
    target = folder / D.out_name(job_spec["id"])
    job = {}
    while time.time() - t0 < timeout and not target.is_file():
        job = D._read_json(D._job_path(folder, job_spec["id"])) or {}
        if job.get("state") == "failed":
            break
        time.sleep(2)
    job = D._read_json(D._job_path(folder, job_spec["id"])) or job
    fbx = target
    head = fbx.read_bytes()[:18] if fbx.is_file() else b""
    magic = {"fbx": head == b"Kaydara FBX Binary", "glb": head[:4] == b"glTF",
             "blend": head[:7] == b"BLENDER" or head[:4] == bytes.fromhex("28b52ffd")}[ext]
    report = job.get("report") or {}
    return {"seconds": round(time.time() - t0, 1), "worker_online": int(online),
            "fbx_valid": int(magic), "bytes": fbx.stat().st_size if fbx.is_file() else 0,
            "verify_ok": int(bool((report.get("verify") or {}).get("ok"))),
            "takes": len(report.get("clips") or []), "worker": job.get("worker"),
            "blender": report.get("blender"), "error": str(job.get("error") or "")[:200]}


# ------------------------------------------------------------------------------------------------ live process
def classic_mirror_live(wait=12):
    """The classic mirror must follow its task: a mirror still «running» after the task is done/error, twice
    `wait` seconds apart, is stuck (66ba97ba's mirror b5c8da5e was)."""
    code = ("import json,sys;sys.path.insert(0,%r);from mt import classic_mirror as C;"
            "print(json.dumps(C.repair(False)))" % str(MT_PROD))
    def once():
        p = subprocess.run([PY, "-P", "-c", code], cwd=str(MT_PROD), capture_output=True, text=True, timeout=120,
                           env={**os.environ, "MT_ROOT": str(MT_PROD)})
        if p.returncode:
            raise RuntimeError(p.stderr[-400:])
        return {r["task"]: r for r in json.loads(p.stdout.strip().splitlines()[-1])}
    a = once()
    time.sleep(wait if a else 0)
    b = once() if a else {}
    stuck = sorted(set(a) & set(b))
    import urllib.request
    st = json.loads(urllib.request.urlopen("http://127.0.0.1:8251/api/mt/classic-mirror", timeout=20).read())
    return {"lagging_first": len(a), "stuck": len(stuck), "stuck_tasks": stuck[:10], "enabled": int(bool(st.get("enabled"))),
            "loop_error": 1 if st.get("last_error") else 0, "scans": st.get("scans")}


def rig_budget_live(hours=24, since="2026-10-10 15:50:00"):
    """Upload -> animated fast rig in the viewer within 60 s (owner: «держать скорость рига в пределах одной
    минуты»). Every classic task since the classic mirror went live (`since`) within the last `hours`: the mirror's
    own record task_agents/<task>.json `fast` (state, seconds_from_upload, the GLB it rigged). A GLB upload is rigged
    at once; FBX/OBJ wait for the converter's prepared GLB (the queue), reported apart."""
    import datetime as dt
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    lo = max((dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S"), since)
    rows = con.execute("select id from tasks where created_at >= ? and coalesce(pipeline_kind,'') != 'v3'",
                       (lo,)).fetchall()
    up, prep, missing, failed = [], [], [], []
    for (tid,) in rows:
        try:
            fast = json.loads((MT_PROD / "task_agents" / f"{tid}.json").read_text()).get("fast") or {}
        except (OSError, ValueError):
            fast = {}
        if fast.get("state") == "failed":
            failed.append(tid)
            continue
        if fast.get("state") != "done" or fast.get("seconds_from_upload") is None:
            missing.append(tid)
            continue
        (prep if str(fast.get("glb") or "").endswith("_prepared.glb") else up).append(float(fast["seconds_from_upload"]))

    def pct(xs, q):
        xs = sorted(xs)
        return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else 0.0
    return {"tasks": len(rows), "since": lo, "upload_rigged": len(up), "upload_p50_s": pct(up, .5),
            "upload_p95_s": pct(up, .95), "upload_max_s": round(max(up), 1) if up else 0.0,
            "prepared_rigged": len(prep), "prepared_p50_s": pct(prep, .5), "prepared_p95_s": pct(prep, .95),
            "fast_failed": len(failed), "fast_failed_tasks": failed[:8],
            "without_fast_rig": len(missing), "without_fast_rig_tasks": missing[:8]}


def posed_prop_fit(ctx, front=""):
    """A posed character holding a prop (66ba97ba, owner 2026-10-11 «женщина с мечем очень плохо»): the front from
    the geometry when the source skeleton is a template beside the mesh, the arms traced (no proportions guess), the
    prop a rigid child of the hand that holds it, the fit check passing."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    bones = {b["name"]: b for b in rig.get("bones") or []}
    H = float(rig.get("model_height_units") or 1.0)
    grip = []
    for b in bones.values():
        if str(b["name"]).startswith("Prop") and str(b.get("parent") or "").endswith("Hand"):
            hand = bones.get(b["parent"])
            if hand:
                grip.append(float(np.linalg.norm(np.asarray(b["head"]) - np.asarray(hand["tail"]))) / H * 100)
    pose = {}
    try:
        pose = json.loads((run / "rig" / "pose_joints.json").read_text())
    except (OSError, ValueError):
        pass
    return {"forward_axis": rig.get("forward_axis"), "front_ok": int(not front or rig.get("forward_axis") == front),
            "fit_ok": int(bool((rig.get("fit_check") or {}).get("ok"))),
            "fit_reasons": (rig.get("fit_check") or {}).get("reasons"),
            "arm_proportions": sum(1 for s_ in ("Left", "Right") for n in ("Arm", "ForeArm", "Hand")
                                   if (bones.get(f"{s_}{n}") or {}).get("source") == "proportions"),
            "pose_traced": int(bool(pose.get("use"))),
            "prop_grip_pct_H": round(min(grip), 2) if grip else 99.0}


# ------------------------------------------------------------------------------------------------ dispatch
def run_check(check: dict, ctx: dict) -> dict:
    k = check["kind"]
    a = check.get("params") or {}
    inp = check.get("input")
    on = check.get("on")                                   # an earlier step's output: "mt_rig" -> the MT run's rig
    target = None
    if on == "mt_rig":
        if "run" not in ctx:
            raise RuntimeError("needs the mt_rig step of this case")
        target = ctx["run"] / "rig" / "rigged.glb"
    elif inp:
        target = _input(inp)
    if k == "mt_rig_first":
        return mt_rig_first(ctx, inp, a.get("words", ""), a.get("forward_axis", ""))
    if k == "mt_fast_analysis":
        return mt_fast_analysis(ctx)
    if k == "hand_rig":
        return hand_rig(ctx)
    if k == "hand_detect":
        return hand_detect(ctx, inp, a.get("words", ""))
    if k == "mt_arm_clearance":
        return mt_arm_clearance(ctx)
    if k == "numeric_qa_file":                                     # QA calibration V3
        return numeric_qa_file(ctx, target, a.get("cap", 2000))
    if k == "mt_numeric_qa":
        return mt_numeric_qa(ctx, a.get("clips"), a.get("cap", 2000))
    if k == "skeleton_fit":
        return skeleton_fit(target, ctx.get("template_fingerprints", ()))
    if k == "hand_weights":
        return hand_weights(target)
    if k == "rest_drift":
        ref = _input(a["prepared"]) if a.get("prepared") else _input(check["reference"])
        return rest_drift(target, ref)
    if k == "centring":
        return centring(target, ctx["work"], a.get("res", 96))
    if k == "prop_rigidity":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        words = a.get("words", ctx.get("words", "") if on == "mt_rig" else "")
        return prop_rigidity(target, ctx["work"], words, rig_json)
    if k == "bone_outside":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else _input(a["rig_json"])
        return bone_outside(target, rig_json)
    if k == "limb_collision":
        return limb_collision(target)
    if k == "deformation_probe":
        return deformation_probe(target, a.get("clips"), a.get("samples", 12))
    if k == "ascii_fbx_intake":
        return ascii_fbx_intake(inp, ctx["work"])
    if k == "backend_unittests":
        return backend_unittests(a["modules"])
    if k == "task_page_js":
        return task_page_js(a["files"])
    if k == "v3_export_fbx":
        return v3_export_fbx(inp, a.get("timeout", 300), a.get("worker"), a.get("spec"))
    if k == "classic_mirror_live":
        return classic_mirror_live(a.get("wait", 12))
    if k == "rig_budget_live":
        return rig_budget_live(a.get("hours", 24), a.get("since", "2026-10-10 15:50:00"))
    if k == "pose_stabilization":
        return pose_stabilization(ctx)
    if k == "mt_quadruped_rig":
        return mt_quadruped_rig(ctx, inp, a.get("what", ""), a.get("forward_axis", "+z"))
    if k == "viewer_opens":
        return viewer_opens(target)
    if k == "quadruped_legs":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quadruped_legs(target, rig_json)
    if k == "rig_stretch":
        return rig_stretch(ctx)
    if k == "leg_joints":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return leg_joints(target, rig_json)
    if k == "hair_limbs":
        return hair_limbs(ctx)
    if k == "constitution":
        return constitution(ctx)
    if k == "rig_joints":
        return rig_joints(ctx)
    if k == "pending_detector":
        return {"pending": True}
    if k == "posed_prop_fit":
        return posed_prop_fit(ctx, a.get("front", ""))
    raise ValueError(f"unknown check kind {k}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", required=True, help="the case document (JSON) with only the checks to run")
    ap.add_argument("--work", required=True)
    a = ap.parse_args()
    case = json.loads(a.case)
    ctx = {"work": pathlib.Path(a.work), "template_fingerprints": case.get("template_fingerprints") or ()}
    ctx["work"].mkdir(parents=True, exist_ok=True)
    for check in case["checks"]:
        t = time.time()
        try:
            metrics = run_check(check, ctx)
            row = {"case": case["id"], "check": check["id"], "metrics": metrics}
        except Exception as exc:                                   # noqa: BLE001 - reported as ERROR, never hidden
            row = {"case": case["id"], "check": check["id"], "error": f"{type(exc).__name__}: {exc}"[:800],
                   "trace": traceback.format_exc()[-1500:]}
        row["wall_s"] = round(time.time() - t, 3)
        print(json.dumps(row, ensure_ascii=False, default=lambda o: o.tolist() if hasattr(o, "tolist") else str(o)),
              flush=True)


if __name__ == "__main__":
    main()
