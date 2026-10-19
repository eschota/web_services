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
    # a busy VPS (customer tasks first) slows every case: the kill timeout grows with the load like the budgets
    timeout = timeout * min(2.5, max(1.0, os.getloadavg()[0] / max(1.0, (os.cpu_count() or 8) / 3)))
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
    # Rig classes · V3: the end bones (Head -> HeadTop_End, Toe, Tail tips) the fit pulls into the mesh
    ends = {str(b["parent"]) for b in doc.get("bones") or [] if str(b.get("name", "")).endswith("_End") and b.get("parent")}
    return {"samples": rep["samples"], "outside_samples": rep["outside_samples"],
            "head_outside_samples": int(rep["bones"].get("Head", {}).get("outside") or 0),
            "neck_head_outside_samples": int(sum(v["outside"] for b, v in rep["bones"].items()
                                                 if b in ("Neck", "Head") or b.startswith("Trunk"))),
            "tail_outside_samples": int(sum(v["outside"] for b, v in rep["bones"].items() if b.startswith("Tail"))),
            "trunk_bones": int(sum(1 for b in rep["bones"] if b.startswith("Trunk"))),
            "leg_outside_samples": int(sum(v["outside"] for b, v in rep["bones"].items()
                                           if re.search(r"(UpLeg|Leg|Foot|Toe)", b))),
            "end_outside_samples": int(sum(v["outside"] for b, v in rep["bones"].items() if b in ends)),
            "outside_fraction": rep["outside_fraction"],
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


def quad_leg_trunk(path, rig_json, tube_radii=1.5):
    """Fable · monsters/animals (2026-10-13): a quadruped leg's shin and paw belong to that leg. Per leg (Left/Right x
    Front/Hind with Leg / Foot / Toe): the vertices below the knee joint inside the shin tube (`tube_radii` x the
    median distance of the leg's own lower vertices from the Leg -> Foot -> Toe chain, + 2 % H) whose dominant bone
    is not this leg's. -> {core_other_pct_max, core_other_vertices, legs {key: {core, other, bones}}}."""
    from mt import joint_views as JV
    doc = json.loads(pathlib.Path(rig_json).read_text(encoding="utf-8"))
    m = JV.Model(pathlib.Path(path), doc)
    P, H = m.P, m.H
    dom = np.asarray(m.dom, object).astype(str)

    def seg(a, b):
        ab = b - a
        t = np.clip(((P - a) @ ab) / max(float(ab @ ab), 1e-12), 0.0, 1.0)
        return np.linalg.norm(P - (a + t[:, None] * ab), axis=1)

    keys = [f"{sd}{lg}" for sd in ("Left", "Right") for lg in ("Front", "Hind")
            if all(f"{sd}{lg}{j}" in m.head for j in ("Leg", "Foot", "Toe"))]
    tubes, knees = {}, {}
    for key in keys:
        names = [f"{key}{j}" for j in ("Leg", "Foot", "Toe")]
        chain = [np.asarray(m.head[n], float) for n in names] + [np.asarray(m.tail[names[-1]], float)]
        tube = np.full(len(P), np.inf)
        for a, b in zip(chain[:-1], chain[1:]):
            tube = np.minimum(tube, seg(a, b))
        tubes[key], knees[key] = tube, float(m.head[names[0]][1])
    nearest = np.array(keys, object)[np.stack([tubes[k] for k in keys], 1).argmin(1)] if keys else None
    legs, worst, total, out = {}, 0.0, 0, {}
    for key in keys:
        tube = tubes[key]
        own_low = np.array([d.startswith(key) and not d.endswith("UpLeg") for d in dom])
        r0 = float(np.median(tube[own_low])) if own_low.sum() >= 10 else 0.03 * H
        core = (P[:, 1] < knees[key]) & (tube <= tube_radii * r0 + 0.01 * H) & (nearest == key)
        other = core & ~np.array([d.startswith(key) for d in dom])
        n_core, n_other = int(core.sum()), int(other.sum())
        pct = round(100.0 * n_other / max(n_core, 1), 2)
        u, c = np.unique(dom[other], return_counts=True) if n_other else ([], [])
        legs[key] = {"core": n_core, "other": n_other, "pct": pct,
                     "bones": {str(k): int(v) for k, v in sorted(zip(u, c), key=lambda kv: -kv[1])[:4]}}
        out[f"pct_{key}"] = pct
        worst, total = max(worst, pct if n_core >= 20 else 0.0), total + n_other
    return {"legs_checked": len(legs), "core_other_pct_max": worst, "core_other_vertices": total, "legs": legs,
            "vertices": int(len(P)), **out}


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


def cloth_report(rig_json):
    """«Skinning · V3» cloth mode (owner 2026-10-11): rig.json checks.cloth - the garment layers found and the vertices
    that took their weights from the body under them."""
    c = (json.loads(pathlib.Path(rig_json).read_text()).get("checks") or {}).get("cloth") or {}
    return {"present": int(bool(c)), "mode": c.get("mode"), "why": c.get("why"), "layers_n": len(c.get("layers") or []),
            "layer_verts": sum(c.get("layers") or []), "transferred": c.get("transferred") or 0, "fused": int(bool(c.get("fused")))}


def cloth_nochange(ctx):
    """«не давать общей регрессии»: the same rig built twice with the same pinned joints, cloth_mode auto and off,
    must carry the same weights on a plain body: changed vertices (> 1e-3) and the largest weight difference."""
    import tempfile
    from mt import fastrig as F, skin_tools as ST
    run = ctx["run"]
    if "cloth_mode" not in ST.SPEC:
        return {"comparable": 0, "pending": {"reason": "skin_tools.SPEC has no cloth_mode: the cloth mode is not installed on this tree", "owner": "Skinning · V3"}}
    doc = json.loads((run / "rig" / "rig.json").read_text())
    pinned = {b["name"]: b["head"] for b in doc["bones"] if not b["name"].startswith(("Prop", "Hair_")) and "Heel" not in b["name"]}
    rigs = {}
    for mode in ("auto", "off"):
        with tempfile.TemporaryDirectory(prefix=f"nochange-{mode}-", dir=str(ctx["work"])) as tmp:
            F.build(str(run), tmp, bool(doc.get("labels_used")), doc.get("body_plan"), None, {"joints": pinned},
                    skin_options={"cloth_mode": mode})
            rigs[mode] = ST.Rig((pathlib.Path(tmp) / "rigged.glb").read_bytes())
    a, b = rigs["auto"], rigs["off"]
    if a.g.n != b.g.n:
        return {"comparable": 0, "changed_vertices": -1, "max_diff": -1.0}
    Wb = np.zeros_like(a.Wg)
    for j, nm in enumerate(b.names):
        if nm in a.index:
            Wb[:, a.index[nm]] = b.Wg[:, j]
    d = np.abs(a.Wg - Wb).max(1)
    return {"comparable": 1, "changed_vertices": int((d > 1e-3).sum()), "max_diff": round(float(d.max()), 4),
            "cloth": ((doc.get("checks") or {}).get("cloth") or {}).get("why")}


def weight_bleed(rig_json):
    """«Skinning · V3» (owner 2026-10-11, «автотесты после скининга»): rig.json checks.weight_bleed from fastrig's
    geodesic assignment - the share of vertices whose top bone belongs to a body part the mesh surface does not
    connect it to (hand | leg, arm | arm, leg | leg, coil | coil), and the cross-part pairs."""
    rig = json.loads(pathlib.Path(rig_json).read_text())
    c = rig.get("checks") or {}
    wb = c.get("weight_bleed") or {}
    ga = c.get("geodesic_assign") or {}
    return {"disagree_pct": wb.get("disagree_pct"), "cross_part": wb.get("cross_part"), "pairs": wb.get("pairs"),
            "ok": int(bool(wb.get("ok"))) if wb else None, "present": int(bool(wb)),
            "geodesic_changed": ga.get("changed"), "geodesic_unreached": ga.get("unreached")}


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


def rig_stretch_file(rig_json):
    """A recorded rig.json's rig_check stretch (detector role: the known-bad rig as it was)."""
    c = json.loads(pathlib.Path(rig_json).read_text()).get("checks") or {}
    st = {k: v for k, v in (c.get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    return {"sum_edges_over_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
            "sum_edges_over_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "worst_pairs": (c.get("rig_check_stretch") or {}).get("worst")}


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
    try:                                                 # Joints · biped (owner 2026-10-15..17, spec
        out.update(limb_shares(heads, H, y0))            # biped-joints-owner-remarks-20261018): hip / knee / ankle
    except Exception as exc:                             # heights, the knee and elbow shares of their chains
        out["limb_shares_error"] = f"{type(exc).__name__}: {exc}"[:160]
    try:                                                 # Spine order · V3 (2026-10-16, 77cc1b88 the bearded Santa:
        out.update(spine_order(run, rig, heads, H))      # Neck at 41 % H under the Hips, shoulders 5.4 % H apart)
    except Exception as exc:                             # noqa: BLE001 - the other numbers stand
        out["spine_order_error"] = f"{type(exc).__name__}: {exc}"[:160]
    return out


def limb_shares(heads, H, y0):
    """Joints · biped: UpLeg / Leg / Foot heights (% H over the lowest joint, both sides' mean), the thigh's share
    of hip -> ankle (table 0.50) and the upper arm's share of shoulder -> wrist (table 0.56), the shortest forearm."""
    out = {}
    pct = lambda n: float(heads[n][1] - y0) / H * 100
    for k, name in (("UpLeg", "upleg_pct_H"), ("Leg", "knee_pct_H"), ("Foot", "ankle_pct_H")):
        v = [pct(s + k) for s in ("Left", "Right") if s + k in heads]
        if v:
            out[name] = round(sum(v) / len(v), 2)
    ln = lambda a, b: float(np.linalg.norm(np.asarray(heads[a]) - np.asarray(heads[b])))
    ks, es, fa = [], [], []
    for s in ("Left", "Right"):
        if all(s + n in heads for n in ("UpLeg", "Leg", "Foot")):
            t, sh = ln(s + "UpLeg", s + "Leg"), ln(s + "Leg", s + "Foot")
            ks.append(t / max(t + sh, 1e-12))
        if all(s + n in heads for n in ("Arm", "ForeArm", "Hand")):
            u, f = ln(s + "Arm", s + "ForeArm"), ln(s + "ForeArm", s + "Hand")
            es.append(u / max(u + f, 1e-12))
            fa.append(f / H * 100)
    if ks:
        out["knee_share_min"], out["knee_share_max"] = round(min(ks), 3), round(max(ks), 3)
    if es:
        out["elbow_share_min"], out["elbow_share_max"] = round(min(es), 3), round(max(es), 3)
        out["forearm_pct_H_min"] = round(min(fa), 2)
    return out


def spine_order(run, rig, heads, H):
    """The spine chain along the up axis (Hips < Spine < Spine1 < Spine2 < Neck < Head < HeadTop_End), heights as %
    of H over the ground (the rest mesh's lowest point, from the GLB's POSITION accessors), the Arm joints' mirror
    error about the Hips' mid-plane, head_to_height = |HeadTop_End - Head| / H (fast analysis' joint rule)."""
    import struct
    ground = None
    try:
        data = (run / "rig" / "rigged.glb").read_bytes()
        n = struct.unpack_from("<I", data, 12)[0]
        js = json.loads(data[20:20 + n])
        ys = [float(js["accessors"][p["attributes"]["POSITION"]]["min"][1]) for m in js.get("meshes") or []
              for p in m.get("primitives") or [] if "POSITION" in (p.get("attributes") or {})
              and "min" in js["accessors"][p["attributes"]["POSITION"]]]
        ground = min(ys) if ys else None
    except Exception:                                    # noqa: BLE001
        ground = None
    if ground is None:
        ground = min(float(h[1]) for h in heads.values())
    chain = [n for n in ("Hips", "Spine", "Spine1", "Spine2", "Neck", "Head", "HeadTop_End") if n in heads]
    ys = [float(heads[n][1]) for n in chain]
    out = {"spine_order_ok": 1 if len(chain) == 7 and all(a_ < b_ for a_, b_ in zip(ys, ys[1:])) else 0}
    for n in ("Neck", "Head", "Hips"):
        if n in heads:
            out[f"{n.lower()}_over_ground_pct_H"] = round((float(heads[n][1]) - ground) / H * 100, 2)
    if "Neck" in heads and "Hips" in heads:
        out["neck_minus_hips_pct_H"] = round(float(heads["Neck"][1] - heads["Hips"][1]) / H * 100, 2)
    if "HeadTop_End" in heads and "Head" in heads:
        out["head_to_height"] = round(float(np.linalg.norm(heads["HeadTop_End"] - heads["Head"])) / H, 3)
    if all(k in heads for k in ("LeftArm", "RightArm", "Hips")):
        x0 = float(heads["Hips"][0])
        mir = heads["RightArm"] * np.array([-1.0, 1.0, 1.0]) + np.array([2 * x0, 0.0, 0.0])
        out["shoulder_mirror_pct_H"] = round(float(np.linalg.norm(heads["LeftArm"] - mir)) / H * 100, 2)
        out["shoulder_dy_pct_H"] = round(abs(float(heads["LeftArm"][1] - heads["RightArm"][1])) / H * 100, 2)
    if "Hips" in heads:                                  # Seams · V3 (77cc1b88): every mirrored pair, the joint gate's
        x0 = float(heads["Hips"][0])                     # rule (fast_analysis symmetry_check), *_End included
        per = {}
        for n_ in heads:
            if n_.startswith("Left") and "Right" + n_[4:] in heads:
                a_, b_ = heads[n_], heads["Right" + n_[4:]]
                per[n_[4:]] = round(float(np.linalg.norm([(a_[0] - x0) + (b_[0] - x0), a_[1] - b_[1], a_[2] - b_[2]]))
                                    / H * 100, 2)
        if per:
            out["pair_mirror_max_pct_H"] = max(per.values())
            out["pair_mirror_worst"] = max(per, key=per.get)
        if "Toe_End" in per:
            out["toe_end_mirror_pct_H"] = per["Toe_End"]
    return out


def autotex_bake(ctx, rig_input, paints):
    """Texturing · V3 (2026-10-11): the recorded Qwen paints baked onto the untextured rig without the farm."""
    from mt import autotex as A
    w = pathlib.Path(ctx["work"]) / "autotex"
    w.mkdir(parents=True, exist_ok=True)
    for dst, name in paints.items():
        shutil.copyfile(_input(name), w / dst)
    pj = json.loads((w / "paint.json").read_text())
    data = _input(rig_input).read_bytes()
    opts = A.check_options({"view_px": pj["view_px"]})
    t = time.time()
    new, rep = A.bake_glb(data, w, opts, pj["forward"], "autotest")
    took = round(time.time() - t, 1)
    fa, fb = A.glb_texture_facts(new), A.glb_texture_facts(data)
    before, after = A.renders(data, pj["forward"], 256), A.renders(new, pj["forward"], 256)
    bm = np.abs(before.astype(np.int16) - 235).max(-1) > 4
    am = np.abs(after.astype(np.int16) - 235).max(-1) > 4
    mats = rep.get("materials") or [{}]
    return {"textured_before": fb["textured_materials"], "textured_after": fa["textured_materials"],
            "images_after": fa["images"], "normals_before_broken": int(rep["normals"]["before"] == "broken"),
            "normals_fixed": rep["normals"]["fixed_vertices"], "uv_area": (rep.get("auto_uv") or {}).get("uv_area", 0),
            "direct_share": mats[0].get("direct_share", 0), "align_iou_min": min(v["after"] for v in
                                                                                rep["align_iou"].values()),
            "colour_std_gain": round(A._rel_lum_std(after / 255.0, am) - A._rel_lum_std(before / 255.0, bm), 4),
            "seconds": took}


def autotex_upload_refs(ctx, upload_input, name):
    """An upload alone in its folder (as the visitor sent it): the files it names that are missing."""
    from mt import autotex as A
    d = pathlib.Path(ctx["work"]) / "upload_alone"
    d.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_input(upload_input), d / name)
    r = A.upload_refs(d / name)
    return {"referenced": len(r.get("referenced") or []), "missing": len(r.get("missing") or []),
            "missing_names": ",".join(r.get("missing") or [])}


def anthro_rig(ctx):
    """Anthro rig · V3 (2026-10-15, button «Новая категория», task 51335965): an anthropomorphic animal is a biped with
    a tail and ears of its own. What the rig carries (rig.json "anthro" + the bones): the verdict, the category the
    registry gives (mt/fast_analysis.categorize), the tail and ear chains, the tail's length and thickness."""
    try:
        from mt import anthro as _AN  # noqa: F401
    except ImportError:
        return {"pending": True, "why": "mt/anthro.py is not in the tree under test"}
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    an = rig.get("anthro") or {}
    names = [b.get("name") or "" for b in rig.get("bones") or []]
    tail = an.get("tail") or {}
    out = {"is_anthro": 1 if an.get("use") else 0, "subcategory": an.get("subcategory"), "category": an.get("category"),
           "category_anthro": 1 if an.get("category") == "anthro" else 0,
           "tail_bones": sum(1 for n in names if n.startswith("Tail")),
           "ear_bones": sum(1 for n in names if "Crest" in n),
           "tail_length_pct_H": tail.get("length_pct_H"), "tail_radius_pct_H": tail.get("radius_pct_H"),
           "bones": len(names), "body_plan": rig.get("body_plan")}
    return out


def biped_tail(ctx, clips=("Idle", "Walking", "Running")):
    """Biped tail · V3.1 (owner 2026-10-18, the monkey 8d2578e3): a biped whose analysis found a tail gets the tail
    chain in its primary rig (mt/biped_tail.py). What the rig carries: the Tail bones under the Hips, the record, and the
    tail region's numbers on the library clips (mt.biped_tail.region_metrics: the tail's vertices + their seam ring, the
    secondary rig's own QA)."""
    try:
        from mt import biped_tail as _BT
    except ImportError:
        return {"pending": True, "why": "mt/biped_tail.py is not in the tree under test"}
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    bones = rig.get("bones") or []
    tails = [b for b in bones if re.fullmatch(r"Tail\d*", str(b.get("name") or ""))]
    root = next((b for b in tails if b.get("name") == "Tail"), None)
    bt = rig.get("biped_tail") or {}
    m = _BT.region_metrics(run, clips=clips)
    # a moving limb (a foot kicking back in Running, a hand) passing through the hanging tail is not the tail's skin:
    # the primary chain is static, the secondary layer's collision is what moves it; counted apart from the trunk
    limb = re.compile(r"(Left|Right)(Foot|ToeBase|Leg|ForeArm|Hand)$")
    pen_body = pen_limb = 0
    for r_ in (m.get("clips") or {}).values():
        for bone, n in (r_.get("pen_by_bone") or {}).items():
            if limb.search(str(bone)):
                pen_limb += int(n)
            else:
                pen_body += int(n)
    if m.get("pen_free") and not any("pen_by_bone" in r_ for r_ in (m.get("clips") or {}).values()):
        pen_body = m.get("pen_free")
    return {"tail_bones": len(tails), "tail_parent_hips": 1 if root and root.get("parent") == "Hips" else 0,
            "used": 1 if bt.get("used") else 0, "why": bt.get("why"), "profile": bt.get("profile"),
            "tail_vertices": m.get("vertices"), "tail_max_stretch": m.get("max_stretch"),
            "tail_edges_over_4x": m.get("edges_over_4x"), "tail_pen_free": m.get("pen_free"),
            "tail_pen_total": m.get("pen_total"), "body_max_stretch": m.get("body_max_stretch"),
            "tail_pen_body": pen_body, "tail_pen_limb_strike": pen_limb,
            "stage_seconds": (rig.get("timings_s") or {}).get("biped_tail"),
            "clips": {c: {k: v for k, v in r.items() if k in ("max_stretch", "edges_over_4x", "pen_free", "pen_root_band",
                                                                    "pen_depth_pct_H", "pen_by_bone")}
                      for c, r in (m.get("clips") or {}).items()}, "metrics_why": m.get("why")}


def anthro_words(cases):
    """Anthro rig · V3: the words rule on a list of Vision `what` strings: {what: expected} -> mismatches (0 = all right)."""
    try:
        from mt import anthro as AN
    except ImportError:
        return {"pending": True, "why": "mt/anthro.py is not in the tree under test"}
    bad = []
    for what, want in (cases or {}).items():
        got = bool(AN.verdict(what, "biped")["anthro"])
        if got != bool(want):
            bad.append(what)
    return {"mismatches": len(bad), "wrong": bad[:8]}


def arm_fit(ctx):
    """Task 432f7bf3 (skirt flaps taken for arms) and limb_in_body.left (one arm fitted into the torso): where the
    arm joints ended up, in % of the height, and how the two sides compare."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    bones = {b["name"]: b for b in rig.get("bones") or []}
    heads = {n: np.asarray(b["head"], float) for n, b in bones.items()}
    ys = np.array([h[1] for h in heads.values()])
    H = float(rig.get("model_height_units") or 0) or float(np.ptp(ys)) or 1.0
    y0 = float(ys.min())
    x0 = float(heads["Hips"][0]) if "Hips" in heads else 0.0
    out = {"height_units": round(H, 4)}
    arm_y, hand_lat, elbow_above, srcs = [], [], 0, []
    for side in ("Left", "Right"):
        a, e, w = heads.get(f"{side}Arm"), heads.get(f"{side}ForeArm"), heads.get(f"{side}Hand")
        if a is None or e is None or w is None:
            continue
        arm_y.append((a[1] - y0) / H * 100)
        hand_lat.append(abs(w[0] - x0) / H * 100)
        elbow_above += int(e[1] > a[1] + 0.08 * H)
        for n in ("Arm", "ForeArm", "Hand"):
            srcs.append(str((bones.get(f"{side}{n}") or {}).get("source")))
        out[f"{side.lower()}_arm_y_pct_H"] = round((a[1] - y0) / H * 100, 1)
        out[f"{side.lower()}_hand_lateral_pct_H"] = round(abs(w[0] - x0) / H * 100, 1)
    out["arm_y_min_pct_H"] = round(min(arm_y), 1) if arm_y else 0.0
    out["arm_y_diff_pct_H"] = round(abs(arm_y[0] - arm_y[1]), 1) if len(arm_y) == 2 else 99.0
    out["hand_lateral_max_pct_H"] = round(max(hand_lat), 1) if hand_lat else 99.0
    out["elbow_above_shoulder"] = elbow_above
    out["arms_found"] = len(arm_y)
    try:                                   # Arm segments (2026-10-15, 96e7637b): clavicle / upper arm / forearm, worst side
        seg = []
        for side in ("Left", "Right"):
            sh, a, e, w = (bones.get(f"{side}Shoulder"), heads.get(f"{side}Arm"), heads.get(f"{side}ForeArm"),
                           heads.get(f"{side}Hand"))
            if sh is None or a is None or e is None or w is None:
                continue
            seg.append((float(np.linalg.norm(np.asarray(sh["tail"], float) - np.asarray(sh["head"], float))) / H,
                        float(np.linalg.norm(e - a)) / H, float(np.linalg.norm(w - e)) / H))
        if seg:
            uf = [u / max(f, 1e-6) for _c, u, f in seg]
            out["upper_to_fore"] = round(max(uf, key=lambda r: abs(np.log(max(r, 1e-6)))), 3)
            out["clavicle_to_upper"] = round(max(c / max(u, 1e-6) for c, u, _f in seg), 3)
            out["clavicle_pct_H"] = round(max(c for c, _u, _f in seg) * 100, 1)
            out["upper_pct_H"] = round(min(u for _c, u, _f in seg) * 100, 1)
            out["shoulder_x_pct_H"] = round(max(abs(float(heads[f"{sd}Arm"][0]) - x0) for sd in ("Left", "Right")
                                                if f"{sd}Arm" in heads) / H * 100, 1)
    except Exception as exc:                                   # noqa: BLE001
        out["segments_error"] = f"{type(exc).__name__}: {exc}"[:120]
    out["proportions_sources"] = sum(1 for s in srcs if s == "proportions")
    out["mirror_sources"] = sum(1 for s in srcs if s == "mirror")
    out["suspect_sources"] = sum(1 for s in srcs if s == "suspect")
    return out


def tessellation(ctx):
    """Pre-rig tessellation (mt/tessellate.py, 2026-10-11): the stage's record from rig.json and the crumpling of the
    rig on its own rig_check clip: faces whose area collapses (< 20 %) or explodes (> 5x) against the rest, edges
    over 2x / 4x, the worst of 8 frames (dd498832: 580-triangle chibi, edges up to 31 % of the height)."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    t = rig.get("tessellation") or {}
    out = {"applied": int(bool(t.get("applied"))), "why": t.get("why"), "faces_before": t.get("faces"),
           "faces_after": t.get("faces_after"), "factor": t.get("factor"),
           "edge_median_pct_H": (t.get("edge_pct_H") or {}).get("median"),
           "edge_max_pct_H_after": (t.get("edge_pct_H_after") or {}).get("max"),
           "original_topology": int((run / "rig" / "rigged_original.glb").is_file())}
    try:
        from mt import fastrig as F, limb_stabilize as LS
        data = (run / "rig" / "rigged.glb").read_bytes()
        js, b = F.read_glb(data)
        src = F.Source(data)
        r = LS.SourceRig(src, js, b)
        P0, faces = src.positions, src.faces
        H = float(np.ptp(P0[:, 1])) or 1.0
        n0 = np.cross(P0[faces[:, 1]] - P0[faces[:, 0]], P0[faces[:, 2]] - P0[faces[:, 0]])
        a0 = np.linalg.norm(n0, axis=1)
        big = a0 > 1e-6 * H * H
        e = np.unique(np.sort(np.concatenate([faces[:, [0, 1]], faces[:, [1, 2]], faces[:, [2, 0]]]), 1), axis=0)
        L0 = np.linalg.norm(P0[e[:, 0]] - P0[e[:, 1]], axis=1)
        ok = L0 > 1e-3 * H
        anim = next((a for a in js.get("animations", []) if a.get("name") == "rig_check"), None)
        worst = {"collapsed": 0, "edges_2x": 0, "edges_4x": 0}
        if anim is not None:
            tmax = max(float((js["accessors"][s["input"]].get("max") or [0])[0]) for s in anim["samplers"])
            for tt in np.linspace(0.05, 0.95, 8) * tmax:
                P = r.deform_to(r.frame_world(anim, float(tt)))
                a1 = np.linalg.norm(np.cross(P[faces[:, 1]] - P[faces[:, 0]], P[faces[:, 2]] - P[faces[:, 0]]), axis=1)
                worst["collapsed"] = max(worst["collapsed"], int((((a1 < 0.2 * a0) | (a1 > 5 * a0)) & big).sum()))
                L1 = np.linalg.norm(P[e[:, 0]] - P[e[:, 1]], axis=1)[ok]
                ratio, grow = L1 / L0[ok], L1 - L0[ok]
                worst["edges_2x"] = max(worst["edges_2x"], int(((ratio > 2) & (grow > 0.002 * H)).sum()))
                worst["edges_4x"] = max(worst["edges_4x"], int(((ratio > 4) & (grow > 0.005 * H)).sum()))
        out.update({f"clip_{k}_max": v for k, v in worst.items()})
        out["faces"] = int(len(faces))
    except Exception as exc:                                   # noqa: BLE001
        out["clip_error"] = f"{type(exc).__name__}: {exc}"[:160]
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


QUAD_FORWARD_YAW = {"+z": 0.0, "-z": 180.0, "+x": -90.0, "-x": 90.0}   # mt.fastrig.FORWARD_YAW


def multileg_legs(run, rig) -> dict:
    """Multileg · V3 (owner 2026-10-18 «сделай пауков»): the creature rig's legs per side, the mirrored pairs (creature
    pair_limbs), the dropped tips, and the orientation the rig stood on (proj/orient.json), for the spider / insect
    cases: a spider stands on its +y with 4 / 4 legs in 4 pairs."""
    cr = rig.get("creature") or {}
    if not cr:
        return {}
    names = [str(b.get("name") or "") for b in rig.get("bones") or []]
    pr = cr.get("pairs") or {}
    o = {}
    try:
        o = json.loads((pathlib.Path(run) / "proj" / "orient.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        pass
    return {"legs_left": sum(1 for n in names if re.fullmatch(r"LeftLeg\d+", n)),
            "legs_right": sum(1 for n in names if re.fullmatch(r"RightLeg\d+", n)),
            "legs_mid": sum(1 for n in names if re.fullmatch(r"Limb\d+", n)),
            "leg_pairs": int(pr.get("pairs") or 0), "expected_pairs": pr.get("expected"),
            "dropped_tips": len(pr.get("dropped") or []), "up": o.get("up"), "up_plan": o.get("plan"),
            "legs_symmetric": int(sum(1 for n in names if re.fullmatch(r"LeftLeg\d+", n))
                                  == sum(1 for n in names if re.fullmatch(r"RightLeg\d+", n)))}


def vehicle_rig(run, rig) -> dict:
    """Vehicles V3 (owner 2026-10-18 «тут две тачки залили, надо чтобы колеса риггались и все такое»; mt/vehicle_mesh.py):
    the wheels on their axles, the up the file stood on and whether it was turned, the vertices per part, the
    detector's score, for the car cases (the Camry 52e76e46: Z-up OBJ, 4 named wheels, 2 axles, front steer)."""
    v = rig.get("vehicle") or {}
    if not v and str(rig.get("body_plan") or "") != "vehicle":
        return {}
    wl = v.get("wheels") or []
    ch = rig.get("checks") or {}
    det = v.get("detect") or {}
    return {"wheels": len(wl), "axles": len(v.get("axles") or []), "steer_wheels": len(v.get("steer_wheels") or []),
            "wheel_pairs": sum(1 for a in (v.get("axles") or []) if sum(1 for w in wl if int(w.get("axle", -1)) == a) == 2),
            "vehicle_up": str(v.get("up") or ""), "vehicle_turned": int(bool(v.get("turned"))),
            "wheels_named": int(ch.get("wheels_named") or 0), "wheel_vertices": int(ch.get("wheel_vertices") or 0),
            "body_vertices": int(ch.get("body_vertices") or 0), "vehicle_score": int(det.get("score") or 0),
            "normaliser_error": 1 if (v.get("normaliser") or {}).get("error") else 0,
            "wheels_norm_s": float(((v.get("normaliser") or {}).get("timings_s") or {}).get("total") or 0),
            # the front (owner 2026-10-18: «колёса задние поворачивают»): voted by the names, then Vision; and the
            # scale from the wheels (the Ferrari 45575dfc shrank to 5 cm on the ruler judge)
            "forward_axis": str(rig.get("forward_axis") or ""),
            "front_source": str((v.get("front") or {}).get("front_source") or ""),
            "front_voted": 1 if str((v.get("front") or {}).get("front_source") or "") in ("names", "vision", "user") else 0,
            "front_review": int(bool(v.get("front_review"))),
            "scale_source": str((v.get("scale") or {}).get("chosen_source") or ""),
            "scale_height_m": float((v.get("scale") or {}).get("chosen_height_m") or 0)}



def quad_leg_frame(run, rig) -> dict:
    """Quad legs · V3 (2026-10-14): a quadruped rig's legs in its fit frame (rig.json forward_axis + proj/yaw.json, as
    mt.fastrig.build turns it: +Z the head end, +Y up, +X the model's left), from rig.json alone:
    quad_crossed_pairs  pairs whose Left knee or foot lies > 2 % H right of the Right one (0 on a right rig);
    quad_front_knees_ahead  the front knees ahead of the hind knees, share of the body length (>= 0.12 on a standing
                        animal; the short-leg column fit put all four knees mid-body: -0.56 .. +0.03);
    quad_up_biped_trace 1 when proj/orient.json took the up from the biped voxel trace (the boars / wolves stood on
                        their tails, 2026-10-14);
    quad_legs_final_fail the failed leg checks fastrig recorded (rig.json quad_legs.final_fail; -1 without the record).
    Empty for a rig without quadruped legs."""
    import math
    heads = {b["name"]: np.asarray(b["head"], float) for b in rig.get("bones") or [] if b.get("head")}
    need = [f"{s_}{l}{k}" for s_ in ("Left", "Right") for l in ("Front", "Hind") for k in ("Leg", "Foot")]
    if any(n not in heads for n in need) or "Hips" not in heads:
        return {}
    fa = str(rig.get("forward_axis") or "+z")
    yp = pathlib.Path(run) / "proj" / "yaw.json"
    yj = json.loads(yp.read_text()) if yp.is_file() else {}
    chg = (rig.get("forward_check") or {}).get("changed")
    yaw = yj.get("yaw_deg") if (yj.get("forward_axis") == fa and yj.get("yaw_deg") is not None and not chg) \
        else QUAD_FORWARD_YAW.get(fa, 0.0)
    a = math.radians(float(yaw))
    Rc = np.array([[math.cos(a), 0, math.sin(a)], [0, 1, 0], [-math.sin(a), 0, math.cos(a)]])
    J = {k: v @ Rc.T for k, v in heads.items()}
    ys = np.array([v[1] for v in J.values()])
    H = float(ys.max() - ys.min()) or 1.0
    front = max(float(J[n][2]) for n in ("Nose_End", "Head", "Spine2", "Hips") if n in J)
    back = min(float(J[n][2]) for n in ("Hips", "LeftHindFoot", "RightHindFoot", "LeftHindToe_End", "RightHindToe_End")
               if n in J)
    crossed = sum(1 for l in ("Front", "Hind")
                  if min(float(J[f"Left{l}{k}"][0] - J[f"Right{l}{k}"][0]) for k in ("Leg", "Foot")) < -0.02 * H)
    zf = (float(J["LeftFrontLeg"][2]) + float(J["RightFrontLeg"][2])) / 2
    zh = (float(J["LeftHindLeg"][2]) + float(J["RightHindLeg"][2])) / 2
    op = pathlib.Path(run) / "proj" / "orient.json"
    oj = json.loads(op.read_text()) if op.is_file() else {}
    ql = rig.get("quad_legs")
    return {"quad_crossed_pairs": crossed, "quad_front_knees_ahead": round((zf - zh) / max(front - back, 1e-9), 3),
            "quad_up_biped_trace": int(str(oj.get("plan")) == "biped" and "voxel trace" in str(oj.get("why"))),
            "quad_legs_final_fail": len(ql.get("final_fail") or []) if isinstance(ql, dict) else -1}


def voxel_legs(input_name):
    """Legless rig · V3 (2026-10-17, docs/category-legless-spec-20261017.md): mt.legless's voxel leg detector on a
    corpus input -> legs (none / two / four / many / unknown), the trail, the evidence (seconds, no rig)."""
    p, wall = _child([PY, "-P", "-m", "mt.legless", "legs", str(_input(input_name))], timeout=240)
    if p.returncode != 0:
        raise RuntimeError(f"mt.legless legs exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    d = json.loads(p.stdout)
    e = d.get("evidence") or {}
    return {"seconds": wall, "legs": d.get("legs"), "legless": d.get("legs") == "none", "trail": bool(d.get("trail")),
            "why": d.get("why"), "leg_columns": e.get("leg_columns"), "foot_sections": e.get("foot_sections"),
            "taper_ratio": e.get("taper_ratio"), "floor_contact": e.get("floor_contact"), "upright": e.get("upright")}


def mt_rig_first(ctx, input_name, words="", forward_axis="", v3_bound=False, body="", analysis=None):
    """The rig the viewer gets first (classic mirror + V3 conveyor): python -m mt.rig_first on the upload, like
    mt/classic_mirror.py does at upload time. forward_axis: the front the conveyor's projections / Vision know
    (proj/forward.json), for a model whose symmetry alone cannot tell it (the potato 583622f3: guess 0.32)."""
    run = pathlib.Path(ctx["work"]) / "run"
    shutil.rmtree(run, ignore_errors=True)
    run.mkdir(parents=True)
    if forward_axis:                                     # as the conveyor's projections record it (rig_first then
        (run / "proj").mkdir(parents=True, exist_ok=True)   # skips its own guess)
        (run / "proj" / "manifest.json").write_text(json.dumps({"forward_axis": forward_axis, "source": "autotests"}))
    if body:                                             # Rig classes · V3: Vision's body field, as the conveyor's
        (run / "phases.json").write_text(json.dumps({"phases": [{"id": "classify", "result": {   # classify phase
            "what": words, "body": body, "props": ""}}]}))                                     # records it
    for dest, name in (analysis or {}).items():           # Biped tail · V3.1: the analyser's files of the run
        (run / "analysis").mkdir(parents=True, exist_ok=True)   # (parts_seg.json / .npz: labels on proj/model.glb, which
        shutil.copyfile(_input(name), run / "analysis" / dest)   # is the corpus input itself), as the conveyor has them
    if v3_bound:                                         # Multi-limb rig · V3: like a hash-bound V3 source, the up
        (run / "v3").mkdir(parents=True, exist_ok=True)     # stage of rig_first leaves the model as uploaded
        (run / "v3" / "source.json").write_text(json.dumps({"schema": "autorig.v3.session-source/1", "source": "autotests"}))
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
            # Voxel parts · V3 (2026-10-14): Prop bones on a root of their own (a base, a floating object), the
            # stage's decisions as fastrig used them (rig.json voxel_parts), the weld-split vertex copies
            "prop_bones_root": sum(1 for b in rig.get("bones", []) if str(b.get("name", "")).startswith("Prop")
                                   and not b.get("parent")),
            "voxel_props": len((rig.get("voxel_parts") or {}).get("props") or []),
            "voxel_held_props": sum(1 for h in (rig.get("voxel_parts") or {}).get("props") or []
                                    if h == "voxel_parts:held_prop"),
            "voxel_floating": sum(1 for h in (rig.get("voxel_parts") or {}).get("props") or []
                                  if h == "voxel_parts:floating_object"),
            "voxel_held_found": int(((rig.get("voxel_parts") or {}).get("counts") or {}).get("held_prop") or 0),
            "split_duplicated": int((rig.get("checks") or {}).get("hair_arm_split_vertices_duplicated") or 0),
            # V3 triage rig_stretch: the rig's own rig_check, edges over 2x / 4x summed over its check times
            "rig_check_2x": sum(int(v.get("edges_over_2x") or 0) for k, v in
                                ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items()
                                if str(k).startswith("t=") and isinstance(v, dict)),
            "rig_check_4x": sum(int(v.get("edges_over_4x") or 0) for k, v in
                                ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items()
                                if str(k).startswith("t=") and isinstance(v, dict)),
            **quad_leg_frame(run, rig), **multileg_legs(run, rig), **vehicle_rig(run, rig)}   # Quad legs · V3 (2026-10-14); Multileg · V3; Vehicles V3 (2026-10-18)


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


def serpent_rig(ctx, head_near=None):
    """Serpent rig · V3 (2026-10-11, the worm aaac7892): the serpent branch of rig_first on this case's MT run:
    body plan, bones, the head end against a known point (distance / arc length), weights jumping between coils,
    bones outside the mesh, the self-intersection gate (rig.json of mt/serpent.py)."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    sp = rig.get("serpent") or {}
    ch = rig.get("checks") or {}
    bones = {b["name"]: b for b in rig.get("bones") or []}
    head_dist = None
    if head_near is not None and "Head" in bones and sp.get("arc"):
        import math
        tip = bones["Head"]["tail"]
        head_dist = round(math.dist(tip, head_near) / float(sp["arc"]), 4)
    st = {k: v for k, v in (ch.get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    return {"body_plan": rig.get("body_plan"), "bones": len(bones), "spine_bones": sp.get("spine_bones"),
            "jaw": bool(sp.get("jaw")), "head_dist": head_dist, "elongation": sp.get("elongation"),
            "gate_passed": bool(ch.get("gate_passed")), "clips_dropped": len(ch.get("clips_dropped") or []),
            "arc_jump_edges": ch.get("arc_jump_edges"), "bones_outside": len(ch.get("bones_outside_mesh") or []),
            "unweighted": ch.get("unweighted_vertices"),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
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


def mt_fast_analysis(ctx, apply=False):
    """The V3 fast analysis stage (mt.fast_analysis: bone_check, heel rules, symmetry, proportions, parts). apply=True
    runs it as the conveyor does (--apply: the fast fix rebuilds and keeps the best candidate, Joint centring · V3);
    applied_after = the bone samples outside the rig that ends up installed."""
    run = ctx["run"]
    p, wall = _child([PY, "-P", "-m", "mt.fast_analysis", "--dir", str(run)] + (["--apply"] if apply else []), timeout=600)
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
            "ops_total_s": (fa.get("timings") or {}).get("total"),
            **({"applied": int(bool((fa.get("applied") or {}).get("applied"))),
                "applied_after": ((fa.get("applied") or {}).get("after") if (fa.get("applied") or {}).get("applied")
                                  else bo.get("outside_before")),
                "applied_candidate": (fa.get("applied") or {}).get("candidate"),
                "joint_centring_proposal": (bo.get("joint_centring") or {}).get("official_after")} if apply else {})}


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
    names = set()                                          # Voxel parts · V3: the FBX's model names (a held prop's
    if ext == "fbx" and fbx.is_file():                     # mesh and its Prop bone must survive the export)
        names = {m.group(1).decode("latin1") for m in re.finditer(rb"([A-Za-z0-9_:.\- ]{1,60})\x00\x01Model",
                                                                    fbx.read_bytes())}
    return {"skinned_meshes": int((report.get("verify") or {}).get("skinned_meshes") or 0),
            "prop_bones_in_fbx": sum(1 for n in names if re.match(r"(mixamorig:)?Prop\d*$", n)),
            "model_names": sorted(names)[:60],
            "seconds": round(time.time() - t0, 1), "worker_online": int(online),
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


def multilimb_rig(ctx):
    """«Multi-limb rig · V3» (mt/multilimb.py, 2026-10-11, task 0d39aaba: four arms and wings): the limbs the trace
    found on this case's MT run (arms, arm pairs, wings, tail), the extra chains fastrig added, their vertices, the
    cross-chain weight (0 by construction outside the seams), the library clips' channels on the extra bones and the
    rig_check stretch. On a plain biped (the control) every extra count is 0."""
    from mt import fastrig as F
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    ml = rig.get("multilimb") or {}
    counts = ml.get("counts") or {}
    mls = (rig.get("checks") or {}).get("multilimb") or {}
    chains = mls.get("chains") or []
    st = {k: v for k, v in ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    names = {b["name"] for b in rig.get("bones") or []}
    extra = sorted(n for n in names if re.fullmatch(r"(Left|Right)(Shoulder|Arm|ForeArm|Hand)\d+|(Left|Right)Wing[A-Z]?\d*|Tail\d*", n))
    js, _ = F.read_glb((run / "rig" / "rigged.glb").read_bytes())
    node = {i: n.get("name") for i, n in enumerate(js.get("nodes") or [])}
    clip_extra = 0
    for a in js.get("animations") or []:
        if a.get("name") == "rig_check":
            continue
        clip_extra += sum(1 for c in a.get("channels") or [] if node.get(c["target"]["node"]) in extra)
    try:                                                 # Classification first (2026-10-18, 98d0fbca): the class the
        cj = json.loads((run / "analysis" / "class.json").read_text())   # three voters wrote (category, arm verdict)
    except (OSError, ValueError):
        cj = {}
    lim = cj.get("limbs") or {}
    return {"arms": counts.get("arms", 0), "arm_pairs": counts.get("arm_pairs", 0), "wings": counts.get("wings", 0),
            "legs": counts.get("legs", 0), "class_category": cj.get("category"), "class_verdict": lim.get("verdict"),
            "class_multi_votes": int((lim.get("votes") or {}).get("multi") or 0),
            "class_needs_review": int(bool(lim.get("needs_review"))),
            "wing_pairs": counts.get("wing_pairs", 0), "tails": counts.get("tails", 0), "crests": counts.get("crests", 0),
            "use": int(bool(ml.get("use"))), "extra_chains": len(chains), "extra_bones": len(extra),
            "chains_without_vertices": sum(1 for c in chains if not c.get("vertices")),
            "chain_vertices": int(mls.get("vertices") or 0),
            "cross_chain_weight_after": float(mls.get("cross_chain_weight_after") or 0.0),
            "seam_edges": int(mls.get("seam_edges") or 0), "seam_split_faces": int(mls.get("seam_split_faces") or 0),
            "clip_channels_extra": clip_extra, "clips": len(rig.get("clips") or []),
            "unweighted": (rig.get("checks") or {}).get("unweighted_vertices"),
            "joints_outside": len((rig.get("checks") or {}).get("joints_outside_mesh") or []),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "stretch_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
            "detect_s": ml.get("seconds"), "rig_total_s": (rig.get("timings_s") or {}).get("total"),
            **_wing_arm_metrics(run)}


def _wing_arm_metrics(run):
    """Wings V3 (2026-10-15): the arm chain of the finished rig against the wings multilimb found (mt.multilimb.
    arm_wing_overlap): arm_joints_on_wing (a joint inside a wing's voxel region = the Arm chain runs along a wing),
    hand_reach_pct_wing_reach (the hand tip's reach over the wing tip's, in %), wing_vertices_excluded (the wing mesh
    fastrig's arm fit left out), wing_bones, and the probe's own numbers."""
    try:
        from mt import multilimb as ML
        r = ML.arm_wing_overlap(run)
        rig = json.loads((run / "rig" / "rig.json").read_text())
        ml = json.loads((run / "rig" / "multilimb.json").read_text()) if (run / "rig" / "multilimb.json").is_file() else {}
        pr = ml.get("wing_probe") or {}
        return {"arm_joints_on_wing": r.get("on_wing"), "hand_reach_pct_wing_reach": r.get("hand_reach_pct_wing_reach"),
                "wing_vertices_excluded": int(((rig.get("arm_chain") or {}).get("wing_vertices_excluded")) or 0),
                "wing_bones": sum(1 for b in rig.get("bones") or [] if "Wing" in str(b.get("name"))),
                "wing_probe_act": int(bool(pr.get("act"))), "wing_probe_pair": pr.get("pair")}
    except Exception as exc:                             # noqa: BLE001 - a metric never costs the case
        return {"arm_joints_on_wing": None, "wing_arm_error": f"{type(exc).__name__}: {exc}"[:120]}


def rig_upright(ctx):
    """Rig path V3 (owner bug f7e2a3f1, a pre-rigged bear lying face down): the rig stands up. Head above the hips,
    the feet below them (by at least 20 % of the height for a biped), and the rig's height is the drawn model's height
    (a skinned upload is drawn in its skinned rest pose, not by the mesh node)."""
    rig = json.loads((ctx["run"] / "rig" / "rig.json").read_text())
    H = float(rig.get("model_height_units") or 0) or 1e-9
    heads = {b["name"]: b["head"] for b in rig.get("bones") or []}
    out = {"body_plan": rig.get("body_plan"), "height_units": round(H, 5)}
    if "Hips" in heads and "Head" in heads:
        out["head_above_hips_pct_H"] = round((heads["Head"][1] - heads["Hips"][1]) / H * 100, 1)
    feet = [heads[n][1] for n in ("LeftFoot", "RightFoot") if n in heads]
    if feet and "Hips" in heads:
        out["hips_above_feet_pct_H"] = round((heads["Hips"][1] - max(feet)) / H * 100, 1)
    oj = ctx["run"] / "proj" / "orient.json"
    out["up"] = json.loads(oj.read_text()).get("up") if oj.is_file() else None
    fc = rig.get("fit_check") or {}                      # Joints · biped / orientation (2026-10-18): an unsure up goes
    out["fit_check_ok"] = int(bool(fc.get("ok")))        # to review, the rig's front is the one forward.json record
    out["orientation_review"] = int(bool(fc.get("orientation_review")))
    fj = ctx["run"] / "proj" / "forward.json"
    fw = json.loads(fj.read_text()).get("forward_axis") if fj.is_file() else None
    out["forward_axis"] = rig.get("forward_axis")
    out["forward_record_match"] = int(fw is None or fw == rig.get("forward_axis"))
    bake = ctx["run"] / "proj" / "skin_bake.json"
    out["skin_baked"] = int(bool(json.loads(bake.read_text()).get("applied"))) if bake.is_file() else 0
    return out


def seam_weights(ctx):
    """Seams · V3 (2026-10-14, docs/seams-spec-20261014.md): one weight per coincident copy, no open slit.
    seam_weight_mismatch: the engine's own count in rig.json (coincident written pairs whose weights differ and that
    are not its split copies of one welded vertex; -1 = the rig predates the check); open_cut_edges: recounted on the
    final GLB (mt/cut_cap.open_cut_edges: boundary edges whose both ends are copies with other weights = holes);
    diverging_copies: every copy whose weights differ from another copy of its position."""
    from mt import cut_cap as CC, seam_policy as SP
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    c = rig.get("checks") or {}
    data = (run / "rig" / "rigged.glb").read_bytes()
    js, binary, prims = SP._parse(data)
    div = 0
    if prims:
        import numpy as np
        names, _parts = SP._part_names(js)
        nb = max(len(names), int(max(r["J"].max(initial=0) for r in prims)) + 1)
        P = np.concatenate([r["P"] for r in prims])
        J = np.concatenate([r["J"] for r in prims])
        W = np.concatenate([r["W"] for r in prims])
        div = int(SP._diverging(P, J, W, nb)[1].sum())
    sp = rig.get("seam_policy") or {}
    m = c.get("seam_weight_mismatch")
    return {"seam_weight_mismatch": -1 if m is None else int(m), "open_cut_edges": int(CC.open_cut_edges(data)),
            "diverging_copies": div, "engine_open_hole_copies": c.get("seam_open_hole_copies"),
            "policy": sp.get("policy"), "candidates": sp.get("candidates_n"), "kept": sp.get("kept"),
            "welded": sp.get("welded"), "policy_seconds": sp.get("seconds")}


def dup_gaps(glb_path, frames=12):
    """Rig path V3 (owner #1935, the boar falling apart): vertices that share a position (UV seams, a triangle soup)
    must move together. -> {diverging_dups: vertices whose weights differ from their position's mean (L1 > 0.05),
    gap_max_pct_H: the largest distance such a vertex is pulled from its position's mean over the clips}."""
    import numpy as np
    from mt import rig_judge as RJ, fastrig as F
    data = pathlib.Path(glb_path).read_bytes()
    z = RJ.Posable(data)
    P = z.raw
    nb = len(z.names)
    W = np.zeros((len(P), nb), np.float32)
    for c in range(4):
        np.add.at(W, (np.arange(len(P)), z.jo[:, c]), z.we[:, c].astype(np.float32))
    ids, n = F.weld(P)
    cnt = np.bincount(ids)
    mean = np.zeros((n, nb), np.float32)
    np.add.at(mean, ids, W)
    mean /= cnt[:, None]
    div = np.abs(W - mean[ids]).sum(1) > 0.05
    H = float(np.ptp(P, 0).max()) or 1.0
    out = {"vertices": int(len(P)), "welded": int(n), "diverging_dups": int(div.sum()), "gap_max_pct_H": 0.0}
    if not div.any():
        return out
    js, binary = F.read_glb(data)
    jn = {nd: k for k, nd in enumerate(z.jnodes)}
    worst = 0.0
    for an in js.get("animations") or []:
        ch = []
        for c in an["channels"]:
            smp = an["samplers"][c["sampler"]]
            t = F.accessor(js, binary, smp["input"]).astype(float).ravel()
            v = F.accessor(js, binary, smp["output"]).astype(float).reshape(len(t), -1)
            ch.append((c["target"]["node"], c["target"]["path"], t, v))
        T = max(c[2][-1] for c in ch)
        for tt in np.linspace(0, T, frames):
            rot, hips = {}, None
            for node, path, t, v in ch:
                i = int(np.clip(np.searchsorted(t, tt), 0, len(t) - 1))
                if path == "rotation" and node in jn:
                    rot[z.names[jn[node]]] = v[i]
                elif path == "translation" and node in jn and z.parent[jn[node]] < 0:
                    hips = v[i]
            pos, _ = z.pose(rot, hips)
            c_ = np.zeros((n, 3))
            np.add.at(c_, ids, pos)
            c_ /= cnt[:, None]
            worst = max(worst, float(np.linalg.norm(pos - c_[ids], axis=1)[div].max()))
    out["gap_max_pct_H"] = round(100 * worst / H, 2)
    return out


def front_guess(input_name):
    """The geometry front guess alone (mt.parts_cluster.guess_forward), seconds even on a dense mesh."""
    p, wall = _child([PY, "-P", "-c", "import json,sys; from mt import fastrig as F, parts_cluster as PC; "
                      "print(json.dumps(PC.guess_forward(F.Source(open(sys.argv[1], 'rb').read()))))",
                      str(_input(input_name))], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"guess_forward exit {p.returncode}: {(p.stderr or p.stdout)[-400:]}")
    out = json.loads(p.stdout.strip().splitlines()[-1])
    return {"forward_axis": out.get("forward_axis"), "source": out.get("source"), "seconds": wall}


def helper_geometry(ctx, inputs=None):
    """Helper geometry · V3 (owner bug #1953, task 31cc3ff3, a stray Icosphere around the horse): with `inputs`, the
    detector (mt.helper_geometry.detect) on each corpus file (props, bases: none must be found); without, what the
    case's mt_rig step left out of its working model (proj/helpers.json) and the meshes the rig was built on."""
    if not (MT_TREE / "mt" / "helper_geometry.py").is_file():
        return {"pending": True, "why": "mt/helper_geometry.py is not in the tree under test"}
    if inputs:
        code = ("import json, sys; from mt import helper_geometry as H; "
                "print(json.dumps({n: [h['name'] for h in H.detect(open(f, 'rb').read())['helpers']] "
                "for n, f in zip(sys.argv[1::2], sys.argv[2::2])}))")
        args = [x for n in inputs for x in (n, str(_input(n)))]
        p, wall = _child([PY, "-P", "-c", code, *args], timeout=300)
        if p.returncode != 0:
            raise RuntimeError(f"helper_geometry exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
        found = json.loads(p.stdout.strip().splitlines()[-1])
        return {"inputs": len(inputs), "helpers_found": sum(len(v) for v in found.values()),
                "found": {k: v for k, v in found.items() if v}, "seconds": wall}
    run = ctx.get("run")
    if run is None:
        raise RuntimeError("needs the mt_rig step of this case")
    rec = json.loads((run / "proj" / "helpers.json").read_text()) if (run / "proj" / "helpers.json").is_file() else {}
    from mt import fastrig as F
    src = F.Source((run / "proj" / "model.glb").read_bytes())
    kept = [str(src.js["nodes"][i["node"]].get("name") or "") for i in src.instances]
    helpers = rec.get("helpers") or []
    return {"helpers_found": len(helpers), "helpers_excluded": len(helpers) if rec.get("applied") else 0,
            "names": [h.get("name") for h in helpers], "kinds": [h.get("kind") for h in helpers],
            "source_kept": int((run / "proj" / "model_source.glb").is_file()), "meshes_rigged": len(kept),
            "kept": kept}


def texture_transform(ctx, inputs):
    """Texture transform · V3 (task 332fb890, 2026-10-16, «почему тут текстуры так коряво лежат»): Meshy / gltfpack
    GLBs keep 12-bit UVs and scale them back with KHR_texture_transform, which the viewer build drops and the MT
    renderers ignored; their WebP-only textures were not read at all. On each corpus file: mt.tex_transform.bake must
    leave no transform (tt_left), the baked plain UVs must equal the transformed source UVs (uv_max_delta), every
    material with a base colour texture must load it (untextured), and the viewer copy (mt.view_glb.convert) must
    carry no transform (viewer_tt_left). A file without a transform is the control: unchanged, delta 0."""
    if not (MT_TREE / "mt" / "tex_transform.py").is_file():
        return {"pending": True, "why": "mt/tex_transform.py is not in the tree under test"}
    code = ("import json, sys, numpy as np\n"
            "from mt import tex_transform as T, glb as G, view_glb as V\n"
            "def left(b):\n"
            "    js, _ = T.read_glb(b)\n"
            "    return sum(1 for m in js.get('materials') or [] for _p, i in T._texinfos(m) if T._tt(i) is not None)\n"
            "out = {}\n"
            "for n, f in zip(sys.argv[1::2], sys.argv[2::2]):\n"
            "    data = open(f, 'rb').read()\n"
            "    ref = G.load_glb(data)\n"
            "    baked, rep = T.bake(data)\n"
            "    m = G.load_glb(baked)\n"
            "    d = float(np.abs(m.uvs - ref.uvs).max()) if m.uvs.shape == ref.uvs.shape else 9.0\n"
            "    js, _ = T.read_glb(data)\n"
            "    want = sum(1 for x in js.get('materials') or [] if (x.get('pbrMetallicRoughness') or {}).get('baseColorTexture'))\n"
            "    got = sum(1 for x in m.materials if x.texture is not None)\n"
            "    view, _ = V.convert(data)\n"
            "    out[n] = {'had_tt': left(data), 'tt_left': left(baked), 'uv_max_delta': d, 'untextured': max(0, want - got),\n"
            "              'viewer_tt_left': left(view), 'baked_primitives': rep.get('baked_primitives') or 0}\n"
            "print(json.dumps(out))\n")
    args = [x for n in inputs for x in (n, str(_input(n)))]
    p, wall = _child([PY, "-P", "-c", code, *args], timeout=600)
    if p.returncode != 0:
        raise RuntimeError(f"texture_transform exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    per = json.loads(p.stdout.strip().splitlines()[-1])
    agg = lambda k: max(v[k] for v in per.values())                       # noqa: E731
    return {"inputs": len(per), "with_transform": sum(1 for v in per.values() if v["had_tt"]),
            "tt_left": agg("tt_left"), "uv_max_delta": agg("uv_max_delta"), "untextured": agg("untextured"),
            "viewer_tt_left": agg("viewer_tt_left"), "per_input": per, "seconds": wall}


def clay_look(ctx, inputs):
    """Clay look · V3 (mt/clay_look.py, 2026-10-16, owner: «cavity ao falloff shader sss для объектов где текстур нет или
    они повреждены»): on each corpus GLB, in a scratch run, resolve(look=clay, wait=True) must build the clay copy through
    its child process (the path the viewer uses), carry COLOR_0 on every primitive, no textures, one material, and the
    same vertex count; the bake time is reported."""
    if not (MT_TREE / "mt" / "clay_look.py").is_file():
        return {"pending": True, "why": "mt/clay_look.py is not in the tree under test"}
    code = ("import json, sys, shutil, tempfile, pathlib, time\n"
            "from mt import clay_look as C, tex_transform as T\n"
            "out = {}\n"
            "for n, f in zip(sys.argv[1::2], sys.argv[2::2]):\n"
            "    d = pathlib.Path(tempfile.mkdtemp(prefix='claylook-'))\n"
            "    (d / 'run.json').write_text('{}'); (d / 'rig').mkdir()\n"
            "    src = d / 'rig' / 'rigged.glb'; shutil.copyfile(f, src)\n"
            "    t = time.time(); got, dec = C.resolve(src, 'clay', wait=True); sec = time.time() - t\n"
            "    js, b = T.read_glb(got.read_bytes()); js0, _ = T.read_glb(src.read_bytes())\n"
            "    prims = [p for m in js.get('meshes') or [] for p in m.get('primitives') or []]\n"
            "    vc = lambda j: sum(j['accessors'][p['attributes']['POSITION']]['count'] for m in j.get('meshes') or [] for p in m.get('primitives') or [])\n"
            "    out[n] = {'built': int(got != src), 'no_color': sum(1 for p in prims if 'COLOR_0' not in p['attributes']),\n"
            "              'textures': len(js.get('textures') or []), 'materials': len(js.get('materials') or []),\n"
            "              'vertex_delta': abs(vc(js) - vc(js0)), 'seconds': round(sec, 2), 'error': dec.get('error')}\n"
            "    shutil.rmtree(d, ignore_errors=True)\n"
            "print(json.dumps(out))\n")
    args = [x for n in inputs for x in (n, str(_input(n)))]
    p, wall = _child([PY, "-P", "-c", code, *args], timeout=900)
    if p.returncode != 0:
        raise RuntimeError(f"clay_look exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    per = json.loads(p.stdout.strip().splitlines()[-1])
    return {"inputs": len(per), "built": min(v["built"] for v in per.values()),
            "no_color": max(v["no_color"] for v in per.values()), "textures": max(v["textures"] for v in per.values()),
            "materials": max(v["materials"] for v in per.values()),
            "vertex_delta": max(v["vertex_delta"] for v in per.values()),
            "bake_seconds_max": max(v["seconds"] for v in per.values()), "per_input": per, "seconds": wall}


def monster_rig(ctx):
    """«Monster rig · V3» (mt/monster.py, 2026-10-13, task 35356012 «elongated humanoid creature»): the monster verdict of
    this case's MT run, the leg type (crouched / digitigrade / stumpy / long_thin / plantigrade), the pinned joints,
    the clip policy, the head-appendage chains (mt/multilimb.py) and the rig_check stretch."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    mn = rig.get("monster") or {}
    ml = rig.get("multilimb") or {}
    st = {k: v for k, v in ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    legs = mn.get("legs") or []
    names = {b["name"] for b in rig.get("bones") or []}
    return {"use": int(bool(mn.get("use"))), "why": mn.get("why"), "leg_type": mn.get("leg_type") or "none",
            "gait": mn.get("gait"), "policy": rig.get("retarget_reference_policy") or "aligned_reference",
            "pinned": len(mn.get("pinned") or []), "arms_traced": sum(1 for a in mn.get("arms") or [] if a.get("source") == "trace"),
            "knee_bend_min": min([float(l.get("knee_bend_deg") or 0) for l in legs] or [0.0]),
            "trunk_tilt_deg": mn.get("trunk_tilt_deg"),
            "appendage_chains": sum(1 for c in ml.get("chains") or [] if c.get("appendage")),
            "crest_bones": sum(1 for n in names if re.fullmatch(r"(Left|Right)Crest\d*", n)),
            "bones": len(names), "clips": len(rig.get("clips") or []),
            "unweighted": (rig.get("checks") or {}).get("unweighted_vertices"),
            "joints_outside": len((rig.get("checks") or {}).get("joints_outside_mesh") or []),
            "fit_ok": int(bool((rig.get("fit_check") or {}).get("ok"))),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "detect_s": mn.get("seconds"), "rig_total_s": (rig.get("timings_s") or {}).get("total")}


def anim_stand(ctx, rig_input, rig_json_input, clip="auto", size=512, on_run=None):
    """Anim stand · V3 (mt/anim_stand.py, owner 2026-10-14): the 4-view stand (1 s bind pose, then Running for a
    biped) of a corpus rig on the tree under test, in a child at the conveyor's size: its wall seconds, that the
    metrics and the mp4 / gif / sheet exist, the step length in H, the elbow width in shoulder widths, the stretch and
    the leakage share."""
    run = pathlib.Path(ctx["work"]) / "stand" / "a57a00000000000000d1"
    shutil.rmtree(run.parent, ignore_errors=True)
    (run / "rig").mkdir(parents=True)
    if on_run is not None:                               # Weight mask · V3 (2026-10-14): the stand of this case's own
        rig_src = (pathlib.Path(on_run) / "rig" / "rigged.glb", pathlib.Path(on_run) / "rig" / "rig.json")   # rig step
    else:
        rig_src = (_input(rig_input), _input(rig_json_input))
    shutil.copyfile(rig_src[0], run / "rig" / "rigged.glb")
    shutil.copyfile(rig_src[1], run / "rig" / "rig.json")
    p, wall = _child([PY, "-P", "-m", "mt.anim_stand", str(run), "--clip", clip, "--size", str(int(size))], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"anim_stand exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    out = run / "rig" / "stand"
    m = json.loads((out / "metrics.json").read_text())
    files = m.get("files") or {}
    present = all(files.get(k) and (out / files[k]).is_file() and (out / files[k]).stat().st_size > 0
                  for k in ("mp4", "gif", "sheet", "metrics"))
    ew = m.get("elbow_width_sw") or {}
    return {"seconds": wall, "stand_s": (m.get("seconds") or {}).get("total"),
            "metrics_present": int(bool(m.get("schema") == "autorig.mt.anim-stand/1" and present)),
            "clip": (m.get("clip") or {}).get("name"), "frames": (m.get("clip") or {}).get("rendered_frames"),
            "step_length_H": m.get("step_length_H"), "stride_period_s": m.get("stride_period_s"),
            "elbow_width_sw_min": ew.get("min"), "elbow_width_sw_mean": ew.get("mean"),
            "worst_edge_ratio": (m.get("stretch") or {}).get("worst_edge_ratio"),
            "leak_pct": (m.get("leak") or {}).get("share_pct"), "seconds_by_stage": m.get("seconds"),
            "leak_far_pct": (m.get("leak") or {}).get("far_share_pct"),
            "leak_far_pct_radii": (((m.get("leak") or {}).get("alt") or {}).get("radii") or {}).get("far_share_pct"),
            "leak_far_pct_tube": (((m.get("leak") or {}).get("alt") or {}).get("tube") or {}).get("far_share_pct"),
            "worst_stretch_ratio": (m.get("stretch") or {}).get("worst_stretch_ratio"),
            "edges_over_4x": (m.get("stretch") or {}).get("edges_over_4x")}


def rig_contract_check(ctx, frames=12):
    """Rig contract V3.1 (owner 2026-10-17, docs/rig-contract-v3.1.md; mt/rig_contract.py) on the case's rig step:
    every clip of the rigged GLB at `frames` samples: edges over 2x / 4x (welded surface), the worst ratio, the volume,
    the far leak (thickness-scaled radii), the L/R mirror; with the skin pose record (rig.json skin.pose: standard,
    contact after, the measured A/T choice) and the layers. Self-intersections are not counted here (no pymeshlab in
    the gate's venv)."""
    run = ctx["run"]
    out = pathlib.Path(ctx["work"]) / "rig_contract.json"
    p_, wall = _child([PY, "-P", "-m", "mt.rig_contract", str(run), "--out", str(out), "--frames", str(int(frames)),
                       "--no-intersections"], timeout=300)
    if p_.returncode != 0:
        raise RuntimeError(f"rig_contract exit {p_.returncode}: {(p_.stderr or p_.stdout)[-600:]}")
    d = json.loads(out.read_text())
    c = d.get("clips") or {}
    rig = json.loads((run / "rig" / "rig.json").read_text())
    pose = (rig.get("skin") or {}).get("pose") or {}
    layers = rig.get("layers") or []
    return {"seconds": wall, "clips": len(c),
            "edges_over_2x_sum": sum(int(x.get("edges_over_2x") or 0) for x in c.values()),
            "edges_over_4x_sum": sum(int(x.get("edges_over_4x") or 0) for x in c.values()),
            "edges_over_4x_max": max([int(x.get("edges_over_4x") or 0) for x in c.values()] or [0]),
            "worst_ratio": max([float(x.get("worst_ratio") or 0) for x in c.values()] or [0]),
            "volume_dev_max_pct": max([abs(float(x.get("volume_dev_pct") or 0)) for x in c.values()] or [0]),
            "far_leak_max_pct": max([float((x.get("leak") or {}).get("far_pct") or 0) for x in c.values()] or [0]),
            "mirror_pct_H": (d.get("mirror") or {}).get("max_pct_H"),
            "contact_after": pose.get("contact_after"), "pose_reached": pose.get("reached"),
            "pose_standard": pose.get("standard"), "layers": len(layers),
            "chain_layers": sum(1 for x in layers if x.get("kind") == "chain"),
            "contract": rig.get("contract"),
            "synthetic_faces": (d.get("synthetic") or {}).get("faces"),
            "strip_max_stretch": max([float(x.get("strip_max_stretch") or 0) for x in c.values()] or [0]) or None,
            "wrist_ring_dev_max_pct": _ring_max(c, "_wrist"), "elbow_ring_dev_max_pct": _ring_max(c, "_elbow"),
            "twist_joints": len((rig.get("twist") or {}).get("joints") or [])}


def hip_blend_check(ctx, frames=12, opts=None):
    """Hem · biped (2026-10-19, Santa 718a99f3, docs/categories/biped/handoff.md «Hip blend band» / «Shoulder blend
    band»): mt/hip_blend.py (the secondary rig's first step: the hip band, then the shoulder band) on the case's rig
    and its clips, then the rig contract on the result: the Hips / UpLeg pairs per clip (edges over 2x / 4x at the
    clip's worst frame, both thighs summed, the max over the clips), the Arm / Spine1,2 pairs (the largest single pair
    and the four pairs summed, the max over the clips), the totals, the volume; plus each band's own numbers before /
    after on the solve's frames. opts: {name: value} passed as --opt to mt.hip_blend."""
    import importlib.util as _ilu
    if _ilu.find_spec("mt.hip_blend") is None:
        return {"pending": True, "why": "mt/hip_blend.py is not in the tree under test"}
    run = ctx["run"]
    work = pathlib.Path(ctx["work"])
    glb = work / "hip_blend.glb"
    p_, wall = _child([PY, "-P", "-m", "mt.hip_blend", str(run / "rig" / "rigged.glb"), "--out", str(glb)]
                      + [x for k, v in (opts or {}).items() for x in ("--opt", f"{k}={v}")], timeout=600)
    if p_.returncode != 0 or not glb.is_file():
        raise RuntimeError(f"hip_blend exit {p_.returncode}: {(p_.stderr or p_.stdout)[-600:]}")
    rep_ = json.loads(p_.stdout[p_.stdout.index("{"):])
    out = work / "hip_blend_contract.json"
    p2, wall2 = _child([PY, "-P", "-m", "mt.rig_contract", str(glb), "--rig-json", str(run / "rig" / "rig.json"),
                        "--out", str(out), "--frames", str(int(frames)), "--no-intersections", "--pairs-top", "60"],
                       timeout=400)
    if p2.returncode != 0:
        raise RuntimeError(f"rig_contract exit {p2.returncode}: {(p2.stderr or p2.stdout)[-600:]}")
    c = (json.loads(out.read_text()).get("clips") or {})

    def hu(x, key):
        return sum(int(r.get(key) or 0) for r in x.get("worst_pairs") or []
                   if "Hips" in r.get("bones", []) and any(b in ("LeftUpLeg", "RightUpLeg") for b in r.get("bones", [])))
    def arm(x, key, agg):
        v = [int(r.get(key) or 0) for r in x.get("worst_pairs") or []
             if any(b in ("LeftArm", "RightArm", "LeftArmHalf", "RightArmHalf") for b in r.get("bones", []))
             and any(b in ("Spine1", "Spine2") for b in r.get("bones", []))]
        return agg(v) if v else 0
    hip_r = (rep_.get("regions") or {}).get("hip") or rep_
    sh_r = (rep_.get("regions") or {}).get("shoulder") or {}
    nk_r = (rep_.get("regions") or {}).get("neck") or {}

    def neck(x, key):                                    # the neck band's pairs: Head / Neck and Neck / Spine2
        v = [int(r.get(key) or 0) for r in x.get("worst_pairs") or []
             if set(r.get("bones", [])) in ({"Head", "Neck"}, {"Neck", "Spine2"})]
        return max(v) if v else 0
    b_, a_ = hip_r.get("before") or {}, hip_r.get("after") or {}
    sb_, sa_ = sh_r.get("before") or {}, sh_r.get("after") or {}
    return {"seconds": round(wall + wall2, 1), "hip_blend_s": rep_.get("seconds"),
            "kept_new": int(hip_r.get("kept") == "new"), "shoulder_kept_new": int(sh_r.get("kept") == "new"),
            "shoulder_passes": len(sh_r.get("passes") or []),
            "shoulder_band_over_4x_before": sb_.get("edges_over_4x"), "shoulder_band_over_4x_after": sa_.get("edges_over_4x"),
            "shoulder_band_over_2x_after": sa_.get("edges_over_2x"),
            "neck_kept_new": int(nk_r.get("kept") == "new"), "neck_passes": len(nk_r.get("passes") or []),
            "neck_band_over_4x_before": (nk_r.get("before") or {}).get("edges_over_4x"),
            "neck_band_over_4x_after": (nk_r.get("after") or {}).get("edges_over_4x"),
            "head_neck_over_4x_max": max([neck(x, "edges_over_4x") for x in c.values()] or [0]),
            "head_neck_over_2x_max": max([neck(x, "edges") for x in c.values()] or [0]),
            "arm_spine_over_4x_max": max([arm(x, "edges_over_4x", max) for x in c.values()] or [0]),
            "arm_spine_over_4x_sum_max": max([arm(x, "edges_over_4x", sum) for x in c.values()] or [0]),
            "arm_spine_over_2x_sum_max": max([arm(x, "edges", sum) for x in c.values()] or [0]),
            "band_vertices": (rep_.get("band") or {}).get("free_vertices"),
            "band_over_2x_before": b_.get("edges_over_2x"), "band_over_2x_after": a_.get("edges_over_2x"),
            "band_over_4x_before": b_.get("edges_over_4x"), "band_over_4x_after": a_.get("edges_over_4x"),
            "band_worst_after": a_.get("worst_ratio"),
            "hips_upleg_over_2x_max": max([hu(x, "edges") for x in c.values()] or [0]),
            "hips_upleg_over_4x_max": max([hu(x, "edges_over_4x") for x in c.values()] or [0]),
            "edges_over_2x_sum": sum(int(x.get("edges_over_2x") or 0) for x in c.values()),
            "edges_over_4x_sum": sum(int(x.get("edges_over_4x") or 0) for x in c.values()),
            "volume_dev_max_pct": max([abs(float(x.get("volume_dev_pct") or 0)) for x in c.values()] or [0]),
            "volume_rule": (json.loads(out.read_text()).get("surface") or {}).get("volume_rule"),
            "clips": len(c)}


def _ring_max(clips, suffix, names=("Waving1", "Hip Hop Dancing")):
    """Sleeve twist (2026-10-18): the largest |volume change| of the wrist / elbow rings in the clips that roll the
    hands most (mt/rig_contract.py rings); None when the rig has no such ring."""
    vals = [abs(float(v.get("volume_dev_pct") or 0)) for n, x in clips.items() if n in names
            for k, v in ((x.get("rings") or {}).items()) if k.endswith(suffix)]
    return round(max(vals), 3) if vals else None


def stand_tear(ctx, clip="auto", fps=12):
    """«Mesh falls apart» on every rig of the corpus (owner order 2026-10-14, Anim stand · V3): the stand's stretch
    (mt.anim_stand.stretch_metrics = mt.skin_postvalidate.validate_glb) on the case's rig step with the stand's clip
    rule (biped Running, quadruped q-gallop, monster its own walk, else rig_check), at the stand's 12 fps samples,
    metrics only (no video). worst_edge_ratio, edges_over_4x (visible: ratio > 4 and grown > 0.5 % of the model),
    tear = ratio > 30."""
    import math as _m
    import shutil as _sh
    from mt import anim_stand as AS
    run = ctx["run"]
    t = time.time()
    holder = []
    try:
        glb, rj, _src = AS.resolve_rig(run)
        rig = AS._pick_rig(run, glb, rj, holder)
        choice = AS.choose_clip(rig, clip)
        c = AS.build_clip(rig, choice)
        o = AS.options({})
        times = np.asarray(c["times"], float)
        dur = float(times[-1]) if len(times) else 0.0
        play = min(dur, float(o["clip_seconds_max"]))
        n = int(_m.floor(play * fps + 1e-6)) + (0 if c.get("loop") and play >= dur - 1e-6 and dur > 0 else 1)
        if not (c["rotations"] or c.get("translations")) or n < 2:
            return {"seconds": round(time.time() - t, 3), "clip": c.get("name"), "status": "no_clip",
                    "worst_edge_ratio": 1.0, "edges_over_4x": 0, "tear": False, "stretch_valid": 1}
        idx = AS.resample(c, np.arange(n) / fps)["_idx"]
        _model = AS.Model(glb)
        st = AS.stretch_metrics(_model, c, idx, int(o["stretch_proxy_verts"]))
        _wp, _ws = None, None
        if hasattr(AS, "stretch_pairs"):                 # Seams · V3 (77cc1b88): the real worst stretch and its bones
            try:
                _wp, _ws = AS.stretch_pairs(_model, c, idx, 6)
            except Exception:                            # noqa: BLE001 - the stretch numbers stand
                _wp, _ws = None, None
    finally:
        for d in holder:
            _sh.rmtree(d, ignore_errors=True)
    return {"seconds": round(time.time() - t, 3), "clip": c.get("name"), "clip_why": choice.get("why"),
            "status": st.get("status"), "worst_edge_ratio": st.get("worst_edge_ratio"),
            "edges_over_4x": st.get("edges_over_4x"), "tear": bool(st.get("tear")), "samples": st.get("samples"),
            "stretch_valid": int(st.get("status") in ("failed", "numeric_deformation_passed")),
            "stretch_reason": st.get("reason"),
            "proxy": bool(st.get("proxy")),
            "worst_stretch_ratio": None if _ws is None else round(float(_ws), 3),
            "worst_pairs": _wp}


def parts_iou(ctx, models, pass_="fine", santa=("santa1", "santa2")):
    """V3.1 part labels (spec B.5, 2026-10-17): mt.analyzer labels every model of the hand-labelled parts corpus on a
    fleet mask service box; area-weighted IoU against parts_gt.json per label, averaged over the models that carry
    the label (coarse: head / arm / leg / tail / wing / torso; fine: beard, hat; cloth = cloak|coat|cape|skirt|robe)."""
    import base64 as _b64
    import zlib as _zlib
    from mt import parts_entities as PE, parts_seg as PS
    from mt.glb import load_glb
    cloth = {"cloak", "coat", "cape", "skirt", "robe"}
    agg, per, secs, failed, label_sets = {}, {}, [], [], {}
    for m in models:
        glb, gtp = _input(f"parts_{m}.model.glb"), _input(f"parts_{m}.parts_gt.json")
        gd = json.loads(gtp.read_text(encoding="utf-8"))
        codes = np.frombuffer(_zlib.decompress(_b64.b64decode(gd["face_label_zlib_b64"])), np.uint8)
        gt = np.array(gd["labels"], dtype=object)[codes]
        out = pathlib.Path(ctx["work"]) / "parts_iou" / m
        out.mkdir(parents=True, exist_ok=True)
        p, wall = _child([PY, "-P", "-m", "mt.analyzer", "--glb", str(glb), "--pass", pass_, "--no-emit",
                          "--forward", gd.get("forward") or "+z", "--category", gd.get("category") or "biped",
                          "--out", str(out)], timeout=300)
        if p.returncode != 0 or not (out / "parts_seg.npz").is_file():
            failed.append(m)
            per[m] = {"error": (p.stderr or p.stdout)[-300:]}
            continue
        z = np.load(out / "parts_seg.npz", allow_pickle=True)
        pred = np.array([str(x) for x in z["labels"]], dtype=object)[z["face_label"]]
        if len(pred) != len(gt):
            failed.append(m)
            per[m] = {"error": f"faces {len(pred)} != gt {len(gt)}"}
            continue
        mesh = load_glb(glb.read_bytes())
        area = PS.face_area(mesh.positions.astype(np.float64), mesh.faces)
        secs.append(wall)
        r = dict(PE.iou(gt, pred, area, level="coarse"))
        for lab in ("beard", "hat"):
            r.update(PE.iou(gt, pred, area, [lab]))
        cg = np.array(["cloth" if str(x).split(".")[0] in cloth else "other" for x in gt], dtype=object)
        cp = np.array(["cloth" if str(x).split(".")[0] in cloth else "other" for x in pred], dtype=object)
        r.update(PE.iou(cg, cp, area, ["cloth"]))
        per[m] = {"seconds": wall, "iou": r}
        tot = float(area.sum()) or 1.0
        label_sets[m] = sorted({str(l) for l in np.unique(pred) if str(l) != "none"
                                and float(area[pred == l].sum()) / tot >= 0.01})
        for k, v in r.items():
            if v is not None:
                agg.setdefault(k.split(".")[0], []).append(v)
    res = {f"{k}_iou_mean": round(float(np.mean(v)), 3) for k, v in sorted(agg.items())}
    res.update({f"{k}_models": len(v) for k, v in sorted(agg.items())})
    res.update({"models": len(models), "models_failed": len(failed), "failed": failed,
                "seconds_max": round(max(secs), 1) if secs else None,
                "seconds_median": round(float(np.median(secs)), 1) if secs else None, "per_model": per})
    a, b = (label_sets.get(s) for s in santa)
    if a is not None and b is not None:
        res["santa_label_sets"] = {santa[0]: a, santa[1]: b}
        res["santa_labels_same"] = int(a == b)
        res["santa_labels_jaccard"] = round(len(set(a) & set(b)) / max(1, len(set(a) | set(b))), 3)
    return res


# ------------------------------------------------------------------------------------------------ dispatch
def proportion_guard(ctx, rig_json, moves_json, glb=None, analysis=None):
    """Joints · biped (2026-10-18): a joint judge proposal recorded on a real task (04f017eb: knees +5 % H and ankles
    to 10.9 % H; 315ffe03: elbows 2 % H from the shoulders, ankles to 11 % H) through mt.proportions.guard_moves on the
    pre-judge rig, as joint_gate.apply_fix does. -> proposed, dropped, kept, the dropped joints."""
    from mt import proportions as BP
    run = pathlib.Path(ctx["work"]) / "guard_run"
    shutil.rmtree(run, ignore_errors=True)
    (run / "rig").mkdir(parents=True)
    if glb:
        shutil.copyfile(_input(glb), run / "rig" / "rigged.glb")
    for dest, name in (analysis or {}).items():
        (run / "analysis").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_input(name), run / "analysis" / dest)
    doc = json.loads(_input(rig_json).read_text())
    moves = json.loads(_input(moves_json).read_text())
    kept, dropped = BP.guard_moves(doc, moves, run=run)
    return {"proposed": len(moves), "dropped": len(dropped), "kept": len(kept), "dropped_joints": sorted(dropped),
            "why": dropped}


def arm_guide_check(ctx):
    """Joints · biped (owner 2026-10-19, the Santa 9e7c06e8: «руки до ладоней не дошли … пайплайн не видит
    собственного анализатора?»): the rig's arms against the analyser's parts (mt.arm_guide.measure): Arm under the
    arm lobe's top, Neck - Arm, Hand -> wrist anchor / palm (% H), the guide's status."""
    from mt import arm_guide as AG
    m = AG.measure(ctx["run"])
    out = {k: m.get(k) for k in ("arm_guide", "fit_source", "hand_to_wrist_max", "hand_to_palm_max",
                                 "arm_under_top_min", "arm_under_top_max", "neck_minus_arm_min")}
    out["guide_applied"] = int(m.get("arm_guide") == "applied")
    out["sides"] = {s_: m.get(s_) for s_ in ("Left", "Right")}
    return out


def run_check(check: dict, ctx: dict) -> dict:
    k = check["kind"]
    a = check.get("params") or {}
    inp = check.get("input")
    on = check.get("on")                                   # an earlier rig step of this case: its MT run's rig
    target = None
    if on:
        # Every rig step records its run under its own id (main), so a case may hold several rigs
        # (mt_rig, elephant_rig, girl_rig...) and a check names the one it judges (2026-10-14).
        run = (ctx.get("runs") or {}).get(on) or (ctx.get("run") if on == "mt_rig" else None)
        if run is None:
            raise RuntimeError(f"needs the {on} step of this case")
        ctx["run"] = run
        target = run / "rig" / "rigged.glb"
    elif inp:
        target = _input(inp)
    if k == "arm_guide":                                           # Joints · biped (2026-10-19)
        return arm_guide_check(ctx)
    if k == "proportion_guard":                                    # Joints · biped (2026-10-18)
        return proportion_guard(ctx, a["rig_json"], a["moves"], a.get("glb"), a.get("analysis"))
    if k == "voxel_legs":                                          # Legless rig · V3 (2026-10-17)
        return voxel_legs(inp)
    if k == "mt_rig_first":
        return mt_rig_first(ctx, inp, a.get("words", ""), a.get("forward_axis", ""), a.get("v3_bound", False),
                            a.get("body", ""), a.get("analysis"))
    if k == "mt_fast_analysis":
        return mt_fast_analysis(ctx, bool(a.get("apply")))
    if k == "hand_rig":
        return hand_rig(ctx)
    if k == "monster_rig":
        return monster_rig(ctx)
    if k == "anim_stand" and a.get("mode") == "tear" and on:   # stand_tear: the stand's stretch of this rig step
        return stand_tear(ctx, a.get("clip", "auto"))
    if k == "anim_stand":                                          # Anim stand · V3 (2026-10-14)
        return anim_stand(ctx, a.get("rig"), a.get("rig_json"), a.get("clip", "auto"), a.get("size", 512),
                          ctx.get("run") if on else None)
    if k == "multilimb_rig":
        return multilimb_rig(ctx)
    if k == "hand_detect":
        return hand_detect(ctx, inp, a.get("words", ""))
    if k == "serpent_rig":                                         # Serpent rig · V3
        return serpent_rig(ctx, a.get("head_near"))
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
        # `on` names any mt_rig_first check of the case (mt_rig, girl_rig, demon_rig...): its run holds the rig.json
        # (2026-10-14: a case whose rig check is not called "mt_rig" ERRORed with KeyError and blocked every gate).
        rig_json = _input(a["rig_json"]) if a.get("rig_json") else (ctx["run"] / "rig" / "rig.json" if on else None)
        words = a.get("words", ctx.get("words", "") if on else "")
        return prop_rigidity(target, ctx["work"], words, rig_json)
    if k == "bone_outside":
        rig_json = _input(a["rig_json"]) if a.get("rig_json") else ctx["run"] / "rig" / "rig.json"
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
    if k == "helper_geometry":
        return helper_geometry(ctx, a.get("inputs"))
    if k == "clay_look":
        return clay_look(ctx, a.get("inputs") or ([inp] if inp else []))
    if k == "texture_transform":
        return texture_transform(ctx, a.get("inputs") or ([inp] if inp else []))
    if k == "viewer_opens":
        return viewer_opens(target)
    if k == "quadruped_legs":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quadruped_legs(target, rig_json)
    if k == "quad_leg_trunk":                            # Fable · monsters/animals: the shin / paw on its own leg
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quad_leg_trunk(target, rig_json, float(a.get("tube_radii", 1.5)))
    if k == "rig_stretch":
        return rig_stretch(ctx)
    if k == "rig_stretch_file":
        return rig_stretch_file(_input(a["rig_json"]))
    if k == "cloth_report":
        return cloth_report(ctx["run"] / "rig" / "rig.json")
    if k == "cloth_nochange":
        return cloth_nochange(ctx)
    if k == "weight_bleed":
        return weight_bleed(ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else _input(a["rig_json"]))
    if k == "leg_joints":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return leg_joints(target, rig_json)
    if k == "hair_limbs":
        return hair_limbs(ctx)
    if k == "constitution":
        return constitution(ctx)
    if k == "anthro":
        return anthro_rig(ctx)
    if k == "biped_tail":                                          # Biped tail · V3.1 (2026-10-18)
        return biped_tail(ctx, tuple(a.get("clips") or ("Idle", "Walking", "Running")))
    if k == "anthro_words":
        return anthro_words(a.get("cases"))
    if k == "rig_joints":
        return rig_joints(ctx)
    if k == "autotex_bake":
        return autotex_bake(ctx, inp, a["paints"])
    if k == "autotex_upload_refs":
        return autotex_upload_refs(ctx, inp, a["name"])
    if k == "arm_fit":
        return arm_fit(ctx)
    if k == "tessellation":
        return tessellation(ctx)
    if k == "parts_iou":                                           # V3.1 part labels (2026-10-17)
        return parts_iou(ctx, a.get("models") or [], a.get("pass", "fine"))
    if k == "pending_detector":
        return {"pending": True}
    if k == "posed_prop_fit":
        return posed_prop_fit(ctx, a.get("front", ""))
    if k == "rig_contract":                               # Rig contract V3.1 (2026-10-17)
        return rig_contract_check(ctx, int(a.get("frames", 12)))
    if k == "hip_blend":                                  # Hem · biped (2026-10-19)
        return hip_blend_check(ctx, int(a.get("frames", 12)), a.get("opts"))
    if k == "detach":                                     # Rig fixes · V3 (2026-10-14): mt/detach_check.py on
        from mt import detach_check as _dc                 # the rig: weld-split holes / torn pieces in the clips
        _r = _dc.check(pathlib.Path(target).read_bytes(), frames=int(a.get("frames", 12)))
        return {k_: _r.get(k_) for k_ in ("cuts", "hole_copies", "slit_max_pct_H", "torn_copies",
                                          "detach_max_pct_H", "released_max_pct_H")}
    if k == "seam_weights":                               # Seams · V3 (2026-10-14)
        return seam_weights(ctx)
    if k == "dup_gaps":
        return dup_gaps(ctx["run"] / "rig" / "rigged.glb")
    if k == "rig_upright":
        return rig_upright(ctx)
    if k == "front_guess":
        out = front_guess(inp)
        out["front_ok"] = int(out["forward_axis"] == a.get("front"))
        return out
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
            if ctx.get("run") is not None and not check.get("on"):   # a rig step: remember its run by id
                ctx.setdefault("runs", {})[check["id"]] = ctx["run"]
        except Exception as exc:                                   # noqa: BLE001 - reported as ERROR, never hidden
            row = {"case": case["id"], "check": check["id"], "error": f"{type(exc).__name__}: {exc}"[:800],
                   "trace": traceback.format_exc()[-1500:]}
        row["wall_s"] = round(time.time() - t, 3)
        print(json.dumps(row, ensure_ascii=False, default=lambda o: o.tolist() if hasattr(o, "tolist") else str(o)),
              flush=True)


if __name__ == "__main__":
    main()
