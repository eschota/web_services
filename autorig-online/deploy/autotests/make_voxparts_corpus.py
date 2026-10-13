#!/usr/bin/env python3
"""Corpus models for Voxel parts · V3 (owner 2026-10-14, docs/voxel-parts-spec-20261014.md in the MT repo), built
deterministically from read-only corpus inputs (no downloads, no new customer data):

    sudo python3 make_voxparts_corpus.py --out /srv/autorig/data/autotests/synthetic \
        --backend /srv/autorig/current/autorig-online/backend

  voxparts_obj_groups_sword.obj   the warrior 66ba97ba as an OBJ: `o body` (usemtl Skin) and `o sword` (usemtl
                                  Blade); the sword touches the hand (one voxel component with the body), so only its
                                  NAME can find it before the fit
  voxparts_obj_groups_sword.glb   that OBJ through the intake's obj_to_glb (one mesh per group, names in
                                  extras.autorig): the MT rig case's input
  voxparts_companion.glb          the mannequin 8a1b1cc5 with the horse 31cc3ff3 (its Icosphere left out) scaled to
                                  30 % of the mannequin's height, standing beside its feet 15 % H away: the horse is a
                                  floating_object on its own root bone, the mannequin rigs as before
"""
import argparse
import json
import pathlib
import shutil
import struct
import sys
import tempfile

import numpy as np

CORPUS = pathlib.Path("/srv/autorig/data/autotests/corpus")
MT_ROOT = pathlib.Path("/srv/autorig/data/motion_transfer")


def glb_meshes(path, parts):
    """parts: [(name, P (n x 3), F (m x 3))] -> a GLB with one node + mesh per part."""
    blob = b""
    views, accs, meshes, nodes = [], [], [], []
    for k, (name, P, F) in enumerate(parts):
        pos = np.ascontiguousarray(P, np.float32)
        idx = np.ascontiguousarray(np.asarray(F).reshape(-1), np.uint32)
        for arr, typ, ct, tgt in ((pos, "VEC3", 5126, 34962), (idx, "SCALAR", 5125, 34963)):
            b = arr.tobytes()
            views.append({"buffer": 0, "byteOffset": len(blob), "byteLength": len(b), "target": tgt})
            a = {"bufferView": len(views) - 1, "componentType": ct, "type": typ,
                 "count": int(len(pos) if typ == "VEC3" else idx.size)}
            if typ == "VEC3":
                a.update(min=pos.min(0).tolist(), max=pos.max(0).tolist())
            accs.append(a)
            blob += b + b"\0" * (-len(b) % 4)
        meshes.append({"name": name, "primitives": [{"attributes": {"POSITION": 2 * k}, "indices": 2 * k + 1,
                                                     "mode": 4}]})
        nodes.append({"mesh": k, "name": name})
    js = {"asset": {"version": "2.0", "generator": "autotests make_voxparts_corpus"}, "scene": 0,
          "scenes": [{"nodes": list(range(len(nodes)))}], "nodes": nodes, "meshes": meshes, "accessors": accs,
          "bufferViews": views, "buffers": [{"byteLength": len(blob)}]}
    jb = json.dumps(js, separators=(",", ":")).encode()
    jb += b" " * (-len(jb) % 4)
    out = (b"glTF" + struct.pack("<II", 2, 12 + 8 + len(jb) + 8 + len(blob)) + struct.pack("<I", len(jb)) + b"JSON"
           + jb + struct.pack("<I", len(blob)) + b"BIN\0" + blob)
    pathlib.Path(path).write_bytes(out)


def obj_groups_sword(out, backend):
    from mt import fastrig as F, parts_cluster as PC
    src_glb = CORPUS / "66ba97ba.upload.glb"
    with tempfile.TemporaryDirectory() as tmp:
        run = pathlib.Path(tmp)
        (run / "proj").mkdir()
        shutil.copyfile(src_glb, run / "proj" / "model.glb")
        doc = PC.cluster(run, rig={}, words="female warrior with a sword", export=False, write=True)
        fp = np.load(run / "analysis" / "parts.npz")["face_part"]
    props = [i for i, p in enumerate(doc["parts"]) if p.get("class") == "prop"]
    if len(props) != 1:
        raise SystemExit(f"expected one sword, parts_cluster found {len(props)}")
    src = F.Source(src_glb.read_bytes())
    P, faces = src.positions, src.faces
    sword = fp == props[0]
    lines = ["# voxparts_obj_groups_sword: 66ba97ba split into o body / o sword (make_voxparts_corpus.py)",
             "mtllib voxparts_obj_groups_sword.mtl"]
    lines += [f"v {x:.6f} {y:.6f} {z:.6f}" for x, y, z in P]
    for name, mtl, sel in (("body", "Skin", ~sword), ("sword", "Blade", sword)):
        lines += [f"o {name}", f"usemtl {mtl}"]
        lines += [f"f {a + 1} {b + 1} {c + 1}" for a, b, c in faces[sel]]
    obj = ("\n".join(lines) + "\n").encode()
    (out / "voxparts_obj_groups_sword.obj").write_bytes(obj)
    sys.path.insert(0, str(backend))
    import v3_intake
    glb = v3_intake.obj_to_glb(obj)
    (out / "voxparts_obj_groups_sword.glb").write_bytes(glb)
    return {"sword_faces": int(sword.sum()), "body_faces": int((~sword).sum()), "glb_bytes": len(glb)}


def subdivide(P, faces, max_len):
    """Midpoint 4-way split of every triangle longer than max_len until none is (an edge's midpoint is shared by
    the two triangles that split it; a short neighbour of a split triangle keeps a T-junction)."""
    P = [tuple(p) for p in np.asarray(P, np.float64)]
    F = [tuple(int(v) for v in f) for f in faces]
    for _ in range(12):
        mids, out, split = {}, [], False

        def mid(a, b):
            k = (min(a, b), max(a, b))
            if k not in mids:
                mids[k] = len(P)
                P.append(tuple((np.array(P[a]) + np.array(P[b])) / 2))
            return mids[k]
        A = np.array(P)
        for a, b, c in F:
            if max(np.linalg.norm(A[a] - A[b]), np.linalg.norm(A[b] - A[c]), np.linalg.norm(A[c] - A[a])) > max_len:
                ab, bc, ca = mid(a, b), mid(b, c), mid(c, a)
                out += [(a, ab, ca), (ab, b, bc), (ca, bc, c), (ab, bc, ca)]
                split = True
            else:
                out.append((a, b, c))
        F = out
        if not split:
            break
    return np.array(P), np.array(F, np.int64)


def companion(out):
    from mt import fastrig as F, helper_geometry as HG
    man = F.Source((CORPUS / "8a1b1cc5.upload.glb").read_bytes())
    data = (CORPUS / "31cc3ff3.upload.glb").read_bytes()
    rep = HG.detect(data)
    data = HG.strip(data, [h["node"] for h in rep["helpers"]])
    horse = F.Source(data)
    H = float(np.ptp(man.positions[:, 1]))
    # the 448-vertex box mannequin subdivided to edges <= 2 % H: next to the dense horse the pre-rig tessellation
    # (relative to the scene's median edge) leaves it alone, and box forearms with corner vertices only got none
    Pm, Fm = subdivide(man.positions, man.faces, 0.02 * H)
    Ph = horse.positions - horse.positions.min(0)
    Ph = Ph * (0.3 * H / float(np.ptp(Ph[:, 1])))
    lo, hi = Pm.min(0), Pm.max(0)
    # beside the mannequin's right side (-x), its feet level, its body along z, 15 % H off the mannequin's box
    Ph = Ph + np.array([lo[0] - 0.15 * H - float(np.ptp(Ph[:, 0])), lo[1], (lo[2] + hi[2]) / 2 - np.ptp(Ph[:, 2]) / 2])
    glb_meshes(out / "voxparts_companion.glb", [("mannequin", Pm, Fm), ("animal", Ph, horse.faces)])
    return {"mannequin_vertices": int(len(Pm)), "animal_vertices": int(len(Ph)), "H": round(H, 4)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/srv/autorig/data/autotests/synthetic")
    ap.add_argument("--backend", default="/srv/autorig/current/autorig-online/backend")
    ap.add_argument("--mt-root", default=str(MT_ROOT))
    a = ap.parse_args()
    sys.path.insert(0, a.mt_root)
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    print(json.dumps({"obj_groups_sword": obj_groups_sword(out, pathlib.Path(a.backend)),
                      "companion": companion(out)}))


if __name__ == "__main__":
    main()
