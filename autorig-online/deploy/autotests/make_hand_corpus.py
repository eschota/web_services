#!/usr/bin/env python3
"""Synthetic hands-only models for the regression corpus (Hands rig · V3, 2026-10-11): closed, connected low-poly
surfaces built from a capsule union on a voxel grid (boundary faces, welded, a few Laplacian steps), written as
GLB. Deterministic, no downloads, no customer data.

    sudo python3 make_hand_corpus.py --out /srv/autorig/data/autotests/synthetic

Models:
    hand_left_open.glb      a left hand, palm facing +z, fingers +y slightly curled toward the palm, thumb at -x
    arm_right_fps.glb       a right hand with a forearm (FPS arm), palm facing +z
    glove_mitten.glb        a glove whose four fingers are fused into one block (thumb separate)
    hands_pair_mirror.glb   the left hand and its mirror image (a right hand) 25 cm apart
"""
import argparse
import json
import pathlib
import struct

import numpy as np


# ------------------------------------------------------------------------------------------------ capsules -> mesh
def capsule_sdf(p, a, b, r0, r1):
    ab = b - a
    t = np.clip(((p - a) @ ab) / max(float(ab @ ab), 1e-12), 0, 1)
    d = np.linalg.norm(p - (a + t[:, None] * ab), axis=1)
    return d - (r0 + (r1 - r0) * t)


def voxel_mesh(capsules, cell=0.003, pad=3):
    pts = np.array([c[0] for c in capsules] + [c[1] for c in capsules])
    rmax = max(max(c[2], c[3]) for c in capsules)
    lo = pts.min(0) - rmax - pad * cell
    hi = pts.max(0) + rmax + pad * cell
    n = np.ceil((hi - lo) / cell).astype(int) + 1
    gx, gy, gz = np.meshgrid(np.arange(n[0]), np.arange(n[1]), np.arange(n[2]), indexing="ij")
    centres = lo + (np.stack([gx, gy, gz], -1).reshape(-1, 3) + 0.5) * cell
    sdf = np.full(len(centres), np.inf)
    for a, b, r0, r1 in capsules:
        sdf = np.minimum(sdf, capsule_sdf(centres, np.asarray(a, float), np.asarray(b, float), r0, r1))
    occ = (sdf <= 0).reshape(n[0], n[1], n[2])
    occ[0, :, :] = occ[-1, :, :] = occ[:, 0, :] = occ[:, -1, :] = occ[:, :, 0] = occ[:, :, -1] = False
    # boundary faces between an occupied voxel and an empty neighbour, as quads on the voxel corners
    verts = {}
    faces = []

    def vid(i, j, k):
        key = (int(i), int(j), int(k))
        if key not in verts:
            verts[key] = len(verts)
        return verts[key]
    oi, oj, ok = np.nonzero(occ)
    for d, (dx, dy, dz) in enumerate(((1, 0, 0), (-1, 0, 0), (0, 1, 0), (0, -1, 0), (0, 0, 1), (0, 0, -1))):
        nb = occ[oi + dx, oj + dy, ok + dz]
        for i, j, k in zip(oi[~nb], oj[~nb], ok[~nb]):
            # the face of voxel (i,j,k) toward (dx,dy,dz), corners counter-clockwise seen from outside
            if dx:
                x = i + (1 if dx > 0 else 0)
                c = [(x, j, k), (x, j + 1, k), (x, j + 1, k + 1), (x, j, k + 1)]
                if dx < 0:
                    c = c[::-1]
            elif dy:
                y = j + (1 if dy > 0 else 0)
                c = [(i, y, k), (i, y, k + 1), (i + 1, y, k + 1), (i + 1, y, k)]
                if dy < 0:
                    c = c[::-1]
            else:
                z = k + (1 if dz > 0 else 0)
                c = [(i, j, z), (i + 1, j, z), (i + 1, j + 1, z), (i, j + 1, z)]
                if dz < 0:
                    c = c[::-1]
            v = [vid(*q) for q in c]
            faces.append((v[0], v[1], v[2]))
            faces.append((v[0], v[2], v[3]))
    keys = np.array(list(verts.keys()), float)
    P = lo + keys * cell
    F = np.array(faces, np.int64)
    # a few Laplacian steps on the positions (the blocky surface becomes a smooth low-poly one)
    nv = len(P)
    eu = np.concatenate([F[:, 0], F[:, 1], F[:, 2]])
    ev = np.concatenate([F[:, 1], F[:, 2], F[:, 0]])
    deg = np.bincount(eu, minlength=nv) + np.bincount(ev, minlength=nv)
    for _ in range(6):
        avg = np.zeros_like(P)
        for c in range(3):
            avg[:, c] = np.bincount(eu, P[ev, c], nv) + np.bincount(ev, P[eu, c], nv)
        avg /= np.maximum(deg, 1)[:, None]
        P = 0.4 * P + 0.6 * avg
    return P.astype(np.float32), F.astype(np.uint32)


def write_glb(path, P, F, name="hand"):
    pos = np.ascontiguousarray(P, np.float32)
    idx = np.ascontiguousarray(F.reshape(-1), np.uint32)
    bin_ = pos.tobytes() + idx.tobytes()
    bin_ += b"\0" * (-len(bin_) % 4)
    js = {"asset": {"version": "2.0", "generator": "autotests make_hand_corpus"},
          "scene": 0, "scenes": [{"nodes": [0]}], "nodes": [{"mesh": 0, "name": name}],
          "meshes": [{"name": name, "primitives": [{"attributes": {"POSITION": 0}, "indices": 1, "mode": 4}]}],
          "buffers": [{"byteLength": len(bin_)}],
          "bufferViews": [{"buffer": 0, "byteOffset": 0, "byteLength": pos.nbytes, "target": 34962},
                          {"buffer": 0, "byteOffset": pos.nbytes, "byteLength": idx.nbytes, "target": 34963}],
          "accessors": [{"bufferView": 0, "componentType": 5126, "count": len(pos), "type": "VEC3",
                         "min": pos.min(0).tolist(), "max": pos.max(0).tolist()},
                        {"bufferView": 1, "componentType": 5125, "count": len(idx), "type": "SCALAR"}]}
    jb = json.dumps(js, separators=(",", ":")).encode()
    jb += b" " * (-len(jb) % 4)
    out = (b"glTF" + struct.pack("<II", 2, 12 + 8 + len(jb) + 8 + len(bin_)) + struct.pack("<I", len(jb)) + b"JSON" + jb
           + struct.pack("<I", len(bin_)) + b"BIN\0" + bin_)
    pathlib.Path(path).write_bytes(out)
    return len(out)


# ------------------------------------------------------------------------------------------------ hands
def hand_capsules(mirror=False, fused=False, forearm=False, curl=1.0):
    """A left hand: palm from the wrist (y=0) to the knuckles (y=0.10), palm facing +z, thumb at -x.  mirror=True
    turns it into a right hand (x -> -x)."""
    caps = []
    # the palm: three vertical capsules side by side, a little thicker at the wrist
    for x in (-0.028, 0.0, 0.028):
        caps.append(((x, 0.005, 0.0), (x, 0.095, 0.0), 0.017, 0.016))
    caps.append(((-0.036, 0.0, 0.0), (0.036, 0.0, 0.0), 0.013, 0.013))         # the wrist bar
    caps.append(((-0.036, 0.092, 0.0), (0.036, 0.092, 0.0), 0.014, 0.014))     # the knuckle bar
    fingers = [(-0.034, 0.075, 0.0085), (-0.011, 0.085, 0.009), (0.012, 0.079, 0.0085), (0.034, 0.063, 0.0075)]
    if fused:
        fingers = [(-0.034, 0.072, 0.012), (-0.011, 0.078, 0.012), (0.012, 0.074, 0.012), (0.034, 0.064, 0.011)]
    for x, length, r in fingers:
        base = np.array([x, 0.098, 0.0])
        d = np.array([0.0, 1.0, 0.0])
        for k, (frac, bend) in enumerate(((0.45, 0.0), (0.30, 12.0 * curl), (0.25, 24.0 * curl))):
            ang = np.radians(bend)
            d = np.array([0.0, np.cos(ang), np.sin(ang)])          # bends toward +z: the palm side
            tip = base + d * length * frac
            caps.append((tuple(base), tuple(tip), r, r * (0.92 if k < 2 else 0.8)))
            base = tip
    # the thumb: from the lower outer palm, diagonal, a little toward the palm side
    base = np.array([-0.038, 0.03, 0.006])
    d = np.array([-0.62, 0.72, 0.3])
    d /= np.linalg.norm(d)
    for frac, r0, r1 in ((0.5, 0.012, 0.011), (0.5, 0.011, 0.009)):
        tip = base + d * 0.066 * frac
        caps.append((tuple(base), tuple(tip), r0, r1))
        base = tip
        d = np.array([-0.5, 0.72, 0.48])
        d /= np.linalg.norm(d)
    if forearm:
        caps.append(((0.0, 0.0, 0.0), (0.0, -0.26, 0.0), 0.028, 0.036))
    if mirror:
        caps = [((-a[0], a[1], a[2]), (-b[0], b[1], b[2]), r0, r1) for a, b, r0, r1 in caps]
    return caps


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--cell", type=float, default=0.003)
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    models = {
        "hand_left_open.glb": hand_capsules(),
        "arm_right_fps.glb": hand_capsules(mirror=True, forearm=True),
        "glove_mitten.glb": hand_capsules(fused=True, curl=0.4),
    }
    for name, caps in models.items():
        P, F = voxel_mesh(caps, a.cell)
        n = write_glb(out / name, P, F, name[:-4])
        print(json.dumps({"file": name, "vertices": int(len(P)), "triangles": int(len(F)), "bytes": n}))
    # the pair: the left hand and its mirror image, 25 cm apart
    P1, F1 = voxel_mesh(hand_capsules(), a.cell)
    P2, F2 = voxel_mesh(hand_capsules(mirror=True), a.cell)
    P2 = P2 + np.array([0.25, 0.0, 0.0], np.float32)
    P = np.concatenate([P1, P2])
    F = np.concatenate([F1, F2 + len(P1)])
    n = write_glb(out / "hands_pair_mirror.glb", P, F, "hands_pair")
    print(json.dumps({"file": "hands_pair_mirror.glb", "vertices": int(len(P)), "triangles": int(len(F)), "bytes": n}))


if __name__ == "__main__":
    main()
