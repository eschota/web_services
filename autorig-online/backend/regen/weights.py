"""Skin weights of hair / cloth vertices to bone chains (AutoRig Regen).

STANDALONE MODULE. The Blender step loads this file BY FILE PATH inside
Blender's bundled Python, so it must stay that way:

* it imports only the standard library and numpy — never scipy, trimesh, or
  anything from the ``regen`` package;
* it has no package-relative imports (``from .x import y`` fails when the
  file is executed outside its package);
* it sticks to Python 3.7 syntax and numpy >= 1.17 APIs.

Loading it from Blender (register in ``sys.modules`` before ``exec_module``,
see the renderfin gotchas)::

    import importlib.util, sys
    spec = importlib.util.spec_from_file_location("regen_weights", "/path/to/regen/weights.py")
    weights = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = weights
    spec.loader.exec_module(weights)
    assert weights.WEIGHTS_API_VERSION == 1
    bone_ids, bone_weights = weights.compute_chain_weights(points, chains, attach_origin=origin)

Bone numbering (a flat list per group): 0 is the attach bone; chain ``c``
joint ``j`` (``j < J_c - 1``; end joints never get weight) is
``1 + offset(c) + j`` with ``offset(c) = sum(J_k - 1 for k < c)``.

Algorithm, per point:

1. Project it onto every chain polyline (root -> end joint). With an
   ``attach_origin`` each chain is extended by a virtual segment from the
   origin to its root; a point nearest that virtual segment is "above the
   root" and belongs to the attach bone.
2. Keep the two nearest chains, weighted by inverse squared distance.
3. Along each chain, blend between the two bones whose centres bracket the
   projection (a bone's centre is the middle of its segment), so bends are
   smooth; before the first centre it is all the first bone, after the last
   deforming bone's centre all that bone.
4. Near the root, fade to the attach bone: weight 1 at the root, 0 once the
   projection is ``attach_blend`` of the way along the first segment.
5. Keep the ``max_influences`` largest weights and renormalise.
"""

from __future__ import annotations

import numpy as np

WEIGHTS_API_VERSION = 1

__all__ = ["WEIGHTS_API_VERSION", "compute_chain_weights", "chain_bone_ids", "bone_count"]

_CHUNK = 65536


def _as_chains(chains):
    out = []
    for c, joints in enumerate(chains):
        arr = np.asarray(joints, dtype=np.float64).reshape(-1, 3)
        if len(arr) < 2:
            raise ValueError("chain %d has %d joint(s); a chain needs a root and an end joint" % (c, len(arr)))
        if not np.all(np.isfinite(arr)):
            raise ValueError("chain %d has non-finite joint positions" % c)
        out.append(arr)
    return out


def chain_bone_ids(chains):
    """Bone ids of every chain's deforming joints (end joints excluded)."""
    ids = []
    offset = 0
    for joints in _as_chains(chains):
        n = len(joints) - 1
        ids.append(np.arange(1 + offset, 1 + offset + n, dtype=np.int32))
        offset += n
    return ids


def bone_count(chains):
    """Number of bone ids, the attach bone included."""
    return 1 + sum(len(np.asarray(j).reshape(-1, 3)) - 1 for j in chains)


def _project(points, joints):
    """Distance to a polyline, continuous parameter u (segment + t) and the
    unclamped parameter on the first segment."""
    a = joints[:-1]
    ab = joints[1:] - a
    l2 = np.einsum("ij,ij->i", ab, ab)
    safe = np.where(l2 > 0.0, l2, 1.0)
    ap = points[:, None, :] - a[None, :, :]
    t_raw = np.einsum("nsc,sc->ns", ap, ab) / safe[None, :]
    t_raw[:, l2 <= 0.0] = 0.0
    t = np.clip(t_raw, 0.0, 1.0)
    diff = ap - t[:, :, None] * ab[None, :, :]
    d2 = np.einsum("nsc,nsc->ns", diff, diff)
    seg = np.argmin(d2, axis=1)
    rows = np.arange(len(points))
    return np.sqrt(d2[rows, seg]), seg + t[rows, seg], t_raw[:, 0]


def _segment_distance(points, a, b):
    ab = b - a
    l2 = float(ab @ ab)
    if l2 <= 0.0:
        return np.linalg.norm(points - a, axis=1)
    t = np.clip((points - a) @ ab / l2, 0.0, 1.0)
    return np.linalg.norm(points - (a + t[:, None] * ab), axis=1)


def compute_chain_weights(
    points,
    chains,
    *,
    attach_origin=None,
    attach_blend=0.25,
    max_influences=4,
):
    """Return (bone_ids, weights), both shape (N, max_influences): bone_ids int32, weights float32.
    bone_ids index a flat list: 0 = the attach bone; chain c joint j (j < J_c - 1, end joints never get weight)
    is 1 + offset(c) + j, with offset(c) = sum(J_k - 1 for k < c).
    Unused slots have id 0 and weight 0. Weights are >= 0 and every row sums to 1 (float32 tolerance).

    points:        (N, 3) vertices to weight, same space as the chains.
    chains:        sequence of (J_c, 3) joint positions root -> end, END JOINT LAST.
    attach_origin: optional (3,) centroid of the attachment line; weight fades
                   from the attach bone to the chains near it.
    attach_blend:  fraction of each chain's first segment over which the
                   attach-bone weight fades to 0.
    """
    k = int(max_influences)
    if k < 1:
        raise ValueError("max_influences must be >= 1")
    pts = np.asarray(points, dtype=np.float64).reshape(-1, 3)
    if not np.all(np.isfinite(pts)):
        raise ValueError("points must be finite")
    n = len(pts)
    bone_ids = np.zeros((n, k), dtype=np.int32)
    weights = np.zeros((n, k), dtype=np.float32)
    if n == 0:
        return bone_ids, weights
    chain_list = _as_chains(chains)
    if not chain_list:
        weights[:, 0] = 1.0
        return bone_ids, weights
    blend = max(float(attach_blend), 0.0)
    origin = None
    if attach_origin is not None:
        origin = np.asarray(attach_origin, dtype=np.float64).reshape(3)
        if not np.all(np.isfinite(origin)):
            origin = None

    n_chains = len(chain_list)
    offsets = np.cumsum([0] + [len(j) - 1 for j in chain_list[:-1]]).astype(np.int64)
    n_bones = np.array([len(j) - 1 for j in chain_list], dtype=np.int64)
    everything = np.concatenate(chain_list + ([origin[None, :]] if origin is not None else []))
    extent = float(np.linalg.norm(everything.max(axis=0) - everything.min(axis=0)))
    eps = max(extent, 1.0) * 1e-9

    for start in range(0, n, _CHUNK):
        p = pts[start : start + _CHUNK]
        m = len(p)
        dist = np.empty((m, n_chains))
        u = np.empty((m, n_chains))
        pre = np.zeros((m, n_chains), dtype=bool)
        for c, joints in enumerate(chain_list):
            d_poly, u_c, t0 = _project(p, joints)
            if origin is not None:
                d_virtual = _segment_distance(p, origin, joints[0])
                pre[:, c] = d_virtual < d_poly
                dist[:, c] = np.minimum(d_virtual, d_poly)
            else:
                pre[:, c] = (u_c <= 0.0) & (t0 < 0.0)
                dist[:, c] = d_poly
            u[:, c] = u_c

        rows = np.arange(m)
        if n_chains == 1:
            picks = np.zeros((m, 1), dtype=np.int64)
        else:
            picks = np.argpartition(dist, 1, axis=1)[:, :2]
            swap = dist[rows, picks[:, 1]] < dist[rows, picks[:, 0]]
            picks[swap] = picks[swap][:, ::-1]
        inv = 1.0 / (dist[rows[:, None], picks] + eps) ** 2
        chain_w = inv / inv.sum(axis=1, keepdims=True)

        cand_ids = [np.zeros(m, dtype=np.int64)]
        cand_w = [np.zeros(m)]
        for slot in range(picks.shape[1]):
            c = picks[:, slot]
            w_c = chain_w[:, slot]
            u_c = u[rows, c]
            if blend > 0.0:
                fade = np.clip(1.0 - u_c / blend, 0.0, 1.0)
            else:
                fade = np.zeros(m)
            a = np.where(pre[rows, c], 1.0, fade)
            nb = n_bones[c]
            x = u_c - 0.5
            j0 = np.floor(x).astype(np.int64)
            frac = x - j0
            low = j0 < 0
            high = j0 >= nb - 1
            j0 = np.where(low, 0, np.where(high, nb - 1, j0))
            frac = np.where(low | high, 0.0, frac)
            j1 = np.minimum(j0 + 1, nb - 1)
            base = 1 + offsets[c]
            cand_w[0] = cand_w[0] + w_c * a
            cand_ids.append(base + j0)
            cand_w.append(w_c * (1.0 - a) * (1.0 - frac))
            cand_ids.append(base + j1)
            cand_w.append(w_c * (1.0 - a) * frac)

        ids = np.stack(cand_ids, axis=1)
        w = np.stack(cand_w, axis=1)
        order = np.argsort(-w, axis=1, kind="stable")[:, :k]
        top_ids = np.take_along_axis(ids, order, axis=1)
        top_w = np.take_along_axis(w, order, axis=1)
        top_w = np.where(top_w > 0.0, top_w, 0.0)
        total = top_w.sum(axis=1, keepdims=True)
        top_w = top_w / np.where(total > 0.0, total, 1.0)
        top_ids = np.where(top_w > 0.0, top_ids, 0)
        if top_w.shape[1] < k:
            pad = k - top_w.shape[1]
            top_w = np.pad(top_w, ((0, 0), (0, pad)))
            top_ids = np.pad(top_ids, ((0, 0), (0, pad)))
        w32 = top_w.astype(np.float32)
        w32[:, 0] += np.float32(1.0) - w32.sum(axis=1, dtype=np.float32)
        weights[start : start + m] = w32
        bone_ids[start : start + m] = top_ids.astype(np.int32)
    return bone_ids, weights
