"""Fabric · V3 (Fable · monsters/animals, 2026-10-13): a secondary bone layer for cloth and long hair on bipeds, the
counterpart of mt/mane.py for quadrupeds.

  * a SKIRT / ROBE / DRESS that hangs over both legs (the midline between the legs is covered from the hips down to
    the hem) gets its own chains hanging from Hips, one per sector around the hips (front, sides, back; the count
    from the hem's girth), each 2-4 bones down to the hem; the skirt's skin takes the chains, blended between the
    two nearest sectors by azimuth, and keeps the hips' / legs' weights only over a short root blend at the top, so
    the leg influence fades toward the hem;
  * a CAPE (a thin sheet hanging behind the back below the shoulders) gets sector chains from Spine2;
  * LONG HAIR below the neck that find_strands did not take (a curtain, not a braid) gets sector chains from Head.
  Where a fabric piece is welded onto a limb (a robe on the boots, hair on a shoulder) the joining faces are split
  like hair | limb welds (weld_split), so the limb slides past.
  The chains follow with a mild lag in every clip (a pendulum swing at the clip's rate with a lag per bone), and in
  the rig_check clip.
Options (SPEC; env FABRIC_<NAME>, per run <run>/fabric_options.json): fabric off | auto | on, ...
"""
from __future__ import annotations

import json
import os
import pathlib
import re

import numpy as np

SPEC = {
    "fabric": {"values": ("off", "auto", "on"), "default": "auto",
               "doc": "auto: a biped whose geometry shows a skirt / robe, a cape or long loose hair gets fabric chains; off: never"},
    "skirt": {"values": ("on", "off"), "default": "on", "doc": "the skirt / robe layer"},
    "cape": {"values": ("on", "off"), "default": "on", "doc": "the cape layer"},
    "hair": {"values": ("on", "off"), "default": "on", "doc": "the long loose hair layer"},
    "skirt_tube": {"min": 1.0, "max": 3.0, "default": 1.4, "doc": "skin farther than this many leg radii from the leg axes is skirt"},
    "skirt_min_midline": {"min": 0.3, "max": 1.0, "default": 0.7, "doc": "share of the hip -> hem rows with surface between the legs that makes a skirt"},
    "skirt_min_vertices": {"min": 20, "max": 5000, "default": 80},
    "skirt_inner": {"min": 0.0, "max": 0.95, "default": 0.55, "doc": "skin closer to a leg axis than this share of its row's girth radius is a leg under the robe, not skirt"},
    "sector_girth_pct_H": {"min": 10.0, "max": 100.0, "default": 30.0, "doc": "one chain per this much of the hem's girth (% of H)"},
    "sectors_min": {"min": 2, "max": 12, "default": 4},
    "sectors_max": {"min": 2, "max": 16, "default": 8},
    "bone_len_pct_H": {"min": 3.0, "max": 40.0, "default": 12.0, "doc": "a chain bone is about this long (% of H): 2-4 bones per chain"},
    "root_skip": {"min": 0.0, "max": 0.6, "default": 0.08, "doc": "share of the piece's height from its top that keeps the body's weights entirely"},
    "root_blend": {"min": 0.02, "max": 1.0, "default": 0.22, "doc": "share of the piece's height over which the body's weights fade into the chains"},
    "split": {"values": ("on", "off"), "default": "on", "doc": "split the faces that weld a fabric piece onto a limb (weld_split)"},
    "sway_deg": {"min": 0.0, "max": 30.0, "default": 6.0, "doc": "swing amplitude of the chains in the clips (deg, at the tip)"},
    "sway_lag": {"min": 0.0, "max": 2.0, "default": 0.6, "doc": "phase lag per bone along a chain"},
    "cape_depth": {"min": 0.1, "max": 1.0, "default": 0.3, "doc": "skin this share of the torso depth behind the spine, below the shoulders, is cape"},
    "cape_min_vertices": {"min": 20, "max": 5000, "default": 120},
    "hair_min_vertices": {"min": 20, "max": 5000, "default": 100},
}
ENV = "FABRIC_"


def options(opts=None) -> dict:
    o, given = {}, dict(opts or {})
    for k, s in SPEC.items():
        v = given.get(k, os.environ.get(ENV + k.upper(), s["default"]))
        if "values" in s:
            v = str(v)
            if v not in s["values"]:
                raise ValueError(f"fabric option {k}: {v!r} not in {s['values']}")
        else:
            v = float(v)
            if not (s["min"] <= v <= s["max"]):
                raise ValueError(f"fabric option {k}: {v} outside [{s['min']}, {s['max']}]")
        o[k] = v
    unknown = sorted(set(given) - set(SPEC))
    if unknown:
        raise ValueError(f"fabric: unknown options {unknown}")
    return o


def options_get() -> dict:
    return {k: {"default": s["default"], **{kk: vv for kk, vv in s.items() if kk != "default"}} for k, s in SPEC.items()}


def options_for(run_dir, opts=None) -> dict:
    given = {}
    try:
        p = pathlib.Path(run_dir) / "fabric_options.json"
        if p.is_file():
            given.update(json.loads(p.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        pass
    given.update(opts or {})
    return options(given)


def _smoothstep(x):
    x = np.clip(x, 0.0, 1.0)
    return x * x * (3 - 2 * x)


def _seg(p, a, b):
    ab = b - a
    t = np.clip(((p - a) @ ab) / max(float(ab @ ab), 1e-12), 0, 1)
    return np.linalg.norm(p - (a + t[:, None] * ab), axis=1)


# ------------------------------------------------------------------------------------------------------- detection
def detect_skirt(b, sk, w, o, hair=None, prop=None):
    """The skirt / robe over both legs (the Rig path's midline test, 2026-10-12, continued down to the hem):
    -> {"mask", "top_y", "hem_y", "centre", "why"} or None."""
    ix = sk.index
    need = ("Hips", "LeftUpLeg", "LeftLeg", "LeftFoot", "RightUpLeg", "RightLeg", "RightFoot")
    if not all(n in ix for n in need):
        return None
    p, H = b.p, b.H
    J = {n: sk.head[ix[n]] for n in need}
    hip_y = float(min(J["LeftUpLeg"][1], J["RightUpLeg"][1]))
    foot_y = float(max(J["LeftFoot"][1], J["RightFoot"][1]))
    if hip_y - foot_y < 0.1 * H:
        return None
    lat = J["LeftUpLeg"] - J["RightUpLeg"]
    lat[1] = 0
    sep = float(np.linalg.norm(lat)) or 1e-6
    lat = lat / sep
    mid = (J["LeftUpLeg"] + J["RightUpLeg"]) / 2
    xs = (p - mid) @ lat
    top = w.argmax(1)
    dmin = np.full(len(p), np.inf)
    for side in ("Left", "Right"):
        for bn, nx in (("UpLeg", "Leg"), ("Leg", "Foot")):
            d = _seg(p, J[side + bn], J[side + nx])
            m = top == ix[side + bn]
            r = float(np.clip(np.median(d[m]), 0.01 * H, 0.15 * H)) if m.sum() >= 10 else 0.05 * H
            dmin = np.minimum(dmin, d / r)
    body = np.asarray(b.body, bool)
    band = body & (p[:, 1] < hip_y - 0.02 * H) & (p[:, 1] > foot_y - 0.02 * H)
    if hair is not None:
        band &= ~hair
    if prop is not None:
        band &= ~prop
    midline = band & (np.abs(xs) < 0.2 * sep) & (dmin > 1.0)
    step = 0.01 * H
    rows = np.floor((p[:, 1] - foot_y) / step).astype(np.int64)
    nrows = int(np.ceil((hip_y - foot_y) / step))
    if midline.sum() < 20:
        return {"why": "the legs have a gap between them (no skirt)", "mask": None}
    cov = np.unique(rows[midline])
    cov = cov[(cov >= 0) & (cov < nrows)]
    # the skirt spans the rows from the hips down to the lowest row still covered without a gap longer than 3 rows
    covered = np.zeros(nrows, bool)
    covered[cov] = True
    hem_row = nrows - 1
    gap = 0
    for r_ in range(nrows - 1, -1, -1):
        if covered[r_]:
            gap = 0
            hem_row = r_
        else:
            gap += 1
            if gap > 3 and r_ < nrows - 4:
                break
    span = nrows - hem_row
    share = float(covered[hem_row:].mean()) if span > 0 else 0.0
    if span < 8 or share < o["skirt_min_midline"]:
        return {"why": f"midline covers {share:.2f} of {span} rows", "mask": None}
    hem_y = foot_y + step * hem_row
    # the robe IS the lower body's surface from the hips to the hem (the leg radius measured on leg-dominated skin is
    # the robe's own radius, so a leg-tube test keeps the robe on the legs): every vertex of the covered rows is
    # skirt, except skin well inside its row's girth (a leg modelled under the robe) and anything far out sideways
    rr = np.full(len(p), np.inf)
    for side in ("Left", "Right"):
        for bn, nx in (("UpLeg", "Leg"), ("Leg", "Foot")):
            rr = np.minimum(rr, _seg(p, J[side + bn], J[side + nx]))
    inrows = band & (p[:, 1] >= hem_y - 0.02 * H) & (np.abs(xs) < 1.2 * sep + 0.15 * H)
    outer = np.zeros(len(p), bool)
    for r_ in np.unique(rows[inrows]):
        m = inrows & (rows == r_)
        if m.sum() < 4:
            continue
        lim = o["skirt_inner"] * float(np.percentile(rr[m], 90))
        outer[m] = rr[m] >= lim
    skirt = outer
    if skirt.sum() < o["skirt_min_vertices"]:
        return {"why": f"too few skirt vertices ({int(skirt.sum())})", "mask": None}
    return {"mask": skirt, "top_y": hip_y, "hem_y": float(p[skirt, 1].min()), "centre": mid, "parent": "Hips",
            "kind": "Skirt", "midline_share": round(share, 2), "rows": int(span)}


def detect_cape(b, sk, w, o, hair=None, prop=None, skirt=None):
    """A thin sheet hanging behind the back below the shoulders: skin behind the spine by cape_depth x the torso depth
    (the torso's own back is nearer the spine), between the shoulders and the hem. -> dict or None."""
    ix = sk.index
    if not all(n in ix for n in ("Hips", "Spine2", "Neck")):
        return None
    p, H = b.p, b.H
    body = np.asarray(b.body, bool)
    hips, sp2, neck = sk.head[ix["Hips"]], sk.head[ix["Spine2"]], sk.head[ix["Neck"]]
    torso = body & (p[:, 1] > hips[1]) & (p[:, 1] < neck[1]) & (np.abs(p[:, 0] - sp2[0]) < 0.15 * H)
    if torso.sum() < 50:
        return None
    # the back line: the torso's rearmost skin per height row, at its deepest
    rows = np.floor((p[:, 1] - hips[1]) / (0.02 * H)).astype(np.int64)
    back = {}
    for r_ in np.unique(rows[torso]):
        m = torso & (rows == r_)
        back[r_] = float(np.percentile(p[m, 2], 15))     # the back surface (forward is +z)
    depth = float(np.ptp(p[torso, 2]))
    zb = np.array([back.get(r_, np.nan) for r_ in rows])
    cand = body & (p[:, 1] < neck[1]) & np.isfinite(zb) & (p[:, 2] < zb - o["cape_depth"] * depth)
    cand |= body & (p[:, 1] <= hips[1]) & (p[:, 2] < float(np.nanmin(list(back.values()))) - o["cape_depth"] * depth) \
        & (np.abs(p[:, 0] - sp2[0]) < 0.5 * H)
    for m_ in (hair, prop, skirt):
        if m_ is not None:
            cand &= ~m_
    top = w.argmax(1)
    limb = np.array([any(k in nm for k in ("Arm", "Hand", "Leg", "Foot", "Toe")) for nm in sk.names])
    cand &= ~limb[top]                                   # an arm hanging behind the back is not a cape
    if cand.sum() < o["cape_min_vertices"]:
        return None
    return {"mask": cand, "top_y": float(min(neck[1], p[cand, 1].max())), "hem_y": float(p[cand, 1].min()),
            "centre": np.array([sp2[0], sp2[1], float(np.median(p[cand, 2]))]), "parent": "Spine2", "kind": "Cape"}


def detect_hair(b, sk, w, o, hair=None, taken=None):
    """Long loose hair below the neck that has no braid chain yet: sector chains from Head. -> dict or None."""
    ix = sk.index
    if hair is None or not hair.any() or "Neck" not in ix or "Head" not in ix:
        return None
    p, H = b.p, b.H
    ny = sk.head[ix["Neck"]][1]
    low = hair & (p[:, 1] < ny - 0.03 * H)
    if taken is not None:
        low &= ~taken
    if low.sum() < o["hair_min_vertices"]:
        return None
    head = sk.head[ix["Head"]]
    return {"mask": low, "top_y": float(ny), "hem_y": float(p[low, 1].min()),
            "centre": np.array([head[0], ny, head[2]]), "parent": "Head", "kind": "Hair_F"}


# ---------------------------------------------------------------------------------------------------------- chains
def _sectors(p, mask, centre, o):
    """Sector count from the hem's girth, sector centre angles. -> (n, angles)."""
    q = p[mask]
    low = q[q[:, 1] <= np.percentile(q[:, 1], 15)]
    girth = 2 * np.pi * float(np.median(np.linalg.norm((low - centre)[:, [0, 2]], axis=1))) if len(low) else 0.0
    n = int(np.clip(round(girth / (o["sector_girth_pct_H"] / 100.0 * 1.0)), o["sectors_min"], o["sectors_max"]))
    return n, np.linspace(-np.pi, np.pi, n, endpoint=False) + np.pi / n


def build(piece, b, sk, w, src, o, H):
    """Chains for one fabric piece; the piece's vertices take them. -> (w, chains, mask) ."""
    p = b.p
    mask = piece["mask"]
    idx = np.nonzero(mask)[0]
    centre = np.asarray(piece["centre"], float)
    top_y, hem_y = float(piece["top_y"]), float(piece["hem_y"])
    height = max(top_y - hem_y, 1e-6)
    n_sec, angles = _sectors(p, mask, centre, o)
    nb = int(np.clip(round(height / (o["bone_len_pct_H"] / 100.0 * H)), 2, 4))
    q = p[idx]
    rel = q - centre
    ang = np.arctan2(rel[:, 0], -rel[:, 2])              # 0 = behind (-z), +-pi = in front; sides at +-pi/2
    rad = np.linalg.norm(rel[:, [0, 2]], axis=1)
    h = np.clip((top_y - q[:, 1]) / height, 0.0, 1.0)   # 0 at the top, 1 at the hem
    chains = []
    first = len(sk.names)
    n0 = w.shape[1]
    names_all = []
    for k, a in enumerate(angles):
        da = np.abs(np.angle(np.exp(1j * (ang - a))))
        mine = da < np.pi / n_sec
        if mine.sum() < 4:
            mine = da < 2 * np.pi / n_sec
        pts = []
        for f in np.linspace(0.0, 1.0, nb + 1):
            sl = mine & (np.abs(h - f) < max(0.6 / nb, 0.08))
            r_ = float(np.median(rad[sl])) if sl.sum() >= 3 else (float(np.median(rad[mine])) if mine.any() else 0.1 * H)
            pts.append(centre + np.array([np.sin(a) * r_, top_y - f * height - centre[1], -np.cos(a) * r_]))
        pts = np.array(pts)
        prefix = f"{piece['kind']}{k + 1}"
        names = [prefix if j == 0 else f"{prefix}_{j}" for j in range(nb)]
        for j, nm in enumerate(names):
            sk.add(nm, piece["parent"] if j == 0 else names[j - 1], pts[j], pts[j + 1])
            src[nm] = "fabric"
        names_all.append(names)
        chains.append({"name": prefix, "bones": names, "parent": piece["parent"], "sector_deg": round(float(np.degrees(a)), 1),
                       "root": pts[0].tolist(), "tip": pts[-1].tolist()})
    w = np.concatenate([w, np.zeros((len(w), n_sec * nb))], 1)
    # the piece's weights: the two nearest sectors by azimuth, the two nearest bones by height
    W = np.zeros((len(idx), w.shape[1]))
    d_all = np.stack([np.abs(np.angle(np.exp(1j * (ang - a)))) for a in angles], 1)
    order = np.argsort(d_all, 1)[:, :2]
    d0, d1 = d_all[np.arange(len(idx)), order[:, 0]], d_all[np.arange(len(idx)), order[:, 1]]
    s1 = np.clip(d0 / np.maximum(d0 + d1, 1e-9), 0.0, 0.5)      # 0.5 on a sector border
    mids = (np.arange(nb) + 0.5) / nb
    kb = np.clip(np.searchsorted(mids, h) - 1, 0, nb - 2)
    t = _smoothstep((h - mids[kb]) / (mids[kb + 1] - mids[kb]))
    for which, share in ((order[:, 0], 1 - s1), (order[:, 1], s1)):
        col0 = first + which * nb + kb
        W[np.arange(len(idx)), col0] += share * (1 - t)
        W[np.arange(len(idx)), col0 + 1] += share * t
    base = np.zeros_like(W)
    base[:, :n0] = w[idx, :n0]
    bs = base.sum(1, keepdims=True)
    base = np.where(bs > 1e-6, base / np.maximum(bs, 1e-12), 0)
    rb = _smoothstep((h - o["root_skip"]) / max(o["root_blend"], 1e-6))[:, None]
    w[idx] = (1 - rb) * base + rb * W
    return w, chains, mask


def apply(run_dir, b, sk, w, Rc, plan="biped", opts=None, hair=None, prop=None, taken=None):
    if plan != "biped":
        return None
    o = options_for(run_dir, opts)
    if o["fabric"] == "off":
        return None
    H = b.H
    pieces, info = [], {}
    sk_piece = detect_skirt(b, sk, w, o, hair, prop) if o["skirt"] == "on" else None
    if sk_piece:
        info["skirt"] = {k: v for k, v in sk_piece.items() if k not in ("mask", "centre")}
        if sk_piece.get("mask") is not None:
            pieces.append(sk_piece)
    cp = detect_cape(b, sk, w, o, hair, prop, sk_piece.get("mask") if sk_piece else None) if o["cape"] == "on" else None
    if cp:
        info["cape"] = {"vertices": int(cp["mask"].sum())}
        pieces.append(cp)
    hp = detect_hair(b, sk, w, o, hair, taken) if o["hair"] == "on" else None
    if hp:
        info["hair"] = {"vertices": int(hp["mask"].sum())}
        pieces.append(hp)
    if not pieces:
        return {"w": w, "mask": np.zeros(b.g.n, bool), "chains": [], "stats": {"pieces": [], "detect": info}}
    n0 = len(sk.names)
    chains_all = []
    mask_all = np.zeros(b.g.n, bool)
    for piece in pieces:
        w, chains, mask = build(piece, b, sk, w, b.src, o, H)
        for c in chains:
            c["kind"] = piece["kind"]
        chains_all += chains
        mask_all |= mask
        info.setdefault("pieces", []).append({"kind": piece["kind"], "vertices": int(mask.sum()), "chains": len(chains),
                                              "top_pct_H": round((piece["top_y"] - float(b.p[:, 1].min())) / H * 100, 1),
                                              "hem_pct_H": round((piece["hem_y"] - float(b.p[:, 1].min())) / H * 100, 1)})
    return {"w": w, "mask": mask_all, "chains": chains_all, "options": o,
            "stats": {"pieces": info.get("pieces", []), "detect": {k: v for k, v in info.items() if k != "pieces"},
                      "bones_added": len(sk.names) - n0, "chains": chains_all}}


# ------------------------------------------------------------------------------------------------------- animation
_RX = re.compile(r"^(Skirt|Cape|Hair_F)(\d+)(?:_(\d+))?$")


def _bones(names):
    out = []
    for nm in names:
        m = _RX.match(nm)
        if m:
            out.append((nm, m.group(1), int(m.group(2)), int(m.group(3) or 0)))
    return out


def rig_check_extend(anim, sk, Rc, opts=None):
    from . import fastrig as F
    times, channels = anim
    o = options(opts)
    ph = 2 * np.pi * times / float(times[-1] or 1)
    X = Rc.T @ np.eye(3)[0]
    for nm, kind, k, seg in _bones(sk.names):
        deg = o["sway_deg"] * (0.4 + 0.6 * seg / 3.0) * np.sin(2 * ph - o["sway_lag"] * seg - 0.3 * k)
        vals = np.stack([F.quat(X, float(d)) for d in deg])
        channels.append((sk.index[nm], "rotation", vals))
    return times, channels


def extend_clips(clips: list, doc: dict, run_dir=None, opts=None) -> list:
    names = [b_["name"] for b_ in doc.get("bones") or []]
    bones = _bones(names)
    if not bones:
        return clips
    o = options_for(run_dir, opts) if run_dir else options(opts)
    from . import fastrig as F
    Rc = F.yaw_matrix(F.FORWARD_YAW.get(doc.get("forward_axis") or "+z", 0.0))
    X = Rc.T @ np.eye(3)[0]
    loco = re.compile(r"walk|run|jog|sprint|jump|hop|dance|trot|gallop", re.I)
    for c in clips:
        times = np.asarray(c["times"], np.float64)
        hz = 1.4 if loco.search(c.get("name") or "") else 0.4
        amp = o["sway_deg"] * (1.0 if loco.search(c.get("name") or "") else 0.5)
        for nm, kind, k, seg in bones:
            deg = amp * (0.4 + 0.6 * seg / 3.0) * np.sin(2 * np.pi * hz * times - o["sway_lag"] * seg - 0.3 * k)
            h = np.radians(deg) / 2
            c["rotations"][nm] = np.stack([X[0] * np.sin(h), X[1] * np.sin(h), X[2] * np.sin(h), np.cos(h)], 1)
    return clips


def summary(stats) -> dict | None:
    if not stats:
        return None
    return {"pieces": stats.get("pieces"), "bones_added": stats.get("bones_added"),
            "chains": [c["name"] for c in stats.get("chains") or []], "detect": stats.get("detect"),
            "naming": "Skirt{k} > Skirt{k}_1 .. from Hips, Cape{k} from Spine2, Hair_F{k} from Head; k = sector around the piece"}
