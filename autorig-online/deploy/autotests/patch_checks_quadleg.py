"""checks.py: the `quad_leg_trunk` check kind (Fable · monsters/animals, 2026-10-13): on a quadruped MT rig, the
vertices of each leg's shin / paw (below the knee joint, inside the shin tube) whose dominant bone is not that leg's
(trunk, head, tail, prop, another leg).  The cat b94999ad had its near front leg on Hips / Spine / a Prop, the boar
fec0161f its hind legs on Tail3.  python patch_checks_quadleg.py <autotests dir>"""
import pathlib
import sys

p = pathlib.Path(sys.argv[1]) / "checks.py"
s = p.read_text(encoding="utf-8")
if "def quad_leg_trunk" in s:
    print("checks: already patched")
    sys.exit(0)
old = '''def quadruped_legs(path, rig_json=None):
'''
new = '''def quad_leg_trunk(path, rig_json, tube_radii=1.5):
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
'''
assert s.count(old) == 1, "quadruped_legs anchor"
s = s.replace(old, new)
old = '''    if k == "quadruped_legs":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quadruped_legs(target, rig_json)
'''
new = '''    if k == "quadruped_legs":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quadruped_legs(target, rig_json)
    if k == "quad_leg_trunk":                            # Fable · monsters/animals: the shin / paw on its own leg
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quad_leg_trunk(target, rig_json, float(a.get("tube_radii", 1.5)))
'''
assert s.count(old) == 1, "dispatch anchor"
s = s.replace(old, new)
p.write_text(s, encoding="utf-8")
assert p.read_bytes().count(bytes([8])) == 0
print("checks: quad_leg_trunk")
