"""Pre-rig tessellation (owner 2026-10-11, the chibi dd498832): the regression case and its check kind.

    sudo python3 patch_tessellation_case.py /srv/autorig/autotests

checks.py    kind `tessellation` (on mt_rig): what mt/tessellate.py decided (applied, faces before / after, factor,
             the edge statistics) and the crumpling of the rig on its own rig_check clip (faces whose area collapses
             under 20 % or grows over 5x, edges over 2x / 4x, over 8 frames) -> thresholds
corpus.json  input dd498832.upload.glb (the V3 run's proj/model.glb, 580 triangles), case `lowpoly_chibi`; controls on
             boy_tshirt and knight_rigpath (dense models are not tessellated)
"""
import json
import pathlib
import sys

CHECK_FN = '''

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
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "tessellation":
        return tessellation(ctx)
    if k == "pending_detector":
        return {"pending": True}
'''

INPUT = ("dd498832.upload.glb", {
    "source": "/srv/autorig/data/motion_transfer/runs/2fd6423ac6aa115b0e97/proj/model.glb",
    "task": "dd498832-09d2-48ca-b7ca-ab546fb02c83",
    "what": "«Пиксельный Чибик»: a 580-triangle chibi, edges up to 31 % of the height (median 8.5 %); huge triangles span the shoulders and hips and crumple in the clips (owner 2026-10-11)"})

CASE = {
    "id": "lowpoly_chibi",
    "task": "dd498832-09d2-48ca-b7ca-ab546fb02c83",
    "cost": 1,
    "defect": "low-poly needs tessellation (owner 2026-10-11): 580 triangles, edges up to 31 % of the height; the huge triangles at the shoulders and hips take one weight per corner and crumple / tear in the clips. Pre-rig tessellation (mt/tessellate.py) splits the long triangles near the joints (580 -> ~1700 faces, UVs kept), the rig runs on that mesh; v0: 5 collapsed faces and 20 edges over 4x on rig_check, after: 1 / 0",
    "checks": [
        {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "dd498832.upload.glb",
         "params": {"words": "pixel chibi"}, "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20], "clips": [">=", 8]}},
        {"id": "mt_tess", "kind": "tessellation", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"applied": ["==", 1], "factor": ["<=", 4.0], "edge_max_pct_H_after": ["<=", 20],
                        "clip_collapsed_max": ["<=", 2], "clip_edges_4x_max": ["<=", 2], "clip_edges_2x_max": ["<=", 60],
                        "original_topology": ["==", 1]}},
        {"id": "mt_stretch", "kind": "rig_stretch", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"sum_edges_over_4x": ["<=", 3], "sum_edges_over_2x": ["<=", 60], "empty_limb_bones": ["==", 0]}},
    ],
}

CONTROLS = {"boy_tshirt": {"applied": ["==", 0]}, "knight_rigpath": {"applied": ["==", 0]}}


def _write(path, text):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def main(root):
    root = pathlib.Path(root)
    cp = root / "checks.py"
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def tessellation(ctx):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: tessellation")
    if 'if k == "tessellation":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: dispatch anchor missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    jp = root / "corpus.json"
    doc = json.loads(jp.read_text(encoding="utf-8"))
    c2 = []
    if INPUT[0] not in doc["inputs"]:
        doc["inputs"][INPUT[0]] = INPUT[1]
        c2.append("input dd498832")
    if CASE["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(CASE)
        c2.append("case lowpoly_chibi")
    for case in doc["cases"]:
        th = CONTROLS.get(case["id"])
        if th and any(ch.get("id") == "mt_rig" for ch in case["checks"]) and not any(ch.get("id") == "mt_tess_control" for ch in case["checks"]):
            case["checks"].append({"id": "mt_tess_control", "kind": "tessellation", "role": "control", "paths": ["mt"],
                                   "on": "mt_rig", "thresholds": th})
            c2.append(f"{case['id']}: control")
    if c2:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    changed += c2
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
