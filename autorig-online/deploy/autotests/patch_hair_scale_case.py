"""Task 137bd37f «Кайо-Бей» (owner 2026-10-11): the regression case for «hair to the arms» and a small absolute scale.

    sudo python3 patch_hair_scale_case.py /srv/autorig/autotests

checks.py    kind `hair_limbs` (on mt_rig): vertices whose dominant bone is an arm bone but that sit above the arm
             joints (dreadlock tips at the shoulders), the arm-bone counts per side, and the model's absolute size
             facts (height in source units, the unit guess); the rig path is scale-invariant, proved on this model at
             0.01x / 1x / 100x (identical rig_check numbers)
corpus.json  input 137bd37f.upload.glb (the V3 run's proj/model.glb, 22.5 cm tall, one welded shell, dreadlocks), case
             `chibi_dreads_scale`
"""
import json
import pathlib
import sys

CHECK_FN = '''

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
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "hair_limbs":
        return hair_limbs(ctx)
    if k == "pending_detector":
        return {"pending": True}
'''

INPUT = ("137bd37f.upload.glb", {
    "source": "/srv/autorig/data/motion_transfer/runs/bcb7177166ea9b28f007/proj/model.glb",
    "task": "137bd37f-09d7-49c8-b780-01c1d30d406b",
    "what": "«Кайо-Бей»: a chibi with dreadlocks, sunglasses and a T-shirt, 22.5 units (cm) tall, one welded shell of 24 357 vertices, no skeleton (owner 2026-10-11: dreads to the arms, a poor palm, too small in the cathedral)"})

CASE = {
    "id": "chibi_dreads_scale",
    "task": "137bd37f-09d7-49c8-b780-01c1d30d406b",
    "cost": 2,
    "defect": "owner 2026-10-11: «дреды сзади на голове прицепил к рукам, ладонь плохо заригана с пальцами, и масштаб слишком мелкий для человека в соборе». Facts: the dreads are Neck / Head skin (3 512 Neck, 17 188 Head of 24 357), 75 dread-tip vertices beside the shoulders are arm skin; the rig is scale-invariant (identical rig_check at 0.01x / 1x / 100x); the 0.28 m card height came from the real-scale card (a toy), the scale check now shows it at 1.5 m (adult_human, conf 0.92). Open: finger chains (handrig) and the dread tips",
    "checks": [
        {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "137bd37f.upload.glb",
         "params": {"words": "stylized human figure chibi"}, "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20], "clips": [">=", 8]}},
        {"id": "mt_hair", "kind": "hair_limbs", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"arm_vertices_above_joints": ["<=", 120], "neck_head_share": [">=", 0.6]},
         "target": {"thresholds": {"arm_vertices_above_joints": ["<=", 10]}, "reason": "dreadlock tips beside the shoulders still go to the clavicles / arms (75)", "owner": "Skinning · V3"}},
        {"id": "mt_stretch", "kind": "rig_stretch", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"sum_edges_over_4x": ["<=", 420], "sum_edges_over_2x": ["<=", 2100], "empty_limb_bones": ["==", 0]}},
    ],
}


def _write(path, text):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def main(root):
    root = pathlib.Path(root)
    cp = root / "checks.py"
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def hair_limbs(ctx):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: hair_limbs")
    if 'if k == "hair_limbs":' not in s:
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
        c2.append("input 137bd37f")
    if CASE["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(CASE)
        c2.append("case chibi_dreads_scale")
    if c2:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    changed += c2
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
