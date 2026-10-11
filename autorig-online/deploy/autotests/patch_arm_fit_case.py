"""Task 432f7bf3 «Сигниферида-7» (owner 2026-10-11: «руки неверно вообще подобрались»): the regression case for arm
chains fitted to skirt flaps while the real arms hang along the torso; plus the limb_in_body.left family (eaec2b0b,
18526c63): one arm fitted into the torso / above the shoulder while the other arm is right.

    sudo python3 patch_arm_fit_case.py /srv/autorig/autotests

checks.py    kind `arm_fit` (on mt_rig): the Arm (shoulder) joint heights in % H, their left / right difference, the
             hands' lateral reach, the elbow above the shoulder, the fit sources (proportions / mirror / suspect)
corpus.json  input 432f7bf3.upload.glb (the V3 run's proj/model.glb, 603 935 vertices, a frilly skirt, arms hanging),
             case `skirt_flaps_arms` (extended tier: the mesh is heavy), and `cyborg_one_arm` on eaec2b0b (base)
"""
import json
import pathlib
import sys

CHECK_FN = '''

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
    out["proportions_sources"] = sum(1 for s in srcs if s == "proportions")
    out["mirror_sources"] = sum(1 for s in srcs if s == "mirror")
    out["suspect_sources"] = sum(1 for s in srcs if s == "suspect")
    return out
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "arm_fit":
        return arm_fit(ctx)
    if k == "pending_detector":
        return {"pending": True}
'''

INPUTS = {
    "432f7bf3.upload.glb": {
        "source": "/srv/autorig/data/motion_transfer/runs/1fa6c2006b1daf2bf962/proj/model.glb",
        "task": "432f7bf3-d372-4e92-9a26-782028557132",
        "what": "«Сигниферида-7»: a woman with arms hanging at her sides and a frilly skirt whose flaps stick out "
                "sideways at hip height; 603 935 vertices, no skeleton (owner 2026-10-11: the arm chains were "
                "fitted to the skirt flaps, the real arms got no bones)",
    },
    "eaec2b0b.upload.glb": {
        "source": "/srv/autorig/data/motion_transfer/runs/eaec2b0b638e840bfb1f/proj/model.glb",
        "task": "bdc37cc9-23df-4d48-81f4-af62a669b421",
        "what": "Cyborg (triage limb_in_body.left): a posed bare mesh whose LeftArm was fitted at hip height "
                "(y 0.17 H, x -0.03) with 89 757 dominant vertices while the RightArm was right",
    },
}

CASES = [
    {
        "id": "skirt_flaps_arms",
        "task": "432f7bf3-d372-4e92-9a26-782028557132",
        "tier": "extended",
        "cost": 8,
        "defect": "owner 2026-10-11 «руки неверно вообще подобрались по какой то причине, хотя они очевидны»: the "
                  "arm reach took the skirt flaps (a sheet sticking out sideways at hip height) for arms; "
                  "pose_skeleton traced the flap chains off the hips as bent arms and stage_traced turned them to T",
        "checks": [
            {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "tier": "extended", "input": "432f7bf3.upload.glb",
             "params": {"words": "woman standing arms at her sides frilly skirt"},
             "thresholds": {"seconds": ["<=", 240], "bones": [">=", 20], "clips": [">=", 8]}},
            {"id": "mt_arms", "kind": "arm_fit", "paths": ["mt"], "tier": "extended", "on": "mt_rig",
             "thresholds": {"arms_found": ["==", 2], "arm_y_min_pct_H": [">=", 60], "arm_y_diff_pct_H": ["<=", 8],
                            "hand_lateral_max_pct_H": ["<=", 22], "elbow_above_shoulder": ["==", 0]}},
        ],
    },
    {
        "id": "cyborg_one_arm",
        "task": "bdc37cc9-23df-4d48-81f4-af62a669b421",
        "tier": "extended",
        "cost": 5,
        "defect": "triage limb_in_body.left (17 tasks, 2026-10-11): on a posed bare mesh fastrig fitted ONE arm "
                  "into the torso (LeftArm at hip height) while the other arm was right; the degenerate side is "
                  "now mirrored from the good one and the limbs are traced on the voxel solid",
        "checks": [
            {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "tier": "extended", "input": "eaec2b0b.upload.glb",
             "params": {"words": "cyborg character"},
             "thresholds": {"seconds": ["<=", 300], "bones": [">=", 20], "clips": [">=", 8]}},
            {"id": "mt_arms", "kind": "arm_fit", "paths": ["mt"], "tier": "extended", "on": "mt_rig",
             "thresholds": {"arms_found": ["==", 2], "arm_y_min_pct_H": [">=", 55], "arm_y_diff_pct_H": ["<=", 10],
                            "elbow_above_shoulder": ["==", 0]}},
        ],
    },
]


def _write(path, text):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def main(root):
    root = pathlib.Path(root)
    cp = root / "checks.py"
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def arm_fit(ctx):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: arm_fit")
    if 'if k == "arm_fit":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: dispatch anchor missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    jp = root / "corpus.json"
    doc = json.loads(jp.read_text(encoding="utf-8"))
    c2 = []
    for name, spec in INPUTS.items():
        if name not in doc["inputs"]:
            doc["inputs"][name] = spec
            c2.append(f"input {name}")
    ids = [c["id"] for c in doc["cases"]]
    for case in CASES:
        if case["id"] not in ids:
            doc["cases"].append(case)
            c2.append(f"case {case['id']}")
    if c2:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    changed += c2
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
