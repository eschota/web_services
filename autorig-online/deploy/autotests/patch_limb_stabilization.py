"""Limb stabilization · V3 (2026-10-10): the regression case for a posed input and its check kind in the autotests.

Anchored, idempotent text edits on files other agents edit (re-read production, then run; corpus.json keeps its
hand-written layout: one input per line, one check per line):

    sudo python3 patch_limb_stabilization.py /srv/autorig/autotests      # checks.py + corpus.json in place

checks.py    kind `pose_stabilization` (on mt_rig): what mt/limb_stabilize.py did on the run (source skin found, pose
             state before / after, angles after, the fit check «skeleton not fitted») -> metrics for thresholds
corpus.json  inputs d1522b45.upload.glb / .upload.fbx / .converter.glb; case `posed_fox_warrior`; a control check on
             `warrior_sword` (a near-T rigged input must not be stabilized)
"""
import json
import pathlib
import sys

CHECK_FN = '''

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
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "pose_stabilization":
        return pose_stabilization(ctx)
    if k == "pending_detector":
        return {"pending": True}
'''

INPUT_LINES = [
    '  "d1522b45.upload.glb": {"source": "/srv/autorig/audits/limb-stab-20261010/d1522b45.assimp.glb", "task": "d1522b45-989b-4da0-a382-a6c73167d1b0", "what": "fox warrior, both arms over the head on the weapon, right knee raised: the customer\'s Tripo FBX (its own 67-joint skin, no clips) as the V3 intake normalizes it (assimp glb2)"}',
    '  "d1522b45.upload.fbx": {"source": "/srv/autorig/data/var/uploads/fbe83331-7107-4736-9589-eaee053a6d38/tripo_convert_01a47a3c-581a-48a0-b5d2-952319eb1e2f.fbx", "task": "d1522b45-989b-4da0-a382-a6c73167d1b0", "what": "the upload itself (binary FBX: Tripo rig + skin + bind pose, posed mesh)"}',
    '  "d1522b45.converter.glb": {"source": "/srv/autorig/data/static/glb_cache/d1522b45-989b-4da0-a382-a6c73167d1b0_animations.glb", "task": "d1522b45-989b-4da0-a382-a6c73167d1b0", "what": "converter f7 rig (1060 s): a T-pose skeleton laid over the posed mesh, arms not bound (0 forearm / hand vertices)"}',
]

CASE_BLOCK = '''  {"id": "posed_fox_warrior", "task": "d1522b45-989b-4da0-a382-a6c73167d1b0", "cost": 3,
   "defect": "a posed input (both arms over the head holding the weapon, right knee raised; a Tripo FBX with its own skin): the classic converter laid a T-pose template over the posed mesh (arms not bound), the fast rig fitted hanging arms. Limb stabilization · V3 turns the source skin's chains to T before the rig (mt/limb_stabilize.py)",
   "converter": {"input_url": "https://autorig.online/u/fbe83331-7107-4736-9589-eaee053a6d38/tripo_convert_01a47a3c-581a-48a0-b5d2-952319eb1e2f.fbx", "words": "fantasy warrior raised weapon",
                 "xfail": {"arms_bound": {"reason": "the classic converter does not stabilize a posed input (the owner cancelled the classic pipeline for new tasks)", "owner": "Limb stabilization · V3"},
                           "limbs": {"reason": "same: the arms are not bound to their bones", "owner": "Limb stabilization · V3"}}},
   "checks": [
    {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "d1522b45.upload.glb", "params": {"words": "fantasy warrior raised weapon"}, "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20], "clips": [">=", 8]}},
    {"id": "mt_stab", "kind": "pose_stabilization", "paths": ["mt"], "on": "mt_rig", "thresholds": {"source_skin": ["==", 1], "posed_before": ["==", 1], "applied": ["==", 1], "arm_elevation_after_abs_max": ["<=", 5], "elbow_bend_after_max": ["<=", 5], "knee_bend_after_max": ["<=", 5], "fit_ok": ["==", 1], "top2_share": ["<=", 0.6], "min_limb_share": [">=", 0.002], "stabilize_s": ["<=", 10]}},
    {"id": "mt_fit", "kind": "skeleton_fit", "paths": ["mt"], "on": "mt_rig", "thresholds": {"joints_outside_bbox": ["<=", 0]}},
    {"id": "mt_limbs", "kind": "limb_collision", "paths": ["mt"], "on": "mt_rig", "thresholds": {"min_arm_vertices": [">=", 1], "severity_rank": ["<=", 1]}},
    {"id": "conv_arms_detector", "kind": "limb_collision", "role": "detector", "paths": ["mt", "converter"], "input": "d1522b45.converter.glb", "thresholds": {"min_arm_vertices": ["==", 0]}}
   ]}'''

CONTROL_LINE = ('    {"id": "mt_stab_control", "kind": "pose_stabilization", "role": "control", "paths": ["mt"], "on": "mt_rig", '
                '"thresholds": {"source_skin": ["==", 1], "applied": ["==", 0], "fit_ok": ["==", 1]}}')


def _write(path: pathlib.Path, text: str):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def patch_checks(cp: pathlib.Path):
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def pose_stabilization(ctx):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: the MT pipeline steps anchor is missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: pose_stabilization")
    if 'if k == "pose_stabilization":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: the pending_detector dispatch anchor is missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    return changed


def patch_corpus(jp: pathlib.Path):
    lines = jp.read_text(encoding="utf-8").split("\n")
    changed = []
    # inputs: before the line " }," that closes "inputs": {
    if not any(l.startswith('  "d1522b45.upload.glb"') for l in lines):
        start = lines.index(' "inputs": {')
        end = next(i for i in range(start + 1, len(lines)) if lines[i] == " },")
        if not lines[end - 1].rstrip().endswith(","):
            lines[end - 1] = lines[end - 1].rstrip() + ","
        lines[end:end] = [l + "," for l in INPUT_LINES[:-1]] + [INPUT_LINES[-1]]
        changed.append("inputs d1522b45")
    # the case: before the " ]" that closes "cases": [
    if not any(l.startswith('  {"id": "posed_fox_warrior"') for l in lines):
        start = lines.index(' "cases": [')
        end = next(i for i in range(start + 1, len(lines)) if lines[i] == " ]")
        if lines[end - 1] == "   ]}":
            lines[end - 1] = "   ]},"
        elif not lines[end - 1].rstrip().endswith(","):
            raise SystemExit(f"corpus.json: unexpected line before the cases' end: {lines[end - 1]!r}")
        lines[end:end] = CASE_BLOCK.split("\n")
        changed.append("case posed_fox_warrior")
    # the control check on warrior_sword: before its "   ]}," line
    if not any('"id": "mt_stab_control"' in l for l in lines):
        start = next(i for i, l in enumerate(lines) if l.startswith('  {"id": "warrior_sword"'))
        end = next(i for i in range(start + 1, len(lines)) if lines[i] in ("   ]},", "   ]}"))
        if not lines[end - 1].rstrip().endswith(","):
            lines[end - 1] = lines[end - 1].rstrip() + ","
        lines[end:end] = [CONTROL_LINE]
        changed.append("warrior_sword: control check")
    if changed:
        text = "\n".join(lines)
        json.loads(text)                                  # still valid JSON, or nothing is written
        _write(jp, text)
    return changed


def patch_corpus_json(jp: pathlib.Path):
    """After `autotests.py sync --write-manifest` the file is json.dumps(indent=1): edit it as a document."""
    doc = json.loads(jp.read_text(encoding="utf-8"))
    changed = []
    for line in INPUT_LINES:
        name, item = json.loads("{" + line + "}").popitem()
        if name not in doc["inputs"]:
            doc["inputs"][name] = item
            changed.append(f"input {name}")
    case = json.loads(CASE_BLOCK)
    if case["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(case)
        changed.append(f"case {case['id']}")
    control = json.loads(CONTROL_LINE)
    for c in doc["cases"]:
        if c["id"] == "warrior_sword" and not any(ch.get("id") == control["id"] for ch in c["checks"]):
            c["checks"].append(control)
            changed.append("warrior_sword: control check")
    if changed:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    return changed


def main(root):
    root = pathlib.Path(root)
    jp = root / "corpus.json"
    lines = jp.read_text(encoding="utf-8").split("\n")
    first = lines[lines.index(' "inputs": {') + 1] if ' "inputs": {' in lines else ""
    hand_written = '"source":' in first                  # one input per line; json.dumps(indent=1) puts it below
    changed = patch_checks(root / "checks.py") + (patch_corpus(jp) if hand_written else patch_corpus_json(jp))
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
