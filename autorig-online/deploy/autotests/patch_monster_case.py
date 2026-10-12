"""Monster rig · V3: the autotests check kind `monster_rig` (checks.py, anchor patch on the CURRENT file) and the corpus
case / input JSON files for corpus_edit.py (never a whole-file corpus write).

    python patch_monster_case.py <autotests dir> [xfail]     -> patches checks.py in place, writes
                                                              <dir>/monster_case.json and <dir>/monster_input.json
"""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
xfail = len(sys.argv) > 2 and sys.argv[2] == "xfail"

p = root / "checks.py"
s = p.read_text(encoding="utf-8")
if "def monster_rig(" not in s:
    anchor = "# ------------------------------------------------------------------------------------------------ dispatch\n"
    assert s.count(anchor) == 1
    func = '''def monster_rig(ctx):
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
            "crest_bones": sum(1 for n in names if re.fullmatch(r"(Left|Right)Crest\\d*", n)),
            "bones": len(names), "clips": len(rig.get("clips") or []),
            "unweighted": (rig.get("checks") or {}).get("unweighted_vertices"),
            "joints_outside": len((rig.get("checks") or {}).get("joints_outside_mesh") or []),
            "fit_ok": int(bool((rig.get("fit_check") or {}).get("ok"))),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "detect_s": mn.get("seconds"), "rig_total_s": (rig.get("timings_s") or {}).get("total")}


'''
    s = s.replace(anchor, func + anchor)
    d_anchor = '    if k == "hand_rig":\n        return hand_rig(ctx)\n'
    assert s.count(d_anchor) == 1
    s = s.replace(d_anchor, d_anchor + '    if k == "monster_rig":\n        return monster_rig(ctx)\n')
    if "\nimport re\n" not in s:
        s = s.replace("\nimport json\n", "\nimport json\nimport re\n", 1)
    p.write_text(s, encoding="utf-8", newline="\n")
    print("checks.py: monster_rig added")
else:
    print("checks.py: already has monster_rig")

inputs = {"35356012.model.glb": {
    "source": "/srv/autorig/data/motion_transfer/runs/b3423278918e78d9aebd/proj/model.glb",
    "task": "35356012", "what": "SCP-178-1: a crouched elongated humanoid creature, long arms to the knees, two long "
                                "ear tentacles (the V3 run's model.glb, OBJ upload normalized)"}}
case = {
    "id": "monster_crouched_scp",
    "task": "35356012",
    "cost": 2,
    "defect": "owner 2026-10-13 («такие ноги типовые для монстров»): task 35356012 (Vision «elongated humanoid creature») "
              "failed numeric QA and triaged arms_unbound.right: the biped fit put straight-leg joints on crouched legs, "
              "the long bent arms fell back to proportions (one forearm without vertices, a hand 19 % H off) and the "
              "library clips straightened the knees. Monster rig · V3 (mt/monster.py): the limbs traced on the voxel "
              "solid with thick-cell paths, legs typed by their bends (crouched: knee 54-59 deg), the traced arm and leg "
              "chains pinned as the rest pose (no T-turn), the clips bind-relative (the crouch stays under the motion), "
              "the ear tentacles as head-appendage chains ({Side}Crest..Crest4, mt/multilimb.py)",
    "checks": [
        {"id": "mt_rig_control", "kind": "mt_rig_first", "paths": ["mt"], "input": "66ba97ba.upload.glb",
         "params": {"words": "Female warrior with sword and armor"},
         "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20]}},
        {"id": "mt_monster_control", "kind": "monster_rig", "paths": ["mt"], "on": "mt_rig", "role": "control",
         "thresholds": {"use": ["==", 0], "crest_bones": ["==", 0], "policy": ["==", "aligned_reference"]}},
        {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "35356012.model.glb",
         "params": {"words": "elongated humanoid creature", "forward_axis": "+z", "v3_bound": True},
         "thresholds": {"seconds": ["<=", 60], "bones": [">=", 30], "clips": [">=", 8], "body_plan": ["==", "biped"]}},
        {"id": "mt_monster", "kind": "monster_rig", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"use": ["==", 1], "leg_type": ["==", "crouched"], "policy": ["==", "bind_relative"],
                        "pinned": [">=", 8], "arms_traced": [">=", 1], "knee_bend_min": [">=", 25],
                        "appendage_chains": [">=", 2], "crest_bones": [">=", 8], "unweighted": ["==", 0],
                        "stretch_4x": ["<=", 900]}},
        {"id": "mt_qa", "kind": "mt_numeric_qa", "paths": ["mt"], "on": "mt_rig",
         "params": {"clips": ["Idle", "Walking"], "cap": 2000},
         "thresholds": {"clips_passed": [">=", 2]}},
    ],
    "note": "control first (checks share one run dir). Local 2026-10-13 (p6 tree): 35 bones, numeric QA 5/8 (Idle, Walking, "
            "Waving1, Hip Hop, Punching pass; Running / Jump / Look Around 'extent' on the head|spine seam: the Head "
            "joint of a crouched trunk), limb_collision high (the hands beside the knees). Known gaps: the right arm "
            "fused with its knee in the voxel solid (fastrig's fit stands); the Head / Neck joints of a tilted trunk; "
            "a monster gait of its own (the crouch rides the Mixamo clips bind-relative).",
}
if xfail:
    for ch in case["checks"]:
        if ch["id"] in ("mt_rig", "mt_monster", "mt_monster_control", "mt_qa"):
            ch["xfail"] = {"reason": "mt/monster.py is not deployed yet (its gate run is pending); flips to pass in the "
                                     "deploy that installs it", "owner": "Fable · monsters/animals"}
(root / "monster_case.json").write_text(json.dumps(case, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
(root / "monster_input.json").write_text(json.dumps(inputs, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
print("corpus files written", "(xfail)" if xfail else "(pass)")
