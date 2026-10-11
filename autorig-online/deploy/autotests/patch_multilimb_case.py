"""Multi-limb rig · V3: the autotests check kind `multilimb_rig` + the `v3_bound` param of mt_rig_first (checks.py), the
corpus case `demon_four_arms_wings` and the warrior_sword_posed/mt_pose_fit case as XFAIL while Limb stabilization's
stage_traced is not installed (corpus.json). Applied on the PROD copies (argv[1] = directory with both files)."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])

# ---------------------------------------------------------------------------------------------------- checks.py
p = root / "checks.py"
s = p.read_text(encoding="utf-8")
if "def multilimb_rig(" not in s:
    anchor = "# ------------------------------------------------------------------------------------------------ dispatch\n"
    assert s.count(anchor) == 1
    func = '''def multilimb_rig(ctx):
    """«Multi-limb rig · V3» (mt/multilimb.py, 2026-10-11, task 0d39aaba: four arms and wings): the limbs the trace
    found on this case's MT run (arms, arm pairs, wings, tail), the extra chains fastrig added, their vertices, the
    cross-chain weight (0 by construction outside the seams), the library clips' channels on the extra bones and the
    rig_check stretch. On a plain biped (the control) every extra count is 0."""
    from mt import fastrig as F
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    ml = rig.get("multilimb") or {}
    counts = ml.get("counts") or {}
    mls = (rig.get("checks") or {}).get("multilimb") or {}
    chains = mls.get("chains") or []
    st = {k: v for k, v in ((rig.get("checks") or {}).get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    names = {b["name"] for b in rig.get("bones") or []}
    extra = sorted(n for n in names if re.fullmatch(r"(Left|Right)(Shoulder|Arm|ForeArm|Hand)\\d+|(Left|Right)Wing[A-Z]?\\d*|Tail\\d*", n))
    js, _ = F.read_glb((run / "rig" / "rigged.glb").read_bytes())
    node = {i: n.get("name") for i, n in enumerate(js.get("nodes") or [])}
    clip_extra = 0
    for a in js.get("animations") or []:
        if a.get("name") == "rig_check":
            continue
        clip_extra += sum(1 for c in a.get("channels") or [] if node.get(c["target"]["node"]) in extra)
    return {"arms": counts.get("arms", 0), "arm_pairs": counts.get("arm_pairs", 0), "wings": counts.get("wings", 0),
            "wing_pairs": counts.get("wing_pairs", 0), "tails": counts.get("tails", 0), "crests": counts.get("crests", 0),
            "use": int(bool(ml.get("use"))), "extra_chains": len(chains), "extra_bones": len(extra),
            "chains_without_vertices": sum(1 for c in chains if not c.get("vertices")),
            "chain_vertices": int(mls.get("vertices") or 0),
            "cross_chain_weight_after": float(mls.get("cross_chain_weight_after") or 0.0),
            "seam_edges": int(mls.get("seam_edges") or 0), "seam_split_faces": int(mls.get("seam_split_faces") or 0),
            "clip_channels_extra": clip_extra, "clips": len(rig.get("clips") or []),
            "unweighted": (rig.get("checks") or {}).get("unweighted_vertices"),
            "joints_outside": len((rig.get("checks") or {}).get("joints_outside_mesh") or []),
            "stretch_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
            "stretch_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
            "detect_s": ml.get("seconds"), "rig_total_s": (rig.get("timings_s") or {}).get("total")}


'''
    s = s.replace(anchor, func + anchor)
    d_anchor = '    if k == "hand_rig":\n        return hand_rig(ctx)\n'
    assert s.count(d_anchor) == 1
    s = s.replace(d_anchor, d_anchor + '    if k == "multilimb_rig":\n        return multilimb_rig(ctx)\n')
    f_anchor = '        return mt_rig_first(ctx, inp, a.get("words", ""), a.get("forward_axis", ""))\n'
    assert s.count(f_anchor) == 1
    s = s.replace(f_anchor, '        return mt_rig_first(ctx, inp, a.get("words", ""), a.get("forward_axis", ""), '
                            'a.get("v3_bound", False))\n')
    sig = 'def mt_rig_first(ctx, input_name, words="", forward_axis=""):\n'
    assert s.count(sig) == 1
    s = s.replace(sig, 'def mt_rig_first(ctx, input_name, words="", forward_axis="", v3_bound=False):\n')
    b_anchor = ('    p, wall = _child([PY, "-P", "-m", "mt.rig_first", "--dir", str(run), "--glb", str(_input(input_name)),\n'
                '                      "--words", words], timeout=300)\n')
    assert s.count(b_anchor) == 1
    s = s.replace(b_anchor, (
        '    if v3_bound:                                         # Multi-limb rig · V3: like a hash-bound V3 source, the up\n'
        '        (run / "v3").mkdir(parents=True, exist_ok=True)     # stage of rig_first leaves the model as uploaded\n'
        '        (run / "v3" / "source.json").write_text(json.dumps({"schema": "autorig.v3.session-source/1", '
        '"source": "autotests"}))\n') + b_anchor)
    if "\nimport re\n" not in s:
        s = s.replace("\nimport json\n", "\nimport json\nimport re\n", 1)
    p.write_text(s, encoding="utf-8", newline="\n")
    print("checks.py: multilimb_rig + v3_bound added")
else:
    print("checks.py: already has multilimb_rig")

# --------------------------------------------------------------------------------------------------- corpus.json
p = root / "corpus.json"
doc = json.loads(p.read_text(encoding="utf-8"))
changed = False
if "0d39aaba.upload.glb" not in doc["inputs"]:
    doc["inputs"]["0d39aaba.upload.glb"] = {
        "source": "/srv/autorig/data/var/uploads/fbbcd866-b414-4907-aa13-0f4621994d21/PixelArtistry_lowpoly_00012.glb",
        "task": "0d39aaba-f657-4130-8982-5880ea1c82ad",
        "what": "red demon: four arms (two big clawed, two small), membrane wings, an abdomen tail, antennae (upload)"}
    changed = True
if not any(c["id"] == "demon_four_arms_wings" for c in doc["cases"]):
    doc["cases"].append({
        "id": "demon_four_arms_wings",
        "task": "0d39aaba-f657-4130-8982-5880ea1c82ad",
        "cost": 4,
        "defect": "owner 2026-10-11 («нужно отправлять какого нибудь умника на новую ветку, 4 руки и крылья»; «универсально для "
                  "произвольного числа рук, а крыльев пусть хотя бы 2»): the biped fit knows one arm pair; the second arm pair, "
                  "the wings and the tail were weighted to the nearest primary arm / leg bones and swung with them. Multi-limb "
                  "rig · V3 (mt/multilimb.py): every limb traced on the voxel solid (tips by geodesic distance, mirror pairs judged "
                  "together, wing = sheet by planarity / depth / back root, crest = horn), the primary pair pinned for fastrig, "
                  "extra arm pairs as {Side}Shoulder{k}..Hand{k}, wings as {Side}Wing..Wing3, Tail..Tail3; weights per chain "
                  "along the chain, root blended into the trunk weights, chain seams split (weld_split); extra arms follow the "
                  "primary (offset), wings flap / idle, tail wags (options)",
        "checks": [
            {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "0d39aaba.upload.glb",
             "params": {"words": "red demon with four arms and wings", "forward_axis": "+z", "v3_bound": True},
             "thresholds": {"seconds": ["<=", 60], "bones": [">=", 40], "clips": [">=", 8], "body_plan": ["==", "biped"]}},
            {"id": "mt_multilimb", "kind": "multilimb_rig", "paths": ["mt"], "on": "mt_rig",
             "thresholds": {"use": ["==", 1], "arms": ["==", 4], "arm_pairs": ["==", 2], "wings": ["==", 2],
                            "wing_pairs": ["==", 1], "tails": ["==", 1], "extra_chains": [">=", 5],
                            "chains_without_vertices": ["==", 0], "extra_bones": [">=", 20],
                            "cross_chain_weight_after": ["<=", 0.001], "clip_channels_extra": [">=", 160],
                            "unweighted": ["==", 0], "stretch_4x": ["<=", 800]}},
            {"id": "mt_qa", "kind": "mt_numeric_qa", "paths": ["mt"], "on": "mt_rig",
             "params": {"clips": ["Idle", "Walking"], "cap": 2000},
             "thresholds": {"clips_passed": [">=", 2]},
             "xfail": {"reason": "the big primary arms are welded onto the wing edges and their label runs along the wing's "
                                 "leading edge: the seam split leaves torn edges under the library clips (Walking: tear > 30, "
                                 "share > 0.006); Idle passes. Local 2026-10-11: 1 of 8 clips pass",
                       "owner": "Multi-limb rig · V3"}},
            {"id": "mt_rig_control", "kind": "mt_rig_first", "paths": ["mt"], "input": "66ba97ba.upload.glb",
             "params": {"words": "Female warrior with sword and armor"},
             "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20]}},
            {"id": "mt_multilimb_control", "kind": "multilimb_rig", "paths": ["mt"], "on": "mt_rig", "role": "control",
             "thresholds": {"extra_chains": ["==", 0], "extra_bones": ["==", 0], "wings": ["==", 0]}},
        ],
        "note": "v3_bound: the orientation refiner reads this model as upside down (\"two feet at the high end\": the wing "
                "tips) and would turn it; the conveyor's hash-bound V3 source is left alone, so is the case (a case for the "
                "orientator: see lying_upright). The control: a plain biped (the warrior) gets no extra chain. Known gaps: "
                "fingers on extra arms; Wing1 joints outside the thin membrane; the arm label along the wing's leading edge."})
    changed = True
# warrior_sword_posed/mt_pose_fit: asserts Limb stabilization's stage_traced, which is NOT installed on production
# (its own handoff: the gate was blocked by other cases). AGENTS.md: an unfixed defect enters as XFAIL, never as a
# blocking FAIL. Flip it back to pass when that stage lands (XPASS shows it).
for c in doc["cases"]:
    if c["id"] == "warrior_sword_posed":
        for ch in c["checks"]:
            if ch["id"] == "mt_pose_fit" and "xfail" not in ch:
                ch["xfail"] = {"reason": "Limb stabilization's stage_traced (mesh-only arm turn) is in Git but not installed "
                                         "on production MT: arm_proportions 4, prop grip 20.7 % H on the live code "
                                         "(gate 20261011T032201Z). Flip to pass when it lands (XPASS)",
                               "owner": "Limb stabilization · V3"}
                changed = True
if changed:
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
    print("corpus.json: updated (demon case, warrior mt_pose_fit xfail)")
else:
    print("corpus.json: nothing to change")
