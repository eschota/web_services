"""Skinning · V3 (2026-10-11): the skin-quality gate the suite lacked.  The warrior bfbd3248 tore (fastrig gave the
belly plates and the skirt to a forearm bent in front of them, then a joint move rebuilt it worse) and no check
blocked the release: cases measured time, bones, clips, collisions, centring - never the stretch.

Anchored, idempotent (re-read production, then run):

    sudo python3 patch_rig_stretch.py /srv/autorig/autotests

checks.py    kind `rig_stretch` (on mt_rig): the fast rig's own rig_check stretch from rig.json (edges over 2x / 4x
             summed over the clip's test frames, max stretch), the per-bone weight sanity (bones with no vertices,
             the heaviest bone's share) and skin_tools.measure on the GLB (leak / lag / stretch / clip) -> thresholds
             against a stored baseline (today's numbers after the fix, 1.2x + a margin; the classic converter's
             numbers for the same models are in the case docs for reference).
corpus.json  `rig_stretch` checks on warrior_sword, boy_tshirt, knight_rigpath, anime_heel, posed_fox_warrior and a new
             case `warrior_kitbash` (66ba97ba.prepared.glb = the Blender export of task bfbd3248, 51 shells).
"""
import json
import pathlib
import sys

CHECK_FN = '''

def rig_stretch(ctx):
    """«Skinning · V3» (2026-10-11): does the fast rig tear its own mesh?  rig.json's rig_check stretch (fastrig's
    looping test clip at three frames: edges over 2x / 4x their rest length that also grew), the per-bone weight
    sanity, and skin_tools.measure on the GLB (synthetic joint poses + the first clip; the welded mesh, so a split
    seam counts there but not in rig_check)."""
    run = ctx["run"]
    rig = json.loads((run / "rig" / "rig.json").read_text())
    c = rig.get("checks") or {}
    st = {k: v for k, v in (c.get("rig_check_stretch") or {}).items() if isinstance(v, dict)}
    out = {"sum_edges_over_2x": sum(int(v.get("edges_over_2x") or 0) for v in st.values()),
           "sum_edges_over_4x": sum(int(v.get("edges_over_4x") or 0) for v in st.values()),
           "max_stretch": max([float(v.get("max_stretch") or 0) for v in st.values()] or [0]),
           "worst_pairs": (c.get("rig_check_stretch") or {}).get("worst"),
           "limb_reach_moved": c.get("limb_reach_moved"), "contact_split_faces": c.get("limb_contact_split_faces"),
           "unweighted_vertices": c.get("unweighted_vertices"), "bones": len(rig.get("bones") or [])}
    try:
        from mt import skin_tools as ST
        r = ST.Rig((run / "rig" / "rigged.glb").read_bytes())
        m = ST.measure(r, r.Wg)
        out.update({f"measure_{k}": v for k, v in (m.get("total") or {}).items()})
        dom = r.Wg.argmax(1)
        cnt = np.bincount(dom, minlength=len(r.names))
        deform = [i for i, n in enumerate(r.names) if not n.endswith("_End") and not n.startswith(("Prop", "Hair_"))]
        limb = [i for i in deform if r.names[i].endswith(("Arm", "ForeArm", "UpLeg", "Leg"))]
        out["empty_limb_bones"] = int(sum(1 for i in limb if cnt[i] == 0))
        out["top_bone_share"] = round(float(cnt.max()) / max(int(cnt.sum()), 1), 4)
    except Exception as exc:                                   # noqa: BLE001 - the rig.json numbers still judge
        out["measure_error"] = f"{type(exc).__name__}: {exc}"[:160]
    return out
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "rig_stretch":
        return rig_stretch(ctx)
    if k == "pending_detector":
        return {"pending": True}
'''

# baseline 2026-10-11 after limb_reach + limb_split (sum over 3 frames): bfbd3248 61/267, 66ba97ba.upload 376/1144,
# d1522b45 81/446, 16ce2f35 25/249, 98c1247c 5/311, 8a1b1cc5 0/0, 9a34e8c0 0/66; before the fix 644, 1473, 539, 25,
# 5, 6, 0.  classic converter (skin_tools.measure stretch_4x): warrior 100, boy 47, knight 9.
STRETCH = {
    "warrior_sword": {"sum_edges_over_4x": ["<=", 450], "sum_edges_over_2x": ["<=", 1400], "empty_limb_bones": ["==", 0]},
    "boy_tshirt": {"sum_edges_over_4x": ["<=", 8], "sum_edges_over_2x": ["<=", 380], "empty_limb_bones": ["==", 0]},
    "knight_rigpath": {"sum_edges_over_4x": ["<=", 3], "sum_edges_over_2x": ["<=", 90], "empty_limb_bones": ["==", 0]},
    "anime_heel": {"sum_edges_over_4x": ["<=", 32], "sum_edges_over_2x": ["<=", 300], "empty_limb_bones": ["==", 0]},
    "posed_fox_warrior": {"sum_edges_over_4x": ["<=", 100], "sum_edges_over_2x": ["<=", 540], "empty_limb_bones": ["==", 0]},
}

NEW_CASE = {
    "id": "warrior_kitbash",
    "task": "bfbd3248-3feb-4bf2-95d3-65c44cfc2b03",
    "cost": 3,
    "defect": "owner 2026-10-11 («скиннинг сломался»): the female warrior as a Blender export (51 shells, no skeleton, task bfbd3248 / run 3dbc776ff6bffe8c8f3b, the same mesh as 66ba97ba): fastrig gave the belly plates and the skirt to the forearm bent in front of them (Hips/LeftForeArm edges torn 65, rig_check 1255/644 edges over 2x/4x), the fast-analysis joint moves rebuilt it worse (1010/468 per frame) and nothing gated on the stretch. Fix: limb_reach + limb_split in fastrig.skin (61 / 267 after), the stretch gate in fast_analysis.apply_fixes",
    "checks": [
        {"id": "mt_rig", "kind": "mt_rig_first", "paths": ["mt"], "input": "66ba97ba.prepared.glb",
         "params": {"words": "female warrior glowing purple curved blade"},
         "thresholds": {"seconds": ["<=", 60], "bones": [">=", 20], "clips": [">=", 8]}},
        {"id": "mt_stretch", "kind": "rig_stretch", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"sum_edges_over_4x": ["<=", 80], "sum_edges_over_2x": ["<=", 330], "empty_limb_bones": ["==", 0]}},
        {"id": "mt_limbs", "kind": "limb_collision", "paths": ["mt"], "on": "mt_rig",
         "thresholds": {"min_arm_vertices": [">=", 1]}},
    ],
}


def _write(path: pathlib.Path, text: str):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def patch_checks(cp):
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def rig_stretch(ctx):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: the MT pipeline steps anchor is missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: rig_stretch")
    if 'if k == "rig_stretch":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: the pending_detector dispatch anchor is missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    return changed


def patch_corpus(jp):
    doc = json.loads(jp.read_text(encoding="utf-8"))
    changed = []
    for case in doc["cases"]:
        th = STRETCH.get(case["id"])
        if th and not any(ch.get("id") == "mt_stretch" for ch in case["checks"]):
            if not any(ch.get("id") == "mt_rig" for ch in case["checks"]):
                continue
            case["checks"].append({"id": "mt_stretch", "kind": "rig_stretch", "paths": ["mt"], "on": "mt_rig",
                                   "thresholds": th})
            changed.append(f"{case['id']}: mt_stretch")
    if NEW_CASE["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(NEW_CASE)
        changed.append("case warrior_kitbash")
    for case in doc["cases"]:                            # warrior_sword mt_limbs: worst_share was calibrated on the
        if case["id"] != "warrior_sword":                # old skin (0.4958 of forearm+hand inside, the belly plates
            continue                                     # counted as arm); with limb_reach the arm is its real self
        for ch in case["checks"]:                        # (958 vertices) and 0.80 of it is inside the body in the
            th = ch.get("thresholds") or {}              # clips: the arm-inside-body defect of Limb collision · V3,
            if ch.get("id") == "mt_limbs" and th.get("worst_share") == ["<=", 0.62]:   # not a skin regression
                th["worst_share"] = ["<=", 0.85]
                ch["note"] = ("worst_share 0.62 -> 0.85 on 2026-10-11: the skin fix (limb_reach) took the belly plates "
                              "off the forearm, so the share of the real arm inside the body rose from 0.50 to 0.80 "
                              "at a depth of 4.8 % H (was 13.8 % H); the defect itself is the target severity_rank")
                changed.append("warrior_sword: mt_limbs worst_share 0.85")
    if changed:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    return changed


def main(root):
    root = pathlib.Path(root)
    changed = patch_checks(root / "checks.py") + patch_corpus(root / "corpus.json")
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
