"""Autotests for the numeric QA calibration (QA calibration · V3, 2026-10-11).

* check kind `numeric_qa_file`: mt.v3_numeric_qa (the conveyor's own cap rule, every embedded clip) on a corpus GLB,
  plus mt.anim_qa.check on it -> passed, clips, clips_passed, tear_clips, extent_clips, worst, peak_share,
  anim_flips, anim_spikes;
* case `numeric_visible`: the known-bad rigs must still fail numeric QA (horse a742491a v0, the kitbash bfbd3248 v0
  fastrig, the chibi dd498832 before tessellation), and the corpus' clean MT rigs must pass it (boy_tshirt,
  warrior_kitbash after the skin fix);
* maniac_neck mt_numeric: its target (2 clips pass) is reached -> a plain threshold (XPASS flipped).

Anchored, idempotent:  python patch_numeric_visible_case.py <checks.py> <corpus.json>
"""
import json
import os
import pathlib
import sys

FUNC = '''

def numeric_qa_file(ctx, target, cap=2000):
    """QA calibration V3: the V3 numeric QA (visible defects, per-clip aggregates) and the pops check of anim_qa on a
    corpus GLB, in a work copy (the live log of the QA child lands there, never in a customer run)."""
    run = pathlib.Path(ctx["work"]) / "numeric" / pathlib.Path(target).stem / "rig"
    shutil.rmtree(run.parent, ignore_errors=True)
    run.mkdir(parents=True)
    glb = run / "rigged.glb"
    shutil.copyfile(target, glb)
    doc = s_doc(glb)
    names = [a.get("name") for a in doc.get("animations") or []]
    acc = [int(x.get("count") or 0) for x in doc.get("accessors") or []]
    need = 1
    for an in doc.get("animations") or []:
        need = max(need, max([acc[x["input"]] for x in an.get("samplers") or []] or [1]) * 2 + 1)
    cj, out = run / "clips.json", run / "numeric.json"
    cj.write_text(json.dumps(names))
    p, wall = _child([PY, "-P", "-m", "mt.v3_numeric_qa", str(glb), _sha(glb), str(cj), str(min(need, int(cap))),
                      str(out)], timeout=900)
    if p.returncode != 0 or not out.is_file():
        raise RuntimeError(f"v3_numeric_qa exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    rep = json.loads(out.read_text())
    clips = rep.get("clips") or {}
    vis = [c.get("visible") or {} for c in clips.values()]
    from mt import anim_qa
    aq = anim_qa.check(glb.read_bytes())
    return {"seconds": wall, "status": rep.get("status"), "rule": rep.get("verdict_rule"),
            "passed": bool(rep.get("numeric_deformation_passed")), "clips": len(clips),
            "clips_passed": sum(1 for c in clips.values() if c.get("numeric_deformation_passed")),
            "tear_clips": sum(1 for v in vis if "tear" in (v.get("why") or [])),
            "extent_clips": sum(1 for v in vis if "extent" in (v.get("why") or [])),
            "worst": max([float(v.get("worst_stretch") or 0) for v in vis] or [0.0]),
            "peak_share": max([float(v.get("peak_share") or 0) for v in vis] or [0.0]),
            "anim_flips": sum(len(r["flips"]) for r in aq.values()),
            "anim_spikes": sum(len(r["spikes"]) for r in aq.values())}
'''

DISPATCH_OLD = '''    if k == "mt_numeric_qa":
'''
DISPATCH_NEW = '''    if k == "numeric_qa_file":                                     # QA calibration V3
        return numeric_qa_file(ctx, target, a.get("cap", 2000))
    if k == "mt_numeric_qa":
'''

OWNER = "QA calibration · V3"
CASE = {
    "id": "numeric_visible",
    "task": "bfbd3248-3feb-4bf2-95d3-65c44cfc2b03",
    "cost": 2,
    "defect": "V3 backfill 2026-10-11: numeric QA failed practically every rig, clean ones included (0 of 9 clips: an "
              "edge 25 % longer failed the frame, one frame the clip), so every task stayed «needs_review». Recalibrated "
              "to visible defects per clip (tear: worst edge > 30x; extent: >= 10 edges and > 0.6 % of edges over 4x in "
              "the worst frame) and spikes to real pops. The known-bad rigs must keep failing, the clean ones must pass.",
    "owner": OWNER,
    "checks": [
        {"id": "bad_horse_v0", "kind": "numeric_qa_file", "role": "detector", "paths": ["mt"],
         "input": "a742491a.mt-before.glb",
         "thresholds": {"passed": ["==", False], "tear_clips": [">=", 1], "worst": [">", 100]},
         "note": "the gltfpack horse rigged sideways, front and hind legs sharing weights: 165x, 4.0 % over 4x"},
        {"id": "bad_kitbash_v0", "kind": "numeric_qa_file", "role": "detector", "paths": ["mt"],
         "input": "bfbd3248.mt-v0.glb",
         "thresholds": {"passed": ["==", False], "tear_clips": [">=", 8], "extent_clips": [">=", 8]},
         "note": "fastrig gave the belly plates and the skirt to the forearm: 84-213x, 1.7-3.9 % over 4x in every clip"},
        {"id": "bad_chibi_untessellated", "kind": "numeric_qa_file", "role": "detector", "paths": ["mt"],
         "input": "dd498832.mt-untess.glb",
         "thresholds": {"passed": ["==", False]},
         "note": "the low-poly chibi before tessellation: fails on extent in Waving1 only (12 edges, 0.73 %) - a "
                 "borderline case; its real gate is the tessellation stage, numeric stretch barely sees faceting"},
    ],
}
CONTROLS = {
    "boy_tshirt": {"id": "mt_numeric", "kind": "mt_numeric_qa", "paths": ["mt"], "on": "mt_rig",
                   "thresholds": {"complete": ["==", True], "clips_passed": ["==", 9]},
                   "note": "QA calibration V3: a clean fast rig passes numeric QA (was 0 of 9 clips with the strict rule)"},
    "warrior_kitbash": {"id": "mt_numeric", "kind": "mt_numeric_qa", "paths": ["mt"], "on": "mt_rig",
                        "thresholds": {"complete": ["==", True], "clips_passed": ["==", 9]},
                        "note": "QA calibration V3: the kitbash after the skin fix passes (<= 21x, <= 0.41 % over 4x)"},
}
INPUTS = {
    "bfbd3248.mt-v0.glb": {"source": "/srv/autorig/data/motion_transfer/runs/3dbc776ff6bffe8c8f3b/rig/skin/v0/rigged.glb",
                           "task": "bfbd3248-3feb-4bf2-95d3-65c44cfc2b03",
                           "what": "the kitbash warrior's first fastrig (run 3dbc776f v0): belly and skirt on the forearm"},
    "dd498832.mt-untess.glb": {"source": "/srv/autorig/data/motion_transfer/runs/2fd6423ac6aa115b0e97/rig/skin/v0/rigged.glb",
                               "task": "dd498832",
                               "what": "the low-poly chibi's fastrig before tessellation (run 2fd6423a v0)"},
}


def patch_checks(path):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    n = 0
    if "def numeric_qa_file(" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ backend / intake"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor for numeric_qa_file not found")
        s = s.replace(anchor, FUNC.rstrip("\n") + "\n" + anchor, 1)
        n += 1
    if DISPATCH_NEW not in s:
        if s.count(DISPATCH_OLD) != 1:
            raise SystemExit("checks.py: dispatch anchor not found")
        s = s.replace(DISPATCH_OLD, DISPATCH_NEW, 1)
        n += 1
    if n:
        tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
        tmp.write_text(s, encoding="utf-8")
        os.replace(tmp, p)
    return n


def patch_corpus(path):
    p = pathlib.Path(path)
    doc = json.loads(p.read_text(encoding="utf-8"))
    n = 0
    cases = {c["id"]: c for c in doc["cases"]}
    for name, row in INPUTS.items():
        if name not in doc["inputs"]:
            doc["inputs"][name] = dict(row)
            n += 1
    if "numeric_visible" not in cases:
        doc["cases"].append(json.loads(json.dumps(CASE)))
        n += 1
    for cid, chk in CONTROLS.items():
        case = cases.get(cid)
        if case and not any(c.get("id") == chk["id"] for c in case.get("checks") or []):
            case["checks"].append(dict(chk))
            n += 1
    mn = cases.get("maniac_neck")
    for chk in (mn or {}).get("checks") or []:
        if chk.get("id") == "mt_numeric" and chk.get("target"):
            chk["thresholds"].update(chk["target"].get("thresholds") or {})
            chk.pop("target")
            chk["note"] = (chk.get("note", "") + " | 2026-10-11 QA calibration V3: the visible-defect rule passes "
                           "Idle and Walking (XPASS flipped to a threshold)").strip(" |")
            n += 1
    if n:
        tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        os.replace(tmp, p)
    return n


if __name__ == "__main__":
    print("checks.py edits:", patch_checks(sys.argv[1]), "corpus.json edits:", patch_corpus(sys.argv[2]))
