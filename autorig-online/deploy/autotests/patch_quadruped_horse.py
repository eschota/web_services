"""Astra TODO · horse (2026-10-11): two regression cases for task a742491a (run 3ef6defe) and their check kinds.

The defect: a gltfpack GLB (KHR_mesh_quantization + EXT_texture_webp required) went through fastrig unchanged, and
the Unity viewer's glTFast refused the rig («ExtensionUnsupported;EXT_texture_webp»); Vision took the horse's side for
its front, the rig was rebuilt sideways, the «left» legs became the hind pair, and front and hind legs shared weights
(QA rig_check 145/145 bad frames) while the triage showed it green.

Anchored, idempotent edits on files other agents edit (re-read production, then run):

    sudo python3 patch_quadruped_horse.py /srv/autorig/autotests        # checks.py + corpus.json in place

checks.py    kinds `mt_quadruped_rig` (the rig the conveyor builds after Vision: classifier «quadruped» and Vision's
             front given), `viewer_opens` (required extensions and image formats the Unity viewer's glTFast 6.20 loads)
             and `quadruped_legs` (leg isolation from the GLB's own bone names + the rig_check stretch of rig.json)
corpus.json  inputs a742491a.upload.glb / .mt-before.glb / .mt-before.rig.json; cases `horse_opens_in_viewer` and
             `horse_quadruped_legs` (each a regression on the code under test plus a detector on the recorded bad rig)
"""
import json
import pathlib
import sys

CHECK_FN = '''

# Astra TODO · horse (a742491a): what the Unity viewer's glTFast 6.20 loads (no Draco / meshopt / KTX packages)
GLTFAST_REQUIRED_OK = {"KHR_mesh_quantization", "KHR_texture_transform", "KHR_materials_pbrSpecularGlossiness",
                       "KHR_materials_unlit", "KHR_materials_variants", "KHR_materials_transmission",
                       "EXT_mesh_gpu_instancing", "KHR_lights_punctual", "KHR_materials_clearcoat"}
LEG_BONE_RX = re.compile(r"^(Left|Right)(Front|Hind)(UpLeg|Leg|Foot|Toe)$")


def viewer_opens(path):
    """Astra TODO · horse: the viewer refuses a GLB that requires an extension glTFast lacks (EXT_texture_webp, Draco,
    meshopt, KTX2) and cannot decode WebP / KTX2 images. -> the counts the thresholds judge."""
    from mt import skin_postvalidate as SP
    doc = SP.read_glb(pathlib.Path(path).read_bytes())[0]
    req = list(doc.get("extensionsRequired") or [])
    bad = [e for e in req if e not in GLTFAST_REQUIRED_OK]
    imgs = []
    for im in doc.get("images") or []:
        mime = im.get("mimeType") or ("image/png" if str(im.get("uri", "")).lower().endswith(".png") else
                                      "image/jpeg" if re.search(r"\\.jpe?g$", str(im.get("uri", "")).lower()) else "?")
        imgs.append(mime)
    bad_img = [m for m in imgs if m not in ("image/png", "image/jpeg")]
    return {"required": req, "unsupported_required": len(bad), "unsupported_list": bad, "images": len(imgs),
            "unsupported_images": len(bad_img), "image_types": sorted(set(imgs)),
            "bytes": pathlib.Path(path).stat().st_size}


def quadruped_legs(path, rig_json=None):
    """Astra TODO · horse: a quadruped rig's legs isolated by topology. From the GLB alone (bone names Left/Right +
    Front/Hind + UpLeg/Leg/Foot/Toe): vertices weighted (> 5 %) to a front and a hind leg, welded edges whose ends
    belong to two different legs (dominant), front|hind among them; from rig.json: the rig_check stretch."""
    s = Skin(path)
    keys = sorted({m.group(1) + m.group(2) for n in s.names for m in [LEG_BONE_RX.match(n)] if m})
    if len(keys) < 4:
        return {"legs": len(keys), "front_hind_vertices": None}
    G = np.zeros((len(s.J), len(keys)))
    for k, key in enumerate(keys):
        cols = [i for i, n in enumerate(s.names) if (m := LEG_BONE_RX.match(n)) and m.group(1) + m.group(2) == key]
        G[:, k] = (np.isin(s.J, cols) * s.W).sum(1)
    front = np.array(["Front" in k for k in keys])
    has = G > 0.05
    fh_v = int((has[:, front].any(1) & has[:, ~front].any(1)).sum())
    rest = np.concatenate([r["rest"] for r in s.rows])
    q = np.round((rest - rest.min(0)) / max(float(np.ptp(rest, 0).max()), 1e-12) * 1e5).astype(np.int64)
    _, wid = np.unique(q, axis=0, return_inverse=True)
    wid = wid.reshape(-1)
    dom = np.where(G.max(1) >= 0.5, G.argmax(1), -1)
    wdom = np.full(int(wid.max()) + 1, -1)
    wdom[wid] = dom
    e = np.concatenate([s.F[:, [0, 1]], s.F[:, [1, 2]], s.F[:, [2, 0]]])
    eu, ev = wid[e[:, 0]], wid[e[:, 1]]
    key = np.unique(np.sort(np.stack([eu, ev], 1), 1), axis=0)
    a, b = wdom[key[:, 0]], wdom[key[:, 1]]
    cross = (a >= 0) & (b >= 0) & (a != b)
    fh_e = cross & (front[np.maximum(a, 0)] != front[np.maximum(b, 0)])
    out = {"legs": len(keys), "front_hind_vertices": fh_v, "cross_leg_edges": int(cross.sum()),
           "front_hind_edges": int(fh_e.sum()), "vertices": int(len(s.J))}
    if rig_json and pathlib.Path(rig_json).is_file():
        rig = json.loads(pathlib.Path(rig_json).read_text(encoding="utf-8"))
        st = (rig.get("checks") or {}).get("rig_check_stretch") or {}
        frames = [v for k, v in st.items() if k.startswith("t=") and isinstance(v, dict)]
        out.update(rig_check_edges_over_2x=max([int(v.get("edges_over_2x") or 0) for v in frames] or [0]),
                   rig_check_edges_over_4x=max([int(v.get("edges_over_4x") or 0) for v in frames] or [0]),
                   rig_check_max_stretch=max([float(v.get("max_stretch") or 0) for v in frames] or [0]),
                   forward_axis=rig.get("forward_axis"), forward_check=rig.get("forward_check"))
    return out


def mt_quadruped_rig(ctx, input_name, what="", forward_axis="+z"):
    """Astra TODO · horse: the rig the V3 conveyor builds once Vision has answered (mt.rig_first.build on a run whose
    classify says quadruped and whose projections carry Vision's front), so a front across the body is exercised."""
    run = pathlib.Path(ctx["work"]) / "run"
    shutil.rmtree(run, ignore_errors=True)
    (run / "proj").mkdir(parents=True)
    shutil.copyfile(_input(input_name), run / "proj" / "model.glb")
    (run / "proj" / "manifest.json").write_text(json.dumps({"forward_axis": forward_axis}))
    (run / "phases.json").write_text(json.dumps({"phases": [{"id": "classify", "result": {
        "what": what, "body": "quadruped", "props": ""}}]}))
    code = ("import json, sys; from mt import rig_first as R; d = R.build(sys.argv[1], sys.argv[2]); "
            "print(json.dumps({k: d.get(k) for k in ('body_plan', 'forward_axis', 'forward_check', 'timings_s')} "
            "| {'bones': len(d.get('bones') or [])}, default=str))")
    p, wall = _child([PY, "-P", "-c", code, str(run), what], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"rig_first.build exit {p.returncode}: {(p.stderr or p.stdout)[-600:]}")
    out = json.loads(p.stdout.strip().splitlines()[-1])
    ctx["run"], ctx["words"] = run, what
    from mt import skin_postvalidate as SP
    doc, binary = SP.read_glb((run / "proj" / "model.glb").read_bytes())
    P = np.concatenate([r["rest"] for r in SP.prepare_primitives(doc, binary)[0]]) if doc.get("skins") else None
    if P is None:
        from mt import fastrig as F
        P = F.Source((run / "proj" / "model.glb").read_bytes()).positions
    ext = np.ptp(P, 0)
    long_ax = "z" if ext[2] >= ext[0] else "x"
    fc = out.get("forward_check") or {}
    return {"seconds": wall, "bones": out.get("bones"), "body_plan": out.get("body_plan"),
            "forward_axis": out.get("forward_axis"), "forward_given": forward_axis,
            "forward_axis_along_body": int(str(out.get("forward_axis") or "")[-1:] == long_ax),
            "forward_changed": int(bool(fc.get("changed"))), "rig_total_s": (out.get("timings_s") or {}).get("fastrig")}
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "mt_quadruped_rig":
        return mt_quadruped_rig(ctx, inp, a.get("what", ""), a.get("forward_axis", "+z"))
    if k == "viewer_opens":
        return viewer_opens(target)
    if k == "quadruped_legs":
        rig_json = ctx["run"] / "rig" / "rig.json" if on == "mt_rig" else (_input(a["rig_json"]) if a.get("rig_json") else None)
        return quadruped_legs(target, rig_json)
    if k == "pending_detector":
        return {"pending": True}
'''

TASK = "a742491a-981e-43aa-bfcb-3b465be8ff91"
INPUTS = {
    "a742491a.upload.glb": {
        "source": "/srv/autorig/data/var/uploads/3ab9ad74-782b-4f00-8750-a806aa6de2bb/3D_Tuyet_Dia.glb", "task": TASK,
        "what": "armored horse with tack (upload: gltfpack 1.2, KHR_mesh_quantization + EXT_texture_webp required, "
                "18 483 vertices, one welded shell, a tail hanging to the hocks)"},
    "a742491a.mt-before.glb": {
        "source": "/srv/autorig/audits/astra-todo-horse-20261011/run_v0/rig/rigged.glb", "task": TASK,
        "what": "V3 rig v0 of run 3ef6defe (2026-10-10): the source's required webp / quantization kept (Unity viewer "
                "refuses it), rigged sideways after Vision's front +x: front and hind legs share weights"},
    "a742491a.mt-before.rig.json": {
        "source": "/srv/autorig/audits/astra-todo-horse-20261011/run_v0/rig/rig.json", "task": TASK,
        "what": "rig.json of that v0 rig (rig_check stretch: 399 edges over 2x, max 42x)"},
}

DEFECT = ("a gltfpack horse (task a742491a, run 3ef6defe, Astra TODO 2026-10-11): the rig kept the source's required "
          "EXT_texture_webp / KHR_mesh_quantization and the Unity viewer (glTFast) refused it; Vision took the side "
          "view for the front, the quadruped was rigged sideways and its front and hind legs shared weights "
          "(rig_check 145/145 bad frames, max 24x) while the triage showed it green")
RIG_STEP = {"id": "mt_rig", "kind": "mt_quadruped_rig", "paths": ["mt"], "input": "a742491a.upload.glb",
            "params": {"what": "armored horse with tack", "forward_axis": "+x"},
            "thresholds": {"seconds": ["<=", 60], "bones": [">=", 26], "body_plan": ["==", "quadruped"],
                           "forward_axis_along_body": ["==", 1]}}
CASES = [
    {"id": "horse_opens_in_viewer", "task": TASK, "cost": 1, "defect": DEFECT,
     "checks": [RIG_STEP,
                {"id": "opens", "kind": "viewer_opens", "paths": ["mt"], "on": "mt_rig",
                 "thresholds": {"unsupported_required": ["==", 0], "unsupported_images": ["==", 0]}},
                {"id": "before_opens_detector", "kind": "viewer_opens", "role": "detector", "paths": ["mt"],
                 "input": "a742491a.mt-before.glb", "thresholds": {"unsupported_required": [">=", 1]}},
                {"id": "source_detector", "kind": "viewer_opens", "role": "detector", "paths": ["mt"],
                 "input": "a742491a.upload.glb", "thresholds": {"unsupported_required": [">=", 1]}}]},
    {"id": "horse_quadruped_legs", "task": TASK, "cost": 1, "defect": DEFECT,
     "checks": [RIG_STEP,
                {"id": "legs", "kind": "quadruped_legs", "paths": ["mt"], "on": "mt_rig",
                 "thresholds": {"legs": ["==", 4], "front_hind_vertices": ["==", 0], "front_hind_edges": ["==", 0],
                                "cross_leg_edges": ["<=", 40], "rig_check_edges_over_2x": ["<=", 60],
                                "rig_check_max_stretch": ["<=", 12]},
                 "note": "2026-10-11 fix: 0 / 0 / 20 cross-leg edges (forelegs at the brisket) / 25 over 2x / 8.4x; "
                         "v0: 26 front+hind vertices, 896 cross-leg edges, 399 over 2x, 42x"},
                {"id": "before_legs_detector", "kind": "quadruped_legs", "role": "detector", "paths": ["mt"],
                 "input": "a742491a.mt-before.glb", "params": {"rig_json": "a742491a.mt-before.rig.json"},
                 "thresholds": {"front_hind_edges": [">=", 100], "rig_check_edges_over_2x": [">=", 200]}}]},
]


def _write(path: pathlib.Path, text: str):
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def patch_checks(cp: pathlib.Path):
    s = cp.read_text(encoding="utf-8")
    changed = []
    if "def viewer_opens(path):" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: the MT pipeline steps anchor is missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: viewer_opens, quadruped_legs, mt_quadruped_rig")
    if 'if k == "mt_quadruped_rig":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: the pending_detector dispatch anchor is missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        compile(s, str(cp), "exec")
        _write(cp, s)
    return changed


def patch_corpus(jp: pathlib.Path):
    """corpus.json as autotests.py sync --write-manifest writes it (json.dumps indent=1): edit it as a document."""
    doc = json.loads(jp.read_text(encoding="utf-8"))
    changed = []
    for name, item in INPUTS.items():
        if name not in doc["inputs"]:
            doc["inputs"][name] = item
            changed.append(f"input {name}")
    have = {c["id"] for c in doc["cases"]}
    for case in CASES:
        if case["id"] not in have:
            doc["cases"].append(case)
            changed.append(f"case {case['id']}")
    if changed:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    return changed


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "/srv/autorig/autotests")
    done = patch_checks(root / "checks.py") + patch_corpus(root / "corpus.json")
    print("\n".join(done) or "already patched")


if __name__ == "__main__":
    main()
