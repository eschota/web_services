"""Texturing · V3 (owner 2026-10-11, the «potato» 583622f3): the regression cases and their check kinds.

    sudo python3 patch_autotex_case.py /srv/autorig/autotests

checks.py    kind `autotex_bake`: mt/autotex.bake_glb of the recorded Qwen paints (no farm) onto the untextured rig
             -> textured materials, broken normals rebuilt, auto-UV atlas area, texels seen directly, colour spread
             gained, seconds;  kind `autotex_upload_refs`: mt/autotex.upload_refs of the upload alone -> the missing
             material library is named (the intake asks the user for it).
corpus.json  inputs 583622f3.* (the OBJ upload, the V3 rig v0, the paints of texture t2), case `untextured_obj`.
             «textures lost» (a source with textures whose rig has none): no such run in the census of 2026-10-11
             (all 53 V3 runs keep their images source -> rig), so no lost case yet.
"""
import json
import pathlib
import sys

CHECK_FN = '''

def autotex_bake(ctx, rig_input, paints):
    """Texturing · V3 (2026-10-11): the recorded Qwen paints baked onto the untextured rig without the farm."""
    from mt import autotex as A
    w = pathlib.Path(ctx["work"]) / "autotex"
    w.mkdir(parents=True, exist_ok=True)
    for dst, name in paints.items():
        shutil.copyfile(_input(name), w / dst)
    pj = json.loads((w / "paint.json").read_text())
    data = _input(rig_input).read_bytes()
    opts = A.check_options({"view_px": pj["view_px"]})
    t = time.time()
    new, rep = A.bake_glb(data, w, opts, pj["forward"], "autotest")
    took = round(time.time() - t, 1)
    fa, fb = A.glb_texture_facts(new), A.glb_texture_facts(data)
    before, after = A.renders(data, pj["forward"], 256), A.renders(new, pj["forward"], 256)
    bm = np.abs(before.astype(np.int16) - 235).max(-1) > 4
    am = np.abs(after.astype(np.int16) - 235).max(-1) > 4
    mats = rep.get("materials") or [{}]
    return {"textured_before": fb["textured_materials"], "textured_after": fa["textured_materials"],
            "images_after": fa["images"], "normals_before_broken": int(rep["normals"]["before"] == "broken"),
            "normals_fixed": rep["normals"]["fixed_vertices"], "uv_area": (rep.get("auto_uv") or {}).get("uv_area", 0),
            "direct_share": mats[0].get("direct_share", 0), "align_iou_min": min(v["after"] for v in
                                                                                rep["align_iou"].values()),
            "colour_std_gain": round(A._rel_lum_std(after / 255.0, am) - A._rel_lum_std(before / 255.0, bm), 4),
            "seconds": took}


def autotex_upload_refs(ctx, upload_input, name):
    """An upload alone in its folder (as the visitor sent it): the files it names that are missing."""
    from mt import autotex as A
    d = pathlib.Path(ctx["work"]) / "upload_alone"
    d.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_input(upload_input), d / name)
    r = A.upload_refs(d / name)
    return {"referenced": len(r.get("referenced") or []), "missing": len(r.get("missing") or []),
            "missing_names": ",".join(r.get("missing") or [])}
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "autotex_bake":
        return autotex_bake(ctx, inp, a["paints"])
    if k == "autotex_upload_refs":
        return autotex_upload_refs(ctx, inp, a["name"])
    if k == "pending_detector":
        return {"pending": True}
'''

RUN = "/srv/autorig/data/motion_transfer/runs/dfb099ac26c8f16ab46b"
TASK = "583622f3-bdc3-48e7-981e-87120f5b12df"
INPUTS = {
    "583622f3.upload.obj": {"source": "/var/autorig/uploads/29fc0ea8-016e-4018-9c45-1bdff61fa28a/dpgoe.obj",
                            "task": TASK, "what": "Roblox OBJ «potato» alone: mtllib dpgoe.mtl not uploaded, no "
                                                  "images; vn all (0,1,0); per-face UV dots (area 0.0025)"},
    "583622f3.rig.glb": {"source": f"{RUN}/rig/skin/v0/rigged.glb", "task": TASK,
                         "what": "the V3 fast rig v0 of the potato: one flat grey material, broken normals"},
    "583622f3.paint_albedo.png": {"source": f"{RUN}/texture/t2/paint_albedo.png", "task": TASK,
                                  "what": "Qwen-edit albedo of the front | back clay guides (texture t2)"},
    "583622f3.paint_roughness.png": {"source": f"{RUN}/texture/t2/paint_roughness.png", "task": TASK,
                                     "what": "Qwen-edit roughness (t2)"},
    "583622f3.paint_normal.png": {"source": f"{RUN}/texture/t2/paint_normal.png", "task": TASK,
                                  "what": "Qwen-edit detail normal (t2)"},
    "583622f3.paint.json": {"source": f"{RUN}/texture/t2/paint.json", "task": TASK,
                            "what": "the paint record of t2 (forward, view_px, description)"},
}

CASE = {
    "id": "untextured_obj",
    "task": TASK,
    "cost": 1,
    "defect": "no PBR materials (owner 2026-10-11: «почему до сих пор картошка без материалов PBR?»): an OBJ uploaded "
              "without its .mtl and images, broken normals (all +Y), unusable per-face UV dots; the viewer showed it "
              "flat white. mt/autotex: the missing library is named for the user; the Qwen paints are baked onto a "
              "new UV atlas with rebuilt normals (v0: 0 textured materials, after: 1)",
    "checks": [
        {"id": "refs", "kind": "autotex_upload_refs", "paths": ["mt"], "input": "583622f3.upload.obj",
         "params": {"name": "dpgoe.obj"},
         "thresholds": {"referenced": [">=", 1], "missing": [">=", 1]}},
        {"id": "bake", "kind": "autotex_bake", "paths": ["mt"], "input": "583622f3.rig.glb",
         "params": {"paints": {"paint_albedo.png": "583622f3.paint_albedo.png",
                               "paint_roughness.png": "583622f3.paint_roughness.png",
                               "paint_normal.png": "583622f3.paint_normal.png",
                               "paint.json": "583622f3.paint.json"}},
         "thresholds": {"textured_before": ["==", 0], "textured_after": [">=", 1], "images_after": [">=", 2],
                        "normals_before_broken": ["==", 1], "normals_fixed": [">=", 9640], "uv_area": [">=", 0.3],
                        "direct_share": [">=", 0.25], "align_iou_min": [">=", 0.9], "colour_std_gain": [">=", 0.01],
                        "seconds": ["<=", 120]}},
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
    if "def autotex_bake(" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ MT pipeline steps\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: autotex kinds")
    if 'if k == "autotex_bake":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: dispatch anchor missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    jp = root / "corpus.json"
    doc = json.loads(jp.read_text(encoding="utf-8"))
    c2 = []
    for name, rec in INPUTS.items():
        if name not in doc["inputs"]:
            doc["inputs"][name] = rec
            c2.append(f"input {name}")
    if CASE["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(CASE)
        c2.append(f"case {CASE['id']}")
    if c2:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    changed += c2
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/srv/autorig/autotests")
