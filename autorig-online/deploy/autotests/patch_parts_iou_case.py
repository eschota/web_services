"""V3.1 part labels (spec B.5, 2026-10-17): the hand-labelled parts corpus and its check kind.

    sudo python3 patch_parts_iou_case.py /srv/autorig/autotests
    sudo /srv/autorig/autotests/autotests.py sync --write-manifest     # copies the inputs read-only, sha256

checks.py    kind `parts_iou`: mt.analyzer (the common GPU analyser, fine pass, on a fleet mask service box) labels each
             corpus model; its labels are compared face by face with the hand labels (parts_gt.json): area-weighted IoU
             per label at the coarse level head / arm / leg / tail / wing / torso, plus beard, hat and the cloth group
             cloak | coat | cape | skirt | robe; the two Santa tasks must get the same label set
corpus.json  inputs parts_<name>.model.glb (the V3 run's proj/model.glb) and parts_<name>.parts_gt.json (the hand
             labels, /srv/autorig/data/autotests/parts_gt/), case `parts_labels_corpus` (tier extended: ~20 s a model)
"""
import json
import pathlib
import sys

MODELS = {   # name -> (V3 run, what)
    "santa1": ("dd3b2e1e9d5a4b205f63", "creepy santa claus (77cc1b88)"),
    "santa2": ("f98bc5933a41cdcf035d", "santa claus (4102abd1)"),
    "knight": ("9fb7d4d9be18ae658844", "armoured knight"),
    "hooded_robe": ("56301f51f8113d3a00b7", "hooded figure in a robe"),
    "witch": ("a438fdd836bc9d43ad72", "witch in a dress and hat"),
    "anime_girl": ("4757baf09f0dc6202ce6", "anime girl in a skirt"),
    "redhair_man": ("415bcd6ac676f4aa75b2", "red-haired man"),
    "plus_woman": ("cc22a7e0f640274a8ddf", "plus-size woman"),
    "woman_armsdown": ("213f2ef6c4edc42536d5", "woman, arms down"),
    "chibi": ("c6e2eb4c85a1c5a15f63", "chibi in a cloak"),
    "soldier": ("0a27d3435c53e83cf05d", "soldier"),
    "tpose_cartoon": ("a9ecd34e602e14be9091", "cartoon character in a T-pose"),
    "furry": ("21cf3386197b7238b223", "furry character with a tail"),
    "demon": ("ccfc481c738464250131", "demon"),
    "oneeye_monster": ("b5a4d055523540cd6600", "one-eyed monster"),
    "demonic_legless": ("385bcba56b26eec48f6e", "demonic creature without legs (a trail)"),
    "red_dragon": ("0fefeab8e9a9a154895f", "red dragon"),
    "horse": ("028d1acdb7187c532921", "horse"),
    "white_horse": ("75550318f8dde26027b3", "white horse"),
    "saddled_horse": ("c33c0d6f735cfb9e1c8f", "saddled horse"),
    "zebra": ("c5ff1c6306858a9e5e8a", "zebra"),
    "doberman": ("95cbe8fd652f7fd5972f", "doberman"),
    "deer": ("74d3849f9bf8a4154a7e", "deer"),
    "elephant": ("2b25bbe13b5117a3a2b6", "elephant"),
    "boar": ("5b49fa4f622634597cbb", "boar"),
    "bird_of_prey": ("48bc7952b573f99f1636", "bird of prey, wings spread"),
    "rooster": ("1b361561a6614ba5c8fd", "rooster"),
    "hero": ("3fe3648b902dcae448ba", "superhero with a floor-length cape (a clear cloak)"),
    "cape_mesh": ("f6e483234400204fc7fe", "armoured knight with a cape and a staff"),
    "elf_woman": ("7146046d151eb301414d", "elf woman in a T-pose, hair down the back"),
    "elf_girl": ("36d76becb174d3a4e945", "anime elf girl with very long hair"),
    "robe_woman": ("83f910e1bd94b57ed715", "stout woman in a hooded robe (coat family)"),
}

CHECK_FN = '''

def parts_iou(ctx, models, pass_="fine", santa=("santa1", "santa2")):
    """V3.1 part labels (spec B.5, 2026-10-17): mt.analyzer labels every model of the hand-labelled parts corpus on a
    fleet mask service box; area-weighted IoU against parts_gt.json per label, averaged over the models that carry
    the label (coarse: head / arm / leg / tail / wing / torso; fine: beard, hat; cloth = cloak|coat|cape|skirt|robe)."""
    import base64 as _b64
    import zlib as _zlib
    from mt import parts_entities as PE, parts_seg as PS
    from mt.glb import load_glb
    cloth = {"cloak", "coat", "cape", "skirt", "robe"}
    agg, per, secs, failed, label_sets = {}, {}, [], [], {}
    for m in models:
        glb, gtp = _input(f"parts_{m}.model.glb"), _input(f"parts_{m}.parts_gt.json")
        gd = json.loads(gtp.read_text(encoding="utf-8"))
        codes = np.frombuffer(_zlib.decompress(_b64.b64decode(gd["face_label_zlib_b64"])), np.uint8)
        gt = np.array(gd["labels"], dtype=object)[codes]
        out = pathlib.Path(ctx["work"]) / "parts_iou" / m
        out.mkdir(parents=True, exist_ok=True)
        p, wall = _child([PY, "-P", "-m", "mt.analyzer", "--glb", str(glb), "--pass", pass_, "--no-emit",
                          "--forward", gd.get("forward") or "+z", "--category", gd.get("category") or "biped",
                          "--out", str(out)], timeout=300)
        if p.returncode != 0 or not (out / "parts_seg.npz").is_file():
            failed.append(m)
            per[m] = {"error": (p.stderr or p.stdout)[-300:]}
            continue
        z = np.load(out / "parts_seg.npz", allow_pickle=True)
        pred = np.array([str(x) for x in z["labels"]], dtype=object)[z["face_label"]]
        if len(pred) != len(gt):
            failed.append(m)
            per[m] = {"error": f"faces {len(pred)} != gt {len(gt)}"}
            continue
        mesh = load_glb(glb.read_bytes())
        area = PS.face_area(mesh.positions.astype(np.float64), mesh.faces)
        secs.append(wall)
        r = dict(PE.iou(gt, pred, area, level="coarse"))
        for lab in ("beard", "hat"):
            r.update(PE.iou(gt, pred, area, [lab]))
        cg = np.array(["cloth" if str(x).split(".")[0] in cloth else "other" for x in gt], dtype=object)
        cp = np.array(["cloth" if str(x).split(".")[0] in cloth else "other" for x in pred], dtype=object)
        r.update(PE.iou(cg, cp, area, ["cloth"]))
        per[m] = {"seconds": wall, "iou": r}
        tot = float(area.sum()) or 1.0
        label_sets[m] = sorted({str(l) for l in np.unique(pred) if str(l) != "none"
                                and float(area[pred == l].sum()) / tot >= 0.01})
        for k, v in r.items():
            if v is not None:
                agg.setdefault(k.split(".")[0], []).append(v)
    res = {f"{k}_iou_mean": round(float(np.mean(v)), 3) for k, v in sorted(agg.items())}
    res.update({f"{k}_models": len(v) for k, v in sorted(agg.items())})
    res.update({"models": len(models), "models_failed": len(failed), "failed": failed,
                "seconds_max": round(max(secs), 1) if secs else None,
                "seconds_median": round(float(np.median(secs)), 1) if secs else None, "per_model": per})
    a, b = (label_sets.get(s) for s in santa)
    if a is not None and b is not None:
        res["santa_label_sets"] = {santa[0]: a, santa[1]: b}
        res["santa_labels_same"] = int(a == b)
        res["santa_labels_jaccard"] = round(len(set(a) & set(b)) / max(1, len(set(a) | set(b))), 3)
    return res
'''

DISPATCH_ANCHOR = '''    if k == "pending_detector":
        return {"pending": True}
'''
DISPATCH = '''    if k == "parts_iou":                                           # V3.1 part labels (2026-10-17)
        return parts_iou(ctx, a.get("models") or [], a.get("pass", "fine"))
    if k == "pending_detector":
        return {"pending": True}
'''

GT_DIR = "/srv/autorig/data/autotests/parts_gt"

# the measured floor of 2026-10-17 (regression guard); the spec targets (head / arm / leg >= 0.9, cloak / beard /
# tail >= 0.8) are the open work, recorded in `defect`
THRESHOLDS = {"models_failed": ["<=", 1],          # measured 2026-10-17 (32 models, fine pass, after the borders /
              "head_iou_mean": [">=", 0.52],        # garment levers): head 0.59, arm 0.56, leg 0.68, tail 0.58,
              "arm_iou_mean": [">=", 0.48], "leg_iou_mean": [">=", 0.60],   # wing 0.59, torso 0.57, cloth 0.35,
              "tail_iou_mean": [">=", 0.45], "wing_iou_mean": [">=", 0.45],  # beard 0.49
              "cloth_iou_mean": [">=", 0.28]}

CASE = {
    "id": "parts_labels_corpus",
    "cost": 3,
    "tier": "extended",
    "defect": "V3.1 part entities (owner 2026-10-17): the two Santa tasks 77cc1b88 / 4102abd1 got different part "
              "labels. The common GPU analyser (mt.analyzer: GroundingDINO + SAM2 on the fleet mask service, "
              "back-projected and voted per face, fused with the geodesic Reeb segments of the voxel solid) must label "
              "the hand-labelled corpus. Spec targets: head / arms / legs IoU >= 0.9, cloak / beard / tail >= 0.8; the "
              "thresholds are the measured floor (a regression guard), the targets are the work still open",
    "checks": [
        {"id": "parts_iou", "kind": "parts_iou", "paths": ["mt"], "role": "detector", "tier": "extended",
         "params": {"models": list(MODELS), "pass": "fine"}, "thresholds": THRESHOLDS},
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
    if "def parts_iou(ctx" not in s:
        anchor = "\n\n# ------------------------------------------------------------------------------------------------ dispatch\n"
        if s.count(anchor) != 1:
            raise SystemExit("checks.py: anchor missing")
        s = s.replace(anchor, CHECK_FN + anchor)
        changed.append("checks.py: parts_iou")
    if 'if k == "parts_iou":' not in s:
        if s.count(DISPATCH_ANCHOR) != 1:
            raise SystemExit("checks.py: dispatch anchor missing")
        s = s.replace(DISPATCH_ANCHOR, DISPATCH)
        changed.append("checks.py: dispatch")
    if changed:
        _write(cp, s)
    jp = root / "corpus.json"
    doc = json.loads(jp.read_text(encoding="utf-8"))
    c2 = []
    for name, (run, what) in MODELS.items():
        k1, k2 = f"parts_{name}.model.glb", f"parts_{name}.parts_gt.json"
        if k1 not in doc["inputs"]:
            doc["inputs"][k1] = {"source": f"/srv/autorig/data/motion_transfer/runs/{run}/proj/model.glb",
                                 "run": run, "what": f"{what} (parts corpus)"}
            c2.append(k1)
        if k2 not in doc["inputs"]:
            doc["inputs"][k2] = {"source": f"{GT_DIR}/{name}.parts_gt.json", "run": run,
                                 "what": f"hand part labels of {name} (schema autorig.autotests.parts-gt/1)"}
            c2.append(k2)
    if CASE["id"] not in [c["id"] for c in doc["cases"]]:
        doc["cases"].append(CASE)
        c2.append("case parts_labels_corpus")
    if c2:
        _write(jp, json.dumps(doc, ensure_ascii=False, indent=1) + "\n")
    changed += [f"{len(c2)} corpus entries"] if c2 else []
    print("patched: " + ", ".join(changed) if changed else "already patched")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent)
