"""Multi-limb rig · V3: the anchor hunks in mt/fastrig.py and mt/rig_first.py (applied on the PROD copies right
before a deploy; every anchor must match exactly once, else the script stops without writing)."""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def patch(name, hunks):
    p = root / name
    s = p.read_text(encoding="utf-8")
    if "Multi-limb rig" in s:
        print(f"{name}: already patched")
        return
    for old, new in hunks:
        if s.count(old) != 1:
            raise SystemExit(f"{name}: anchor found {s.count(old)} times:\n{old[:200]}")
        s = s.replace(old, new)
    p.write_text(s, encoding="utf-8")
    print(f"{name}: {len(hunks)} hunks applied")


patch("fastrig.py", [
    ("""        stats["leg_isolation"] = leg_isolation(g, sk, joints, weights)
    T["weights"] = round(time.time() - t, 3)
""", """        stats["leg_isolation"] = leg_isolation(g, sk, joints, weights)
    _mlr = None
    try:                                                 # Multi-limb rig · V3 (mt/multilimb.py): extra arm pairs,
        from . import multilimb as _ml                      # wings and a tail as their own chains, weights per chain
        _mlr = _ml.apply(run, b, sk, w, Rc, plan, (sopts or {}).get("multilimb"))
        if _mlr:
            w = _mlr["w"]
            _mi = _st.fastrig_opts(sopts)["max_influences"] if sopts is not None else 4
            joints, weights = top4(w.astype(np.float32), _mi)
            if split is not None:                        # the duplicated contact vertices take the same weights
                _m = _mlr["mask"]
                split[0][_m], split[1][_m] = joints[_m], weights[_m]
            if _mlr.get("split") is not None:            # the seams between chains: faces duplicated (weld_split)
                _alt, _cm, _skip = _mlr["split"]
                _aj, _aw = top4(_alt.astype(np.float32), _mi)
                if split is None:
                    split = (_aj, _aw, _cm, _skip)
                else:
                    _rows = np.zeros(g.n, bool)
                    _rows[g.faces[_cm]] = True
                    split[0][_rows], split[1][_rows] = _aj[_rows], _aw[_rows]
                    split = (split[0], split[1], split[2] | _cm, split[3] | _skip)
            stats["multilimb"] = _mlr["stats"]
    except Exception as exc:                             # noqa: BLE001 - the branch never costs the rig
        _mlr = None
        print(f"[fastrig] multilimb skipped: {exc!r}", file=sys.stderr)
    T["weights"] = round(time.time() - t, 3)
"""),
    ("""    anim = rig_check(plan, sk, Rc, b.H)
    wr.finish(out / "rigged.glb", sk.names, sk.parent, heads_model, anim)
""", """    anim = rig_check(plan, sk, Rc, b.H)
    if _mlr:                                             # Multi-limb rig · V3: the extra chains move in the check
        anim = _ml.rig_check_extend(anim, sk, Rc)
    wr.finish(out / "rigged.glb", sk.names, sk.parent, heads_model, anim)
"""),
    ("""    if plan == "biped":
        doc["hip_estimates"] = getattr(b, "hip_estimates", {})
""", """    if plan == "biped":
        doc["hip_estimates"] = getattr(b, "hip_estimates", {})
    if _mlr:                                             # Multi-limb rig · V3: the record (chains, counts, naming)
        doc["multilimb"] = _ml.summary(run)
"""),
])

patch("rig_first.py", [
    ("""    t = time.time()
    doc = F.build(str(run), str(run / "rig"), False, plan)
    T["fastrig"] = round(time.time() - t, 3)
""", """    # Multi-limb rig · V3 (owner 2026-10-11, 0d39aaba «4 руки и крылья»): every limb traced on the voxel solid;
    # with more arms than the plan has, the primary pair is pinned for fastrig and the rest get their own chains
    t = time.time()
    ml = None
    try:
        from . import multilimb as ML
        ml = ML.stage(run, words=words, plan=plan)
    except Exception as exc:                                     # noqa: BLE001 - the branch never costs the rig
        T["multilimb_error"] = f"{type(exc).__name__}: {exc}"[:200]
    T["multilimb"] = round(time.time() - t, 3)
    t = time.time()
    doc = F.build(str(run), str(run / "rig"), False, plan)
    T["fastrig"] = round(time.time() - t, 3)
"""),
    ("""        if tess.get("applied"):
            doc = F.build(str(run), str(run / "rig"), False, doc.get("body_plan"))
""", """        if tess.get("applied"):
            if ml and ml.get("use"):                             # Multi-limb rig · V3: traced again on the new mesh
                ml = ML.stage(run, words=words, plan=doc.get("body_plan"))
            doc = F.build(str(run), str(run / "rig"), False, doc.get("body_plan"))
"""),
    ("""        if doc.get("body_plan") == "biped" and pm != "off" and (pm == "on" or any(
                srcs.get(f"{s}{n}") == "proportions" for s in ("Left", "Right") for n in ("Arm", "Hand"))):
""", """        ml_pinned = bool((ml or {}).get("pinned_primary"))        # Multi-limb rig · V3: its pins stay
        if doc.get("body_plan") == "biped" and pm != "off" and not ml_pinned and (pm == "on" or any(
                srcs.get(f"{s}{n}") == "proportions" for s in ("Left", "Right") for n in ("Arm", "Hand"))):
"""),
    ("""        elif PS.pinned(run):
            PS.clear(run)
            rebuild = True
""", """        elif PS.pinned(run) and not ml_pinned:
            PS.clear(run)
            rebuild = True
"""),
    ("""    elif str(doc.get("body_plan") or "") == "hands":              # Hands rig · V3: the hand clips, no library
""", """    try:                                                         # Multi-limb rig · V3: extra arms follow the primary
        from . import multilimb as ML                            # ones, the wings flap / idle, the tail wags
        if doc.get("multilimb") and clips:
            clips = ML.extend_clips(clips, doc, run)
    except Exception:                                            # noqa: BLE001
        pass
    if str(doc.get("body_plan") or "") == "hands":              # Hands rig · V3: the hand clips, no library
"""),
])
