"""Monster rig · V3: the anchor hunks in mt/rig_first.py and mt/fastrig.py (applied on the PROD copies right before a
deploy; every anchor must match exactly once, else nothing is written)."""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def patch(name, hunks):
    p = root / name
    s = p.read_text(encoding="utf-8")
    if "Monster rig" in s:
        print(f"{name}: already patched")
        return
    for old, new in hunks:
        if s.count(old) != 1:
            raise SystemExit(f"{name}: anchor found {s.count(old)} times:\n{old[:200]}")
        s = s.replace(old, new)
    p.write_text(s, encoding="utf-8", newline="\n")
    print(f"{name}: {len(hunks)} hunks applied")


patch("rig_first.py", [
    ("""    T["multilimb"] = round(time.time() - t, 3)
""", """    T["multilimb"] = round(time.time() - t, 3)
    # Monster rig · V3 (owner 2026-10-13, 35356012 «elongated humanoid creature»): a crouched / digitigrade /
    # stumpy / long-legged biped keeps its pose as the rest pose: the traced arm and leg chains are pinned, the
    # library clips are bind-relative (mt/monster.py)
    t = time.time()
    mn = None
    try:
        from . import monster as MN
        mn = MN.stage(run, words=words, plan=plan if plan else (ml or {}).get("plan"))
    except Exception as exc:                                     # noqa: BLE001 - never costs the rig
        T["monster_error"] = f"{type(exc).__name__}: {exc}"[:200]
    T["monster"] = round(time.time() - t, 3)
"""),
    ("""        ml_pinned = bool((ml or {}).get("pinned_primary"))        # Multi-limb rig · V3: its pins stay
""", """        ml_pinned = bool((ml or {}).get("pinned_primary"))        # Multi-limb rig · V3: its pins stay
        ml_pinned = ml_pinned or bool((mn or {}).get("pinned"))   # Monster rig · V3: its pose is the rest pose
"""),
    ("""    if str(doc.get("body_plan") or "").lower().startswith(("biped", "human")):
        for name in RT.MAIN:
""", """    if (doc.get("monster") or {}).get("use") and (doc.get("monster") or {}).get("policy"):
        doc["retarget_reference_policy"] = doc["monster"]["policy"]   # Monster rig · V3: the crouch stays under the motion
    if str(doc.get("body_plan") or "").lower().startswith(("biped", "human")):
        for name in RT.MAIN:
"""),
])

patch("fastrig.py", [
    ("""    if plan == "biped":
        doc["hip_estimates"] = getattr(b, "hip_estimates", {})
""", """    if plan == "biped":
        doc["hip_estimates"] = getattr(b, "hip_estimates", {})
        try:                                                 # Monster rig · V3: the record (leg type, gait, policy)
            from . import monster as _mn
            _mrec = _mn.summary(run)
            if _mrec and _mrec.get("use"):
                doc["monster"] = _mrec
        except Exception:                                    # noqa: BLE001
            pass
"""),
])
