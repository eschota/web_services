"""Head appendages (ears, horns, tentacles, strands) as their own chains in mt/multilimb.py, switched on by the monster
branch (mt/monster.py). Run: python patch_appendages.py <mt dir>."""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def rep(s, old, new, n=1):
    assert s.count(old) == n, (s.count(old), old[:120])
    return s.replace(old, new)


p = root / "multilimb.py"
s = p.read_text(encoding="utf-8")
if "appendage_min_pct_H" not in s:
    s = rep(s, '''    "require": {"values": ("confirmed", "pairs", "any"), "default": "confirmed",''',
            '''    "appendages": {"values": ("auto", "on", "off"), "default": "auto",
                   "doc": "ears, horns, tentacles and strands hanging from the head (crest / hair limbs) get their own "
                          "chains ({Side}Crest, Crest1..); auto: only on a body this module or the monster branch "
                          "rigs (use_appendages), on: always, off: never"},
    "appendage_min_pct_H": {"min": 5.0, "max": 80.0, "default": 20.0, "doc": "a head appendage shorter than this gets no chain"},
    "require": {"values": ("confirmed", "pairs", "any"), "default": "confirmed",''')
    s = rep(s, '''    "tail_wag_deg": {''',
            '''    "crest_sway_deg": {"min": 0.0, "max": 40.0, "default": 6.0, "doc": "head appendages: sway amplitude per segment"},
    "tail_wag_deg": {''')
    s = rep(s, '''    if L["kind"] == "tail":
        return ["Tail", "Tail1", "Tail2", "Tail3"]
    return []''', '''    if L["kind"] == "tail":
        return ["Tail", "Tail1", "Tail2", "Tail3"]
    if L["kind"] in ("crest", "hair"):
        k = "" if L.get("index", 1) == 1 else str(L["index"])
        base = f"{L['side']}Crest{k}"
        return [base] + [f"{base}{j + 1}" for j in range(len(L["joints"]) - 2)]
    return []''')
    s = rep(s, '''    if L["kind"] == "tail":
        return [("Tail", pts[0])] + [(f"Tail{k}", _at(pts, s, f)) for k, f in zip((1, 2, 3), (0.25, 0.5, 0.75))] + \\
            [("tip", pts[-1])]''', '''    if L["kind"] == "tail":
        return [("Tail", pts[0])] + [(f"Tail{k}", _at(pts, s, f)) for k, f in zip((1, 2, 3), (0.25, 0.5, 0.75))] + \\
            [("tip", pts[-1])]
    if L["kind"] in ("crest", "hair"):
        n = int(np.clip(round(Lt / (0.12 * H)), 2, 5))
        return [("Crest", pts[0])] + [(f"Crest{k + 1}", _at(pts, s, f)) for k, f in enumerate(np.linspace(0, 1, n + 1)[1:-1])] + \\
            [("tip", pts[-1])]''')
    s = rep(s, '''    kinds = [L["kind"] for L in tr["limbs"]]
    return {"arms": kinds.count("arm"),''', '''    for side in ("Left", "Right"):                                # appendages numbered per side, longest first
        for n, L in enumerate(sorted([L for L in tr["limbs"] if L["kind"] in ("crest", "hair") and L["side"] == side],
                                     key=lambda L: -L["length"]), start=1):
            L["index"] = n
    kinds = [L["kind"] for L in tr["limbs"]]
    return {"arms": kinds.count("arm"),''')
    s = rep(s, '''        if L["kind"] in ("sub", "leg", "crest", "stub", "hair"):
            limbs.append({"id": L["id"], "kind": L["kind"], "side": L["side"], "parent": L["parent"], "top": L.get("top"),''',
            '''        if L["kind"] in ("crest", "hair") and L["length"] >= o["appendage_min_pct_H"] / 100 * H:
            L["joints"] = _joints(L, o)
            pts = [p for _, p in L["joints"]]
            limbs.append({"id": L["id"], "kind": L["kind"], "side": L["side"], "index": L.get("index"), "parent": L["parent"],
                          "appendage": True, "bones": chain_names(L),
                          "joints": [[round(float(x), 5) for x in p @ Rc] for p in pts[:-1]],
                          "tip": [round(float(x), 5) for x in pts[-1] @ Rc],
                          "chain": [[round(float(x), 5) for x in p @ Rc] for p in L["chain"][:: max(1, len(L["chain"]) // 24)]],
                          "length_pct_H": round(L["length"] / H * 100, 1), "cells": L["cells_n"],
                          "join_frac": round(L.get("join_frac", 0.0), 2)})
            continue
        if L["kind"] in ("sub", "leg", "crest", "stub", "hair"):
            limbs.append({"id": L["id"], "kind": L["kind"], "side": L["side"], "parent": L["parent"], "top": L.get("top"),''')
    s = rep(s, '''    extra = [l for l in limbs if l["kind"] in ("wing", "tail") or (l["kind"] == "arm" and not l["primary"])]''',
            '''    extra = [l for l in limbs if l["kind"] in ("wing", "tail") or (l["kind"] == "arm" and not l.get("primary"))]
    appendages = [l for l in limbs if l.get("appendage")]''')
    s = rep(s, '''    doc = {"schema": SCHEMA, "mesh": path.name, "forward_axis": info["forward_axis"], "words": words[:120],
           "counts": counts, "use": use, "why": why,''', '''    use_app = bool(appendages) and (o["appendages"] == "on" or (o["appendages"] == "auto" and use))
    doc = {"schema": SCHEMA, "mesh": path.name, "forward_axis": info["forward_axis"], "words": words[:120],
           "counts": counts, "use": use, "why": why, "use_appendages": use_app, "appendages": len(appendages),''')
    s = rep(s, '''    doc = _read(run / "rig" / FILE)
    if not doc or not doc.get("use") or plan not in ("biped", "quadruped"):
        return None''', '''    doc = _read(run / "rig" / FILE)
    if not doc or not (doc.get("use") or doc.get("use_appendages")) or plan not in ("biped", "quadruped"):
        return None''')
    s = rep(s, '''    for l in doc["limbs"]:
        if l["kind"] in ("sub", "leg", "crest", "stub", "hair"):
            continue
        names = l["bones"]
        primary = l["kind"] == "arm" and l["primary"]''', '''    for l in doc["limbs"]:
        if l.get("appendage"):
            if not doc.get("use_appendages"):
                continue
        elif l["kind"] in ("sub", "leg", "crest", "stub", "hair") or not doc.get("use"):
            continue
        names = l["bones"]
        primary = l["kind"] == "arm" and l.get("primary")''')
    s = rep(s, '''            if l["kind"] == "tail":
                parent = "Hips" if "Hips" in sk.index else sk.names[0]
            else:''', '''            if l["kind"] == "tail":
                parent = "Hips" if "Hips" in sk.index else sk.names[0]
            elif l.get("appendage"):
                parent = "Head" if "Head" in sk.index else ("Neck" if "Neck" in sk.index else sk.names[0])
            else:''')
    s = rep(s, '''        tcols = [sk.index[n] for n in TRUNK + ("Neck",) if n in sk.index]''',
            '''        tcols = [sk.index[n] for n in TRUNK + ("Neck", "Head") if n in sk.index]''')
    s = rep(s, '''        m = re.fullmatch(r"Tail(\\d*)", nm)
        if m:
            key(nm, Y, 16 * np.sin(2 * ph - 0.7 * int(m.group(1) or 0)))
    for name, parts in rot.items():''', '''        m = re.fullmatch(r"Tail(\\d*)", nm)
        if m:
            key(nm, Y, 16 * np.sin(2 * ph - 0.7 * int(m.group(1) or 0)))
        m = re.fullmatch(r"(Left|Right)Crest(\\d*)", nm)
        if m:
            key(nm, X, 8 * np.sin(2 * ph - 0.6 * int(m.group(2) or 0)))
    for name, parts in rot.items():''')
    s = rep(s, '''            rot[n] = _qaxis(Y, o["tail_wag_deg"] * np.sin(ph))
    return clip''', '''            rot[n] = _qaxis(Y, o["tail_wag_deg"] * np.sin(ph))
            continue
        m = re.fullmatch(r"(Left|Right)Crest(\\d*)", n)
        if m:                                                    # a head appendage: a slow secondary sway
            seg = int(m.group(2) or 0)
            ph = 2 * np.pi * (0.8 if loco else 0.35) * times - 0.6 * seg
            rot[n] = _qaxis(X, o["crest_sway_deg"] * (0.5 if seg == 0 else 1.0) * np.sin(ph))
    return clip''')
    s = rep(s, '''    if not any(re.fullmatch(r"(Left|Right)(Arm\\d+|Wing[A-Z]?\\d*)|Tail\\d*", n) for n in names):
        return clip''', '''    if not any(re.fullmatch(r"(Left|Right)(Arm\\d+|Wing[A-Z]?\\d*|Crest\\d*)|Tail\\d*", n) for n in names):
        return clip''')
    s = rep(s, '''            "chains": [{k: l.get(k) for k in ("kind", "side", "index", "primary", "bones", "length_pct_H", "wing_score")}
                       for l in doc.get("limbs") or [] if l.get("bones")],''', '''            "use_appendages": doc.get("use_appendages"),
            "chains": [{k: l.get(k) for k in ("kind", "side", "index", "primary", "appendage", "bones", "length_pct_H", "wing_score")}
                       for l in doc.get("limbs") or [] if l.get("bones")],''')
    p.write_text(s, encoding="utf-8", newline="\n")
    print("multilimb: appendages added")
else:
    print("multilimb: already has appendages")

p = root / "monster.py"
s = p.read_text(encoding="utf-8")
if "use_appendages" not in s:
    s = rep(s, '''    if joints:
        PS._atomic_json(run / "rig" / PS.FILE, {''', '''    try:                                                         # the ears / horns / tentacles of a monster get
        from . import multilimb as ML                            # their own chains (they tear on the Head|Spine seam)
        mdoc = ML._read(run / "rig" / ML.FILE) or {}
        if mdoc.get("appendages") and not mdoc.get("use_appendages") and ML.options_for(run)["appendages"] != "off":
            mdoc["use_appendages"] = True
            mdoc["why_appendages"] = "monster"
            ML._atomic_json(run / "rig" / ML.FILE, mdoc)
            doc["appendages"] = mdoc["appendages"]
    except Exception as exc:                                     # noqa: BLE001
        doc["appendages_error"] = f"{type(exc).__name__}: {exc}"[:120]
    if joints:
        PS._atomic_json(run / "rig" / PS.FILE, {''')
    p.write_text(s, encoding="utf-8", newline="\n")
    print("monster: appendages switch added")
else:
    print("monster: already")
