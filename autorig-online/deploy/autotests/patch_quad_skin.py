"""Quadruped skin: a leg's skin is the leg's (Fable · monsters/animals, 2026-10-13; the corpus cat b94999ad whose near
front leg sat on Hips / Spine / a Prop, the chibi deer whose front-leg cores leaked to Spine2, the boar whose flank went
to the UpLeg bones).
  1. limb_reach never drops a quadruped leg component that holds leg_regions core vertices (the cat: the component
     missed the UpLeg root zone and the whole leg went to the trunk, its paw to the floor prop);
     and its fallback never hands a body vertex to a Prop bone.
  2. leg_masks: the core (below the knee) forbids every other bone, trunk and head included (not only the other
     legs); isolate_legs applies it after the smoothing too (the deer's Spine2 in the cannon).
  3. option leg_column_trunk (on | off): body above the fit's leg column top that is not inside the leg's tube
     forbids that pair's leg bones (the boar's back on FrontUpLeg / HindUpLeg); leg_column_margin (0.03 H).
  4. leg_regions reports the legs it skipped (b.leg_topo["skipped"]).
python patch_quad_skin.py <mt dir>"""
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
p = d / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "leg_column_trunk" in s:
    print("fastrig: already patched")
else:
    # ---- 4. fit_quadruped stores the leg column top / half width for the skin
    old = '''        top = top or b.y(0.45)
        legs[leg] = {"top": top, "column": column}
'''
    new = '''        top = top or b.y(0.45)
        legs[leg] = {"top": top, "column": column}
        if not hasattr(b, "leg_column"):               # quadruped skin: the column top and half width, per pair
            b.leg_column = {}
        b.leg_column[leg] = {"top": float(top), "half_w": float(np.median(ref) / 2) if ref else 0.03 * H}
'''
    assert s.count(old) == 1, "fit anchor"
    s = s.replace(old, new)

    # ---- 4. leg_regions: the skipped legs
    old = '''        if core.sum() < 12 or core.sum() > 0.3 * g.n:    # not a leg: leave it to the distances
            continue
        out[key] = {"core": core, "upper": upper, "seeds": int((lab_low == k).sum())}
'''
    new = '''        if core.sum() < 12 or core.sum() > 0.3 * g.n:    # not a leg: leave it to the distances
            skipped_legs[key] = {"core": int(core.sum()), "upper": int(upper.sum()), "seeds": int((lab_low == k).sum()),
                                 "mine": int(mine.sum())}
            continue
        out[key] = {"core": core, "upper": upper, "seeds": int((lab_low == k).sum()),
                    "core_tube": (core & tube_ok) if tube_ok is not None else core}   # the core inside the shin tube:
'''                                                         # the strict rules never take a tail on the ground (boar)
    assert s.count(old) == 1, "leg_regions skip anchor"
    s = s.replace(old, new)
    old = '''    out, recentred, proposed, skipped_pairs = {}, {}, {}, {}
'''
    new = '''    out, recentred, proposed, skipped_pairs, skipped_legs = {}, {}, {}, {}, {}
'''
    assert s.count(old) == 1
    s = s.replace(old, new)
    old = '''    if not out:
        return {}
    upper_any = np.zeros(g.n, bool)
'''
    new = '''    if not out:
        b.leg_topo_skipped = skipped_legs
        return {}
    upper_any = np.zeros(g.n, bool)
'''
    assert s.count(old) == 1
    s = s.replace(old, new)
    old = '''    return {"legs": out, "recentred_pct_H": recentred, "on_leg": upper_any,   # tail strands, tassels, long hair
            "hanging": hanging, "walk": (dist, lab)}
'''
    new = '''    return {"legs": out, "recentred_pct_H": recentred, "on_leg": upper_any,   # tail strands, tassels, long hair
            "hanging": hanging, "walk": (dist, lab), "skipped": skipped_legs}
'''
    assert s.count(old) == 1
    s = s.replace(old, new)

    # ---- 2. leg_masks: the core forbids every other bone
    old = '''    for key, reg in (topo.get("legs") or {}).items():
        others = [c for k2, cs in cols.items() if k2 != key for c in cs]
        far = [c for k2, cs in cols.items() if ("Front" in k2) != ("Front" in key) for c in cs]
        rules.append((reg["core"], others))
        rules.append((reg["upper"], far))
    return rules, cols
'''
    new = '''    for key, reg in (topo.get("legs") or {}).items():
        mine = set(cols.get(key) or [])                  # strict: the core (below the knee) is this leg's alone, no
        others = [c for c in range(len(sk.names)) if c not in mine] if strict_core else \
            [c for k2, cs in cols.items() if k2 != key for c in cs]   # trunk, head or prop either (the cat b94999ad)
        far = [c for k2, cs in cols.items() if ("Front" in k2) != ("Front" in key) for c in cs]
        rules.append((reg.get("core_tube", reg["core"]) if strict_core else reg["core"], others))
        rules.append((reg["upper"], far))
    return rules, cols
'''
    assert s.count(old) == 1, "leg_masks anchor"
    s = s.replace(old, new)
    old = '''def leg_masks(sk, topo):
    """Bone columns per leg and the exclusions leg_regions implies. -> [(rows, forbidden columns)]."""
'''
    new = '''def leg_masks(sk, topo, strict_core=False):
    """Bone columns per leg and the exclusions leg_regions implies. -> [(rows, forbidden columns)]. strict_core: the
    core below the knee forbids every other bone (trunk, head, prop), not only the other legs."""
'''
    assert s.count(old) == 1, "leg_masks def anchor"
    s = s.replace(old, new)
    old = '''def isolate_legs(w, sk, topo):
    """Weights (n x B) with every leg's topology exclusions applied, and no vertex on both a front and a hind leg (the
    smaller side goes); rows renormalised. -> vertices changed."""
    rules, cols = leg_masks(sk, topo)
'''
    new = '''def isolate_legs(w, sk, topo, strict_core=False):
    """Weights (n x B) with every leg's topology exclusions applied, and no vertex on both a front and a hind leg (the
    smaller side goes); rows renormalised. -> vertices changed."""
    rules, cols = leg_masks(sk, topo, strict_core)
'''
    assert s.count(old) == 1, "isolate_legs anchor"
    s = s.replace(old, new)
    old = '''        stats["legs_isolated_vertices"] = isolate_legs(w, sk, b.leg_topo)
'''
    new = '''        stats["legs_isolated_vertices"] = isolate_legs(w, sk, b.leg_topo, str(o.get("leg_core_own", "off")) == "on")
'''
    assert s.count(old) == 1, "isolate call anchor"
    s = s.replace(old, new)

    # ---- 1. limb_reach: the fallback never picks a Prop bone for a body vertex
    old = '''            De2 = De[drop].copy()
            De2[:, cols] = np.inf
            alt = De2.argmin(1)
            lost = ~np.isfinite(De2.min(1))
            if lost.any():                               # no other allowed bone: the nearest non-limb bone at all
                D2 = D[drop][lost].copy()
                D2[:, cols] = np.inf
                alt[lost] = D2.argmin(1)
'''
    new = '''            De2 = De[drop].copy()
            De2[:, cols] = np.inf
            _props = [i for i, nm in enumerate(sk.names) if str(nm).startswith("Prop")]
            if _props:                                   # a body vertex never falls onto a prop (the cat's paw on the
                De2[:, _props] = np.inf                  # floor blob beside it)
            alt = De2.argmin(1)
            lost = ~np.isfinite(De2.min(1))
            if lost.any():                               # no other allowed bone: the nearest non-limb bone at all
                D2 = D[drop][lost].copy()
                D2[:, cols] = np.inf
                if _props:
                    D2[:, _props] = np.inf
                alt[lost] = D2.argmin(1)
'''
    assert s.count(old) == 1, "limb_reach fallback anchor"
    s = s.replace(old, new)

    # ---- 1 + 3. skin(): the column rule before the assignment, the core restored after limb_reach
    old = '''        topo = getattr(b, "leg_topo", None) if plan == "quadruped" else None
        if topo:                                         # Astra TODO horse: a leg's skin is its own leg's (topology)
            for rows, forbid in leg_masks(sk, topo)[0]:
                if forbid and rows.any():
                    A[np.ix_(np.nonzero(rows)[0], forbid)] = False
'''
    new = '''        topo = getattr(b, "leg_topo", None) if plan == "quadruped" else None
        _tailish = np.zeros(n, bool)
        if topo:                                         # Astra TODO horse: a leg's skin is its own leg's (topology)
            _tp = [i for i, nm in enumerate(sk.names) if nm.startswith(("Tail", "Prop"))]
            if _tp:                                      # a tail lying on the ground seeds a "leg" from its tip (the
                _tailish = np.isin(D.argmin(1), _tp)     # ferret 1e079e65, the boar): never the strict core
            _strict = str(o.get("leg_core_own", "off")) in ("hard", "on")
            for rows, forbid in leg_masks(sk, topo, _strict)[0]:
                if _strict:
                    rows = rows & ~_tailish
                if forbid and rows.any():
                    A[np.ix_(np.nonzero(rows)[0], forbid)] = False
        if plan == "quadruped" and str(o.get("leg_column_trunk", "off")) != "off" and getattr(b, "leg_column", None):
            # Fable · monsters/animals 2026-10-13 (the boar fec0161f: the flank and the back on FrontUpLeg / HindUpLeg):
            # the leg is the column the fit found standing on the ground; the body above the column top that is not
            # inside the leg's tube (the thigh / shoulder around the UpLeg -> knee segment) never takes that pair's bones
            _margin = float(o.get("leg_column_margin", 0.03)) * H
            _n_col = 0
            for _pair, _lc in b.leg_column.items():
                _cols = [sk.index[f"{sd_}{_pair}{j}"] for sd_ in ("Left", "Right") for j in ("UpLeg", "Leg", "Foot", "Toe")
                         if f"{sd_}{_pair}{j}" in sk.index]
                if not _cols:
                    continue
                _above = b.body & ~prop & (p[:, 1] > _lc["top"] + _margin)
                _tube = np.full(n, np.inf)
                for sd_ in ("Left", "Right"):
                    if f"{sd_}{_pair}UpLeg" in sk.index and f"{sd_}{_pair}Leg" in sk.index:
                        _tube = np.minimum(_tube, seg_dist(p, sk.head[sk.index[f"{sd_}{_pair}UpLeg"]],
                                                           sk.head[sk.index[f"{sd_}{_pair}Leg"]])[0])
                _r = max(1.5 * float(_lc["half_w"]), 0.04 * H)
                _rows = np.nonzero(_above & (_tube > _r))[0]
                if len(_rows):
                    A[np.ix_(_rows, _cols)] = False
                    _n_col += int(len(_rows))
            _none = ~A.any(1)
            if _none.any():                              # a vertex left with no bone takes any deform bone again
                A[_none] = np.asarray(sk.deform, bool)[None, :]
            stats["leg_column_trunk_vertices"] = _n_col
'''
    assert s.count(old) == 1, "skin topo anchor"
    s = s.replace(old, new)
    old = '''        if str(o.get("limb_reach", "on")) != "off" and plan in ("biped", "quadruped"):   # Skinning · V3
            stats["limb_reach_moved"] = limb_reach(g, sk, hard, D, De, H, float(o.get("limb_capture", 1.6)), plan)
'''
    new = '''        if str(o.get("limb_reach", "on")) != "off" and plan in ("biped", "quadruped"):   # Skinning · V3
            stats["limb_reach_moved"] = limb_reach(g, sk, hard, D, De, H, float(o.get("limb_capture", 1.6)), plan)
        if topo and str(o.get("leg_core_own", "off")) != "off":   # Fable 2026-10-13: the core below the knee is this
            _restored = 0                                # leg's whatever limb_reach or the distances said (the cat
            for _key, _reg in (topo.get("legs") or {}).items():   # b94999ad: limb_reach dropped the near front leg
                _cols = [sk.index[f"{_key}{j}"] for j in ("UpLeg", "Leg", "Foot", "Toe") if f"{_key}{j}" in sk.index]
                _rows = np.nonzero(_reg.get("core_tube", _reg["core"]) & ~_tailish & ~np.isin(hard, _cols))[0]
                if len(_rows) and _cols:
                    hard[_rows] = np.array(_cols)[D[np.ix_(_rows, _cols)].argmin(1)]
                    _restored += int(len(_rows))
            stats["leg_core_restored"] = _restored
            if topo.get("skipped"):
                stats["leg_topology_skipped"] = topo["skipped"]
'''
    assert s.count(old) == 1, "limb_reach call anchor"
    s = s.replace(old, new)
    p.write_text(s, encoding="utf-8")
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: quadruped leg skin")

# ---- options registered (Tools Have Options)
q = d / "skin_tools.py"
t = q.read_text(encoding="utf-8")
if "leg_column_trunk" in t:
    print("skin_tools: already patched")
else:
    old = '''    "limb_capture": {
'''
    new = '''    "leg_core_own": {
        "kind": "enum", "values": ("hard", "on", "restore", "off"), "default": "off",
        "doc": "rebuild, quadruped: hard: the leg core below the knee inside the shin tube (leg_regions) keeps this "
               "leg's bones whatever limb_reach or the distances said, and its hard assignment forbids every other bone, "
               "trunk included (the cat b94999ad: the near front leg sat on Hips / a Prop; the boar fec0161f: the hind "
               "legs on Tail3); restore: only the restore after limb_reach; on: the smoothed weights drop trunk bones "
               "there too. Measured 2026-10-13 (gate metric rig_check_4x, sum over frames): horse 40 -> 0, cat 161 -> "
               "262 and ferret 218 -> 425 (the legs now move, so the fused paws / the tail shell welded along the legs "
               "tear: quadrupeds get no weld split), boar 331 -> 580; off (today's behaviour) until the case owners "
               "accept the thresholds or the contacts are split."},
    "leg_column_trunk": {
        "kind": "enum", "values": ("on", "off"), "default": "off",
        "doc": "rebuild, quadruped: body above the fit's leg column top that is not inside the leg's tube never takes "
               "that pair's leg bones. Measured 2026-10-13: it strips the horse's shoulder / thigh (QA 95 -> 70 edges but "
               "the UpLeg owns only the column) and tears the boar (331 -> 571); off until the tube is the thigh's."},
    "leg_column_margin": {
        "kind": "float", "min": 0.0, "max": 0.2, "default": 0.03,
        "doc": "rebuild, quadruped: leg_column_trunk starts this far (in H) above the column top."},
    "limb_capture": {
'''
    assert t.count(old) == 1, "skin_tools anchor"
    t = t.replace(old, new, 1)
    q.write_text(t, encoding="utf-8")
    print("skin_tools: options")
