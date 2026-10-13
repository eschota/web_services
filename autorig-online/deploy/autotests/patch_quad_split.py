"""Quadruped contact split (Fable · monsters/animals, 2026-10-13): with leg_core_own=hard the legs move, and the paw
welded to the other paw (the cat b94999ad) or the tail shell welded along the hind legs (the boar fec0161f, the ferret
1e079e65) tears.  Bipeds have their fused contacts split (contact_split) and the holes capped (cut_cap v3 lids);
quadrupeds had no split by Rig path's policy (the old split hid the stretch behind gaps, before the caps).  Here the
split is given to quadrupeds ONLY at leg | leg and leg | tail contacts (never leg | trunk, where the thigh blends), and
the caps close the rims as for bipeds.  Option quad_contact_split on | off (skin_tools SPEC).
python patch_quad_split.py <mt dir>   (after patch_quad_skin.py)"""
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
p = d / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "quad_split_min_welded" in s:
    print("fastrig: already patched (split)")
else:
    old = '''def contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats):
'''
    new = '''def contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats, pairs="all"):
'''
    assert s.count(old) == 1, "contact_split def"
    s = s.replace(old, new)
    old = '''    limb_g = np.array([x.startswith(("arm", "leg", "fleg", "hleg", "Prop")) for x in names])   # a prop fused to
    faces = 0                                                                                   # the hair splits too
    for k, gname in enumerate(names):
        if not limb_g[k]:
            continue
        m = cut & (gid[eu] != gid[ev]) & ((gid[eu] == k) | (gid[ev] == k))
'''
    new = '''    limb_g = np.array([x.startswith(("arm", "leg", "fleg", "hleg", "Prop")) for x in names])   # a prop fused to
    faces = 0                                                                                   # the hair splits too
    if pairs in ("limbs", "legs", "paws"):               # quadrupeds (Fable 2026-10-13): only leg | leg (and leg | tail
        _lt = np.array([x.startswith(("leg", "fleg", "hleg") + (("tail",) if pairs in ("limbs", "paws") else ()))
                        for x in names])                 # for "limbs" / "paws") contacts, never leg | trunk
        cut = cut & _lt[gid[eu]] & _lt[gid[ev]]
    if pairs == "paws":                                  # only below the knees, and never a shell welded along the
        _knee = np.full(len(names), np.inf)              # whole leg (the boar's tail: a split there opens a gap)
        for k_, gname_ in enumerate(names):
            if gname_.startswith(("fleg", "hleg", "leg")):
                _sd = "Left" if gname_.endswith(".L") else "Right"
                _kind = "Front" if gname_.startswith("fleg") else "Hind" if gname_.startswith("hleg") else ""
                _nm = f"{_sd}{_kind}Leg"
                if _nm in sk.index:
                    _knee[k_] = float(sk.head[sk.index[_nm]][1])
        _y = g.p[:, 1]
        _kn = np.minimum(_knee[gid[eu]], _knee[gid[ev]])
        cut = cut & (_y[eu] < _kn) & (_y[ev] < _kn)
        _ga, _gb = np.minimum(gid[eu], gid[ev]), np.maximum(gid[eu], gid[ev])
        for _pa, _pb in {(int(a_), int(b_)) for a_, b_ in zip(_ga[cut], _gb[cut])}:
            _m = cut & (_ga == _pa) & (_gb == _pb)
            _vs = np.concatenate([eu[_m], ev[_m]])
            if np.ptp(_y[_vs]) > 0.2 * H:                # the contact runs along the leg: a welded shell, no split
                cut[_m] = False
    for k, gname in enumerate(names):
        if not limb_g[k]:
            continue
        m = cut & (gid[eu] != gid[ev]) & ((gid[eu] == k) | (gid[ev] == k))
'''
    assert s.count(old) == 1, "contact_split body"
    s = s.replace(old, new)
    old = '''    _split_ok = plan == "biped" or str(o.get("limb_split", "on")) == "all"   # Rig path V3 (fec0161f): one position,
'''
    new = '''    _split_ok = plan == "biped" or str(o.get("limb_split", "on")) == "all"   # Rig path V3 (fec0161f): one position,
    _quad_split = plan == "quadruped" and not _split_ok and str(o.get("quad_contact_split", "on")) in ("on", "legs", "paws")         and g.n / max(len(g.of), 1) >= float(o.get("quad_split_min_welded", 0.6))   # never a triangle soup / seam-heavy
    if plan == "quadruped":                              # mesh (the boar 0.32, the tack horse 0.26): its copies diverge
        stats["welded_share"] = round(g.n / max(len(g.of), 1), 3)
'''
    assert s.count(old) == 1, "split_ok anchor"
    s = s.replace(old, new)
    old = '''    if _split_ok and str(o.get("limb_split", "on")) != "off":           # Skinning · V3: fused contacts split
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats)
'''
    new = '''    if _split_ok and str(o.get("limb_split", "on")) != "off":           # Skinning · V3: fused contacts split
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats)
    elif _quad_split:                                    # Fable 2026-10-13: a quadruped's paw | paw and leg | tail
        _qs = str(o.get("quad_contact_split", "on"))
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats,
                                                  pairs="limbs" if _qs == "on" else _qs)
        stats["quad_contact_split_faces"] = int(cm_all.any(1).sum()) if cm_all is not None else 0
'''
    assert s.count(old) == 1, "contact_split call anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: quadruped contact split")

q = d / "skin_tools.py"
t = q.read_text(encoding="utf-8")
if "quad_split_min_welded" in t:
    print("skin_tools: already patched (split)")
else:
    old = '''    "leg_column_trunk": {
'''
    new = '''    "quad_contact_split": {
        "kind": "enum", "values": ("on", "legs", "paws", "off"), "default": "on",
        "doc": "rebuild, quadruped: on (default): every leg | leg and leg | tail contact is split and capped (the cat 161 -> 85, the ferret 218 -> 51); legs: leg | leg only; paws: below the knees and no contact running along the leg. Only on a welded mesh (quad_split_min_welded): on a triangle soup / seam-heavy mesh the copies diverge (the boar 0.32: 9-40 % H gaps, boar_unwelded_gaps; the tack horse 0.26: cross_leg_edges 10 -> 46). Details: the fused contacts between two legs (paw on paw) and between a leg and the tail "
               "(a tail shell welded along the hind legs) are split like a biped's contacts and the holes capped "
               "(cut_cap v3 lids); leg | trunk is never split. Makes leg_core_own=hard viable (the cat b94999ad, the "
               "boar fec0161f, the ferret 1e079e65)."},
    "quad_split_min_welded": {
        "kind": "float", "min": 0.0, "max": 1.0, "default": 0.6,
        "doc": "rebuild, quadruped: quad_contact_split runs only when at least this share of the source vertices survive the "
               "position weld (welded / vertices; the cat 0.84, the ferret 0.86, the deer 0.94 split; the boar 0.32, the "
               "horses 0.42 / 0.26 do not)."},
    "leg_column_trunk": {
'''
    assert t.count(old) == 1, "skin_tools anchor"
    t = t.replace(old, new, 1)
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("skin_tools: quad_contact_split option")
