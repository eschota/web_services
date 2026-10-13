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
if "quad_contact_split" in s:
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
    if pairs == "limbs":                                 # quadrupeds (Fable 2026-10-13): only leg | leg and leg | tail
        _lt = np.array([x.startswith(("leg", "fleg", "hleg", "tail")) for x in names])   # contacts, never leg | trunk
        cut = cut & _lt[gid[eu]] & _lt[gid[ev]]
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
    _quad_split = plan == "quadruped" and not _split_ok and str(o.get("quad_contact_split", "on")) == "on"
'''
    assert s.count(old) == 1, "split_ok anchor"
    s = s.replace(old, new)
    old = '''    if _split_ok and str(o.get("limb_split", "on")) != "off":           # Skinning · V3: fused contacts split
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats)
'''
    new = '''    if _split_ok and str(o.get("limb_split", "on")) != "off":           # Skinning · V3: fused contacts split
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats)
    elif _quad_split:                                    # Fable 2026-10-13: a quadruped's paw | paw and leg | tail
        alt_all, cm_all, skip_all = contact_split(g, sk, hard, H, w, alt_all, cm_all, skip_all, stats, pairs="limbs")
        stats["quad_contact_split_faces"] = int(cm_all.any(1).sum()) if cm_all is not None else 0
'''
    assert s.count(old) == 1, "contact_split call anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: quadruped contact split")

q = d / "skin_tools.py"
t = q.read_text(encoding="utf-8")
if "quad_contact_split" in t:
    print("skin_tools: already patched (split)")
else:
    old = '''    "leg_column_trunk": {
'''
    new = '''    "quad_contact_split": {
        "kind": "enum", "values": ("on", "off"), "default": "on",
        "doc": "rebuild, quadruped: the fused contacts between two legs (paw on paw) and between a leg and the tail "
               "(a tail shell welded along the hind legs) are split like a biped's contacts and the holes capped "
               "(cut_cap v3 lids); leg | trunk is never split. Makes leg_core_own=hard viable (the cat b94999ad, the "
               "boar fec0161f, the ferret 1e079e65)."},
    "leg_column_trunk": {
'''
    assert t.count(old) == 1, "skin_tools anchor"
    t = t.replace(old, new, 1)
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("skin_tools: quad_contact_split option")
