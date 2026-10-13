"""Fabric · V3 hooks (Fable · monsters/animals, 2026-10-13): mt/fabric.py (skirt / robe, cape, long hair chains) into
fastrig.build (after the mane: chains + weights, the fabric | limb welds split, the rig_check clip, the record) and
rig_first.animate (the chains sway in every clip).  python patch_fabric.py <mt dir>  (copies fabric.py beside it)"""
import pathlib
import shutil
import sys

d = pathlib.Path(sys.argv[1])
here = pathlib.Path(__file__).resolve().parent
src = here / "fabric.py"
if src.is_file():
    shutil.copy(src, d / "fabric.py")
    print("fabric.py copied")
p = d / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "_fab = None" in s:
    print("fastrig: already patched (fabric)")
else:
    old = '''    T["weights"] = round(time.time() - t, 3)
'''
    new = '''    _fab = None
    try:                                                 # Fabric · V3 (mt/fabric.py): skirt / robe, cape and long hair
        from . import fabric as _fb                      # as secondary chains (Fable · monsters/animals, 2026-10-13)
        _hair_m = (np.asarray(b.part) == HAIR) if getattr(b, "part", None) is not None else None
        _prop_m = np.zeros(g.n, bool)
        for _pr in props:
            _prop_m |= np.asarray(_pr[0], bool)
        _taken = np.array([str(nm).startswith("Hair_") for nm in sk.names])[w.argmax(1)]
        _fab = _fb.apply(run, b, sk, w, Rc, plan, (sopts or {}).get("fabric"), hair=_hair_m, prop=_prop_m, taken=_taken)
        if _fab and _fab["chains"]:
            w = _fab["w"]
            _mi = _st.fastrig_opts(sopts)["max_influences"] if sopts is not None else 4
            joints, weights = top4(w.astype(np.float32), _mi)
            _fm = _fab["mask"]
            if split is not None:
                split[0][_fm], split[1][_fm] = joints[_fm], weights[_fm]
            if str(_fab["options"].get("split", "on")) == "on":   # a robe welded onto the boots, hair on a shoulder
                _limb_cols = [i for i, nm in enumerate(sk.names)
                              if any(k in nm for k in ("Arm", "Hand", "Leg", "Foot", "Toe")) and not nm.startswith(("Skirt", "Cape", "Hair_"))]
                _limb_m = np.isin(w.argmax(1), _limb_cols) & ~_fm
                _alt, _cm = weld_split(g, _fm, _limb_m, w)
                if _alt is not None:
                    _aj, _aw = top4(_alt.astype(np.float32), _mi)
                    _skip = (_fm[g.eu] & _limb_m[g.ev]) | (_limb_m[g.eu] & _fm[g.ev])
                    if split is None:
                        split = (_aj, _aw, _cm, _skip)
                    else:
                        _rows = np.zeros(g.n, bool)
                        _rows[g.faces[_cm]] = True
                        split[0][_rows], split[1][_rows] = _aj[_rows], _aw[_rows]
                        split = (split[0], split[1], split[2] | _cm, split[3] | _skip)
                    _fab["stats"]["split_faces"] = int(_cm.any(1).sum())
            stats["fabric"] = _fab["stats"]
        elif _fab:
            stats["fabric"] = _fab["stats"]
            _fab = None
    except Exception as exc:                             # noqa: BLE001 - never costs the rig
        _fab = None
        print(f"[fastrig] fabric skipped: {exc!r}", file=sys.stderr)
    T["weights"] = round(time.time() - t, 3)
'''
    assert s.count(old) == 1, "weights anchor"
    s = s.replace(old, new)
    old = '''    if _mane:                                            # Mane · V3: the mane sways in the check
        anim = _mn.rig_check_extend(anim, sk, Rc)
'''
    new = '''    if _mane:                                            # Mane · V3: the mane sways in the check
        anim = _mn.rig_check_extend(anim, sk, Rc)
    if _fab:                                             # Fabric · V3: the chains swing in the check
        anim = _fb.rig_check_extend(anim, sk, Rc, (sopts or {}).get("fabric"))
'''
    assert s.count(old) == 1, "rig_check anchor"
    s = s.replace(old, new)
    old = '''    if _mane:                                            # Mane · V3: the record
        doc["mane"] = _mn.summary(_mane["stats"])
'''
    new = '''    if _mane:                                            # Mane · V3: the record
        doc["mane"] = _mn.summary(_mane["stats"])
    if _fab:                                             # Fabric · V3: the record
        doc["fabric"] = _fb.summary(_fab["stats"])
'''
    assert s.count(old) == 1, "record anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: fabric hooks")

q = d / "rig_first.py"
t = q.read_text(encoding="utf-8")
if "fabric" in t:
    print("rig_first: already patched (fabric)")
else:
    old = '''    try:                                                         # Limb stabilization · V3: the customer's pose as a clip
'''
    new = '''    try:                                                         # Fabric · V3: the skirt / cape / hair chains swing
        from . import fabric as _FAB                             # in every clip (mt/fabric.py)
        if doc.get("fabric") and clips:
            clips = _FAB.extend_clips(clips, doc, run)
    except Exception:                                            # noqa: BLE001
        pass
    try:                                                         # Limb stabilization · V3: the customer's pose as a clip
'''
    assert t.count(old) == 1, "rig_first anchor"
    t = t.replace(old, new)
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("rig_first: fabric clips")

# bone_check: fabric chains run through sheets of cloth, not the body solid: not a bone outside the mesh
bc = d / "bone_check.py"
u = bc.read_text(encoding="utf-8")
if "Skirt" in u:
    print("bone_check: already patched (fabric)")
else:
    old = '''    return [n for n in m.drawn() if not n.startswith(("Prop", "Mane", "Hair_"))]   # hair chains run through cards
'''
    new = '''    return [n for n in m.drawn() if not n.startswith(("Prop", "Mane", "Hair_", "Skirt", "Cape"))]   # hair / cloth chains
'''
    assert u.count(old) == 1, "bone_check anchor"
    u = u.replace(old, new)
    bc.write_bytes(u.replace("
", "
").encode("utf-8"))
    print("bone_check: fabric chains skipped")
