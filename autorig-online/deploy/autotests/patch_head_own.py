"""head_own (Fable, equines, 2026-10-13; the zebra d3d37033: its muzzle - 1385 vertices at 66-80 % H in front of the
Head joint - was skinned to Spine2 / Spine1, because the free-vertex assignment scales distances by each bone's
radius and the thick trunk bones outbid the thin Head; every neck clip then tore the throat: Look Around 205, Rear
493 stretched edges).  A quadruped's skin past the Neck joint along the neck axis may take only Neck / Head bones,
and past the Head joint along the head axis only the Head (the hard assignment; the joint blends stay).
Option head_own on | off (skin_tools SPEC), head_own_margin_pct_H.  python patch_head_own.py <mt dir>"""
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
p = d / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "head_own" in s:
    print("fastrig: already patched (head_own)")
else:
    old = '''        De = D.copy()
        free = part < 0                                  # no label: distances in units of each bone's own thickness,
'''
    new = '''        _head_keep = np.zeros(n, bool)
        if plan == "quadruped" and str(o.get("head_own", "on")) != "off" and all(n in sk.index for n in ("Neck", "Head")):
          try:
            # Fable · equines 2026-10-13 (the zebra d3d37033: the muzzle on Spine2 / Spine1): past the Neck joint along
            # the neck axis only the neck and the head may own skin; past the Head joint along the head axis only the
            # head (the thick trunk bones outbid the thin head in the radius-scaled distances below)
            _nk, _hd = sk.head[sk.index["Neck"]], sk.head[sk.index["Head"]]
            _ht = sk.tail[sk.index["Head"]]
            _nax = (_hd - _nk) / max(float(np.linalg.norm(_hd - _nk)), 1e-9)
            _hax = (_ht - _hd) / max(float(np.linalg.norm(_ht - _hd)), 1e-9)
            _mg = float(o.get("head_own_margin_pct_H", 2.0)) / 100.0 * H
            _tn = (p - _nk) @ _nax
            _th = (p - _hd) @ _hax
            _under = set()                               # everything hanging under the Head (the elephant's Trunk
            _hi = sk.index["Head"]                       # chain, horns, ears, a mane) stays allowed there
            for _i in range(len(sk.names)):
                _j = _i
                while _j >= 0 and _j != _hi:
                    _j = sk.parent[_j]
                if _j == _hi:
                    _under.add(_i)
            _head_only = sorted(_under | {_hi})
            _headish = sorted(_under | {_hi, sk.index["Neck"]} | {i for i, nm in enumerate(sk.names) if nm.startswith("Mane")})
            _others = [i for i in range(len(sk.names)) if i not in _headish and sk.deform[i] and not sk.names[i].startswith("Prop")]
            _near0 = D.argmin(1)                         # the neck's own thickness: the skin nearest the Neck / Head
            _nh = [sk.index["Neck"], sk.index["Head"]]   # bones through the air; the region is a tube of
            _rr = D[np.isin(_near0, _nh)][:, _nh].min(1)   # head_own_tube such radii (the sitting horse 74f7cd37: a
            _rad = float(np.median(_rr)) if len(_rr) >= 10 else 0.08 * H   # lowered head's half-space took the chest)
            _nlen = float(np.linalg.norm(_hd - _nk) + np.linalg.norm(_nk - sk.head[sk.index["Spine2"]])) if "Spine2" in sk.index else float(np.linalg.norm(_hd - _nk))
            if _rad > float(o.get("head_own_max_radius_pct_H", 10.0)) / 100.0 * H:   # a thick neck (the elephant e41336fe
                raise StopIteration(f"neck radius {_rad / H * 100:.1f} % H over {o.get('head_own_max_radius_pct_H', 10.0)}: a thick-necked animal keeps today's blends")   # 12.4, the armoured quadruped 19)
            if _nlen < float(o.get("head_own_min_neck_radii", 2.0)) * _rad:   # a short thick neck (the elephant e41336fe,
                raise StopIteration(f"neck {_nlen / H * 100:.0f} % H is under {o.get('head_own_min_neck_radii', 2.0)} radii ({_rad / H * 100:.1f} % H): no tube")
            _tube = np.minimum(seg_dist(p, _nk, _hd)[0], seg_dist(p, _hd, _ht)[0]) <= float(o.get("head_own_tube", 2.0)) * _rad
            _legs = np.ones(n, bool)                     # never the shoulders: skin inside the front legs' tubes stays
            for _sd in ("Left", "Right"):                # with the legs / trunk (the elephant e41336fe: Head took
                for _a, _b in (("FrontUpLeg", "FrontLeg"), ("FrontLeg", "FrontFoot")):   # the shoulder, rig_check 0 -> 74)
                    if f"{_sd}{_a}" in sk.index and f"{_sd}{_b}" in sk.index:
                        _ia, _ib = sk.index[f"{_sd}{_a}"], sk.index[f"{_sd}{_b}"]
                        _dl = D[_near0 == _ia, _ia]
                        _rl = float(np.clip(np.median(_dl), 0.01 * H, 0.2 * H)) if len(_dl) >= 10 else 0.05 * H
                        _legs &= seg_dist(p, sk.head[_ia], sk.head[_ib])[0] > float(o.get("head_own_leg_tube", 2.0)) * _rl
            _tube &= _legs
            _jaw = np.linalg.norm(p - _hd, axis=1) <= float(o.get("head_own_sphere", 2.5)) * _rad   # the throat under
            _rows = np.nonzero(((_tn > _mg) | _jaw) & _tube & ~prop & (part != HAIR))[0]     # the jaw is neck, not trunk
            if len(_rows) and _others:
                A[np.ix_(_rows, _others)] = False
            _rows2 = np.nonzero((_th > _mg) & _tube & ~prop & (part != HAIR))[0]
            _not_head = [i for i in range(len(sk.names)) if i not in _head_only and sk.deform[i] and not sk.names[i].startswith("Prop")]
            if len(_rows2) and _not_head:
                A[np.ix_(_rows2, _not_head)] = False
            _none = ~A.any(1)
            if _none.any():
                A[_none] = np.asarray(sk.deform, bool)[None, :]
            _head_keep[_rows2] = True                    # the skull rule below never bars the Head on this skin
            stats["head_own"] = {"neck_rows": int(len(_rows)), "head_rows": int(len(_rows2)), "radius_pct_H": round(_rad / H * 100, 2),
                                 "neck_pct_H": round(_nlen / H * 100, 1)}
          except StopIteration as _why:
            stats["head_own"] = {"why": str(_why)}
        De = D.copy()
        free = part < 0                                  # no label: distances in units of each bone's own thickness,
'''
    assert s.count(old) == 1, "De anchor"
    s = s.replace(old, new)
    old = '''        De = D.copy()
        free = part < 0                                  # no label: distances in units of each bone's own thickness,
'''
    new = '''        if "_head_keep" not in dir():
            _head_keep = np.zeros(n, bool)
        De = D.copy()
        free = part < 0                                  # no label: distances in units of each bone's own thickness,
'''
    assert s.count(old) == 1, "De anchor 2"
    s = s.replace(old, new)
    old = '''                De[free & ((p - hd) @ ax < 0), sk.index["Head"]] = np.inf'''
    new = '''                De[free & ((p - hd) @ ax < 0) & ~_head_keep, sk.index["Head"]] = np.inf   # head_own: the throat
                #                                          under the jaw is the head's (the zebra d3d37033)'''
    assert s.count(old) == 1, "skull rule anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: head_own")

q = d / "skin_tools.py"
t = q.read_text(encoding="utf-8")
if "head_own" in t:
    print("skin_tools: already patched (head_own)")
else:
    anchor = '''    "leg_column_trunk": {
'''
    i = t.rindex(anchor)
    t = t[:i] + '''    "head_own": {
        "kind": "enum", "values": ("on", "off"), "default": "on",
        "doc": "rebuild, quadruped: skin past the Neck joint along the neck axis belongs to the neck / head bones and skin "
               "past the Head joint to the head alone (hard assignment; the thick trunk bones outbid the thin head in the "
               "radius-scaled distances: the zebra d3d37033's muzzle sat on Spine2)."},
    "head_own_tube": {
        "kind": "float", "min": 1.0, "max": 6.0, "default": 2.0,
        "doc": "rebuild, quadruped: head_own acts within this many neck radii of the Neck -> Head -> nose chain (a lowered "
               "head's half-space alone took the sitting horse's chest)."},
    "head_own_sphere": {
        "kind": "float", "min": 0.0, "max": 6.0, "default": 2.5,
        "doc": "rebuild, quadruped: skin within this many neck radii of the Head joint (the jaw, the throat under it) is "
               "neck / head too (the zebra: Head | Spine2 edges at the throat tore in Look Around / Rear)."},
    "head_own_leg_tube": {
        "kind": "float", "min": 1.0, "max": 5.0, "default": 2.0,
        "doc": "rebuild, quadruped: head_own never takes skin within this many leg radii of the front legs' axes (the "
               "shoulders of a big-headed animal: the elephant e41336fe)."},
    "head_own_max_radius_pct_H": {
        "kind": "float", "min": 2.0, "max": 30.0, "default": 10.0,
        "doc": "rebuild, quadruped: head_own only for necks thinner than this (% of H): equines 6-8 %, the elephant "
               "e41336fe 12 % and the armoured quadruped a982eb2f 19 % keep today's blends (their rig_check rose 0 -> 65 "
               "and 887 -> 931 with it)."},
    "head_own_min_neck_radii": {
        "kind": "float", "min": 0.5, "max": 8.0, "default": 2.0,
        "doc": "rebuild, quadruped: head_own needs a neck (Spine2 -> Neck -> Head) at least this many neck radii long; a "
               "short thick neck (the elephant e41336fe, the armoured quadruped a982eb2f) keeps today's blends."},
    "head_own_margin_pct_H": {
        "kind": "float", "min": 0.0, "max": 10.0, "default": 2.0,
        "doc": "rebuild, quadruped: head_own starts this far past each joint plane, in % of H."},
''' + t[i:]
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("skin_tools: head_own options")
