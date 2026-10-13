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
    new = '''        if plan == "quadruped" and str(o.get("head_own", "on")) != "off" and all(n in sk.index for n in ("Neck", "Head")):
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
            _headish = [i for i, nm in enumerate(sk.names) if nm in ("Neck", "Head") or nm.startswith(("HeadTop", "Nose", "Jaw", "Ear", "Eye", "Mane"))]
            _head_only = [i for i, nm in enumerate(sk.names) if nm == "Head" or nm.startswith(("HeadTop", "Nose", "Jaw", "Ear", "Eye"))]
            _others = [i for i in range(len(sk.names)) if i not in _headish and sk.deform[i] and not sk.names[i].startswith("Prop")]
            _near0 = D.argmin(1)                         # the neck's own thickness: the skin nearest the Neck / Head
            _nh = [sk.index["Neck"], sk.index["Head"]]   # bones through the air; the region is a tube of
            _rr = D[np.isin(_near0, _nh)][:, _nh].min(1)   # head_own_tube such radii (the sitting horse 74f7cd37: a
            _rad = float(np.median(_rr)) if len(_rr) >= 10 else 0.08 * H   # lowered head's half-space took the chest)
            _tube = np.minimum(seg_dist(p, _nk, _hd)[0], seg_dist(p, _hd, _ht)[0]) <= float(o.get("head_own_tube", 2.0)) * _rad
            _rows = np.nonzero((_tn > _mg) & _tube & ~prop & (part < 0))[0]
            if len(_rows) and _others:
                A[np.ix_(_rows, _others)] = False
            _rows2 = np.nonzero((_th > _mg) & _tube & ~prop & (part < 0))[0]
            _not_head = [i for i in range(len(sk.names)) if i not in _head_only and sk.deform[i] and not sk.names[i].startswith("Prop")]
            if len(_rows2) and _not_head:
                A[np.ix_(_rows2, _not_head)] = False
            _none = ~A.any(1)
            if _none.any():
                A[_none] = np.asarray(sk.deform, bool)[None, :]
            stats["head_own"] = {"neck_rows": int(len(_rows)), "head_rows": int(len(_rows2)), "radius_pct_H": round(_rad / H * 100, 2)}
        De = D.copy()
        free = part < 0                                  # no label: distances in units of each bone's own thickness,
'''
    assert s.count(old) == 1, "De anchor"
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
    "head_own_margin_pct_H": {
        "kind": "float", "min": 0.0, "max": 10.0, "default": 2.0,
        "doc": "rebuild, quadruped: head_own starts this far past each joint plane, in % of H."},
''' + t[i:]
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("skin_tools: head_own options")
