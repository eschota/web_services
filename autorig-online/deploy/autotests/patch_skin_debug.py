"""Skin stage dump (diagnostic for the quadruped weights, Fable 2026-10-13): option skin_debug_dump=<path.npz> (or env
FASTRIG_SKIN_DUMP) writes part labels, every stage's hard label (euclid, geodesic, limb_reach), the final top bone and
the leg_regions masks. python patch_skin_debug.py <mt dir>"""
import pathlib, sys
p = pathlib.Path(sys.argv[1]) / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "_skin_dump" in s:
    print("fastrig: already patched"); sys.exit(0)
old1 = '''        hard = np.where(np.isfinite(De.min(1)), De.argmin(1), D.argmin(1))
        if str(o.get("assign_mode", "geodesic")) == "geodesic" and plan != "root":   # Skinning V3: geodesic assignment
            hard, stats["geodesic_assign"] = geodesic_assign(g, sk, hard, D, A, H)
        if str(o.get("limb_reach", "on")) != "off" and plan in ("biped", "quadruped"):   # Skinning · V3
            stats["limb_reach_moved"] = limb_reach(g, sk, hard, D, De, H, float(o.get("limb_capture", 1.6)), plan)
'''
new1 = '''        hard = np.where(np.isfinite(De.min(1)), De.argmin(1), D.argmin(1))
        _skin_dump = {"part": part.copy(), "hard_euclid": hard.copy(), "allowed_count": A.sum(1)}
        if str(o.get("assign_mode", "geodesic")) == "geodesic" and plan != "root":   # Skinning V3: geodesic assignment
            hard, stats["geodesic_assign"] = geodesic_assign(g, sk, hard, D, A, H)
        _skin_dump["hard_geodesic"] = hard.copy()
        if str(o.get("limb_reach", "on")) != "off" and plan in ("biped", "quadruped"):   # Skinning · V3
            stats["limb_reach_moved"] = limb_reach(g, sk, hard, D, De, H, float(o.get("limb_capture", 1.6)), plan)
        _skin_dump["hard_reach"] = hard.copy()
'''
assert s.count(old1) == 1
s = s.replace(old1, new1)
old2 = '''    joints, weights = top4(w.astype(np.float32), o["max_influences"])   # Rig tools V3
    split = None
'''
new2 = '''    _dump_to = str(o.get("skin_debug_dump", "") or os.environ.get("FASTRIG_SKIN_DUMP", ""))
    if _dump_to and plan != "root":                      # diagnostic: every stage of the assignment (Fable 2026-10-13)
        try:
            _d = {k: np.asarray(v) for k, v in _skin_dump.items()}
            _d["top"] = w.argmax(1); _d["w"] = w.astype(np.float32); _d["names"] = np.array(sk.names, object)
            _d["p"] = p; _d["H"] = np.array(H); _d["eu"] = g.eu; _d["ev"] = g.ev; _d["heads"] = sk.head; _d["tails"] = sk.tail; _d["body"] = np.asarray(b.body); _d["prop"] = prop
            _topo = getattr(b, "leg_topo", None) or {}
            for _k, _reg in (_topo.get("legs") or {}).items():
                _d[f"leg_core_{_k}"] = _reg["core"]; _d[f"leg_upper_{_k}"] = _reg["upper"]
            for _k in ("on_leg", "hanging", "tail"):
                if _topo.get(_k) is not None:
                    _d[f"topo_{_k}"] = np.asarray(_topo[_k])
            np.savez_compressed(_dump_to, **_d)
        except Exception as exc:                         # noqa: BLE001 - a dump never costs the rig
            stats["skin_debug_dump_error"] = f"{type(exc).__name__}: {exc}"[:120]
    joints, weights = top4(w.astype(np.float32), o["max_influences"])   # Rig tools V3
    split = None
'''
assert s.count(old2) == 1
s = s.replace(old2, new2)
old3 = '''    o = _st.fastrig_opts(opts)
    jm = _st.joint_mult(sk, o)
'''
new3 = '''    o = _st.fastrig_opts(opts)
    if os.environ.get("FASTRIG_SKIN_OPTS"):              # diagnostic runs: option overrides as JSON (Fable 2026-10-13)
        try:
            o.update(json.loads(os.environ["FASTRIG_SKIN_OPTS"]))
        except ValueError:
            pass
    jm = _st.joint_mult(sk, o)
'''
assert s.count(old3) == 1, "opts anchor"
s = s.replace(old3, new3)
if "\nimport os\n" not in s:
    s = s.replace("\nimport json\n", "\nimport json\nimport os\n", 1)
p.write_text(s, encoding="utf-8")
assert p.read_bytes().count(bytes([8])) == 0
print("fastrig: skin debug dump")
