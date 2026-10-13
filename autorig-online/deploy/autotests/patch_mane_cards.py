"""Mane v2 (Fable · monsters/animals, 2026-10-13): the mane from the thin hair cards.  v1 read the mane off the neck
skin (vertices standing off the neck's upper skin), which on the horse 31cc3ff3 took welded neck skin too and dropped
the numeric QA 6/6 -> 3/6, so it shipped off by default.  v2 takes only separate thin shells (hair cards: PCA
thickness <= card_thickness % H, a small share of the mesh) lying above the Spine1 -> Spine2 -> Neck -> Head axis,
groups them along the axis into segments and hands them to the same chains (Mane{k} > Mane{k}1).  The body skin is
never touched, so nothing tears at a seam.  Options: source cards | skin | auto; mane back to auto.
python patch_mane_cards.py <mt dir>"""
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
p = d / "mane.py"
s = p.read_text(encoding="utf-8")
_bs0 = p.read_bytes().count(bytes([8]))
if "def detect_cards" in s:
    print("mane: already patched")
else:
    old = '''    "mane": {"values": ("off", "auto", "on"), "default": "off",
             "doc": "auto: a quadruped whose neck skin stands off the axis gets mane chains; off: never"},
'''
    new = '''    "mane": {"values": ("off", "auto", "on"), "default": "auto",
             "doc": "auto: a maned quadruped (words) whose hair cards / neck skin read as a mane gets mane chains; off: never"},
    "source": {"values": ("cards", "skin", "auto"), "default": "cards",
               "doc": "cards: only separate thin shells above the neck / back axis (hair cards) are mane (v2, 2026-10-13); "
                      "skin: v1, vertices standing off the neck's upper skin (welded manes; took neck skin on the horse "
                      "31cc3ff3, QA 6/6 -> 3/6); auto: cards when any are found, else skin"},
    "card_thickness": {"min": 0.1, "max": 5.0, "default": 1.6, "doc": "a shell whose PCA thickness is at most this % of H is a card"},
    "card_max_share": {"min": 0.001, "max": 0.3, "default": 0.05, "doc": "a shell with more than this share of the mesh's vertices is body, not a card"},
    "card_axis_reach": {"min": 0.5, "max": 6.0, "default": 3.0, "doc": "a card whose centre is farther than this many local neck radii from the axis is not mane"},
'''
    assert s.count(old) == 1, "spec anchor"
    s = s.replace(old, new)

    old = '''MANE_WORDS = re.compile('''
    new = '''def detect_cards(b, sk, w, Rc, o) -> dict | None:
    """Mane v2: the thin separate shells (hair cards) above the back / neck axis -> the same structure as detect()
    (segments with vertices, root on the skin, tip, height), or None when no card reads as mane."""
    from . import fastrig as F
    ix = sk.index
    if not all(n in ix for n in NECK):
        return None
    g, p, H = b.g, b.p, b.H
    axis_names = [n for n in ("Spine1", "Spine2", "Neck", "Head") if n in ix]
    axis = np.array([sk.head[ix[n]] for n in axis_names])
    comp = F.components(g.n, g.eu, g.ev)
    cnt = np.bincount(comp)
    main = int(cnt.argmax())
    body = getattr(b, "body", None)
    cards = []
    for c in np.nonzero((cnt >= 6) & (cnt <= o["card_max_share"] * g.n))[0]:
        if c == main:
            continue
        vi = np.nonzero(comp == c)[0]
        if body is not None and not body[vi].any():               # a prop's shell is no card
            continue
        q = p[vi]
        qc = q - q.mean(0)
        ev_ = np.linalg.eigvalsh(qc.T @ qc / max(len(q), 1))
        thick = 2.0 * float(np.sqrt(max(ev_[0], 0.0)))
        if thick > o["card_thickness"] / 100.0 * H:
            continue
        cards.append(vi)
    if not cards:
        return None
    allv = np.concatenate(cards)
    t, dist, foot = _poly_param(p[allv], axis)
    rel = p[allv] - foot
    # the body's own radius along the axis (upper side), from the main shell's neck / back skin
    cols = [ix[n] for n in axis_names if n != "Head"]
    skin = (comp == main) & (w[:, cols].sum(1) >= 0.5)
    r_axis = 0.08 * H
    if skin.sum() >= 20:
        ts, ds, fs = _poly_param(p[skin], axis)
        upper = (p[skin] - fs)[:, 1] > 0
        r_axis = float(np.percentile(ds[upper], 60)) if upper.sum() >= 8 else float(np.percentile(ds, 60))
    keep_cards, mane_idx = [], []
    off = 0
    for vi in cards:
        sl = slice(off, off + len(vi))
        off += len(vi)
        up_share = float((rel[sl][:, 1] > 0).mean())
        centre_d = float(np.median(dist[sl]))
        if up_share < 0.6 or centre_d > o["card_axis_reach"] * r_axis:   # under the belly, or far off: not mane
            continue
        keep_cards.append(sl)
        mane_idx.append(vi)
    if not mane_idx:
        return None
    idx = np.concatenate(mane_idx)
    if len(idx) < o["min_vertices"] or len(idx) < o["min_share"] * len(p):
        return None
    sel = np.concatenate([np.arange(g.n)[sl] for sl in keep_cards]) if False else None   # (positions below)
    pos = np.concatenate([np.arange(sl.start, sl.stop) for sl in keep_cards])
    tt, dd, ff = t[pos], dist[pos], foot[pos]
    body_len = float(np.ptp(p[:, 2]))
    span = float(np.ptp(tt)) * float(np.linalg.norm(np.diff(axis, axis=0), axis=1).sum())
    n_seg = int(np.clip(round(span / (o["segment_pct_body"] / 100 * body_len)), 1, o["max_segments"]))
    edges = np.linspace(tt.min(), tt.max() + 1e-6, n_seg + 1)
    segments = []
    for k in range(n_seg):
        m = (tt >= edges[k]) & (tt < edges[k + 1])
        if m.sum() < 6:
            continue
        vi = idx[m]
        q = p[vi]
        height = dd[m] - r_axis
        far = q[int(np.argmax(dd[m]))]
        axis_pt = ff[m].mean(0)
        outward = far - axis_pt
        if np.linalg.norm(outward) < 1e-9:
            continue
        outward /= np.linalg.norm(outward)
        low = q[dd[m] <= np.percentile(dd[m], 20)]                   # the cards' base row sits on the skin
        root = low.mean(0) if len(low) else axis_pt + outward * r_axis
        tip = far
        if np.linalg.norm(tip - root) < 1e-9:
            continue
        segments.append({"vertices": vi, "root": root, "tip": tip, "t": float((edges[k] + edges[k + 1]) / 2),
                         "height": float(max(height.max(), 1e-6))})
    if not segments:
        return None
    return {"axis": axis, "mane": idx, "segments": segments, "vertices": int(len(idx)), "neck_radius": [r_axis] * 8,
            "segments_n": len(segments), "cards": len(mane_idx), "source": "cards"}


MANE_WORDS = re.compile('''
    assert s.count(old) == 1, "words anchor"
    s = s.replace(old, new, 1)

    old = '''    det = detect(b, sk, w, Rc, o)
    if det is None:
        return None
'''
    new = '''    det = None
    if o["source"] in ("cards", "auto"):
        try:
            det = detect_cards(b, sk, w, Rc, o)
        except Exception as exc:                                 # noqa: BLE001 - the cards never cost the rig
            det = None
            print(f"[mane] cards skipped: {exc!r}", file=sys.stderr)
    if det is None and o["source"] in ("skin", "auto"):
        det = detect(b, sk, w, Rc, o)
    if det is None:
        return None
'''
    assert s.count(old) == 1, "apply anchor"
    s = s.replace(old, new)
    old = '''    return {"w": w, "mask": mask, "stats": {"chains": chains, "vertices": det["vertices"], "bones_added": len(sk.names) - n0,
                                           "neck_radius_pct_H": [round(float(r) / b.H * 100, 2) for r in det["neck_radius"]]}}
'''
    new = '''    return {"w": w, "mask": mask, "stats": {"chains": chains, "vertices": det["vertices"], "bones_added": len(sk.names) - n0,
                                           "source": det.get("source", "skin"), "cards": det.get("cards"),
                                           "neck_radius_pct_H": [round(float(r) / b.H * 100, 2) for r in det["neck_radius"]]}}
'''
    assert s.count(old) == 1, "return anchor"
    s = s.replace(old, new)
    if "\nimport sys\n" not in s:
        s = s.replace("\nimport re\n", "\nimport re\nimport sys\n", 1)
    if "\nimport pathlib\n" not in s:
        s = s.replace("\nimport re\n", "\nimport pathlib\nimport re\n", 1)
    old = '''    return {"chains": [c["name"] for c in stats.get("chains") or []], "vertices": stats.get("vertices"),
            "bones_added": stats.get("bones_added"), "naming": "Mane{k} > Mane{k}1 along the neck, parent Spine2 / Neck / Head"}
'''
    new = '''    return {"chains": [c["name"] for c in stats.get("chains") or []], "vertices": stats.get("vertices"),
            "bones_added": stats.get("bones_added"), "source": stats.get("source"), "cards": stats.get("cards"),
            "naming": "Mane{k} > Mane{k}1 along the neck, parent Spine2 / Neck / Head"}
'''
    assert s.count(old) == 1, "summary anchor"
    s = s.replace(old, new)
    s = s.replace("\x08", "\\b")                         # prod MANE_WORDS carried backspace bytes for \b (mare, lion, лев)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0, "backspace bytes"
    print("mane: cards (v2)")

# bone_check: hair chains (Mane*, Hair_*) run through hair cards, not the body solid: not a bone outside the mesh
q = d / "bone_check.py"
t = q.read_text(encoding="utf-8")
if "Mane" in t:
    print("bone_check: already patched")
else:
    old = '''    return [n for n in m.drawn() if not n.startswith("Prop")]
'''
    new = '''    return [n for n in m.drawn() if not n.startswith(("Prop", "Mane", "Hair_"))]   # hair chains run through cards
'''
    assert t.count(old) == 1, "bone_check anchor"
    t = t.replace(old, new)
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("bone_check: hair chains skipped")
