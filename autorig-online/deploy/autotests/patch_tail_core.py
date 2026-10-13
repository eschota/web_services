"""tail_core_own (Fable · monsters/animals, 2026-10-13; Rig classes' diagnosis: on the baby elephant 0e32ccf1 Tail3 owned
~21k vertices while the true tail is a 0.7 % H tube; the boar's hind legs sat on Tail3).  The counterpart of
leg_core_own: tail bones may own only the vertices inside the tail tube (the traced tail's radius, mt/tail_trace.py,
or the tip's own thickness when the trace was not used) around the Tail -> Tail_End chain, plus a short blend at the
root; everything else goes to Hips / the hind legs by distance.  Hard stage (allowed bones) and the smoothed weights.
Options (skin_tools SPEC): tail_core_own on | off, tail_tube (radii), tail_tube_min_pct_H, tail_root_blend_pct_H.
python patch_tail_core.py <mt dir>"""
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
p = d / "fastrig.py"
s = p.read_text(encoding="utf-8")
if "def tail_tube_mask" in s:
    print("fastrig: already patched (tail)")
else:
    old = '''def leg_regions(b, J, pinned=()):
'''
    new = '''def tail_tube_mask(b, sk, D, H, o):
    """tail_core_own (Fable 2026-10-13): the vertices a tail bone may own: inside the tail tube around the Tail ->
    Tail_End chain (radius: the traced tail's ring radius when the trace was used, else the tip's own thickness), plus
    the root blend around the Tail head. -> (mask, tail columns, info) or (None, [], info)."""
    names = sk.names
    cols = [i for i, nm in enumerate(names) if nm.startswith("Tail") and sk.deform[i]]
    if not cols or "Tail" not in sk.index:
        return None, [], {"why": "no tail bones"}
    p = b.p
    chain = [sk.head[sk.index[n]] for n in ("Tail", "Tail1", "Tail2", "Tail3") if n in sk.index]
    last = max(cols, key=lambda i: names[i])
    chain.append(sk.tail[last])
    tube = np.full(len(p), np.inf)
    for a_, e_ in zip(chain[:-1], chain[1:]):
        tube = np.minimum(tube, seg_dist(p, a_, e_)[0])
    tt = getattr(b, "tail_trace", None) or {}
    r, src = None, "trace"
    if tt.get("used") and tt.get("radius_pct_H"):
        r = float(tt["radius_pct_H"]) / 100.0 * H
    if r is not None and r > float(o.get("tail_fat_pct_H", 10.0)) / 100.0 * H:   # a fat tail (the cartoon monster
        return None, cols, {"why": f"a fat tail ({r / H * 100:.1f} % H): its rings are the tail, nothing to strip",   # 48db2b25:
                            "radius_pct_H": round(r / H * 100, 2)}                                      # 16.7 % H, 62 % body)
    if r is None and str(o.get("tail_core_own", "trace")) != "always":
        return None, cols, {"why": "the tail trace was not used: no tube (tail_core_own=always estimates one)",
                            "trace": {k: tt.get(k) for k in ("why", "short", "shell_rejected")}}
    if r is None:                                        # the trace was not used: the thickness of what lies behind
        src = "behind"                                   # the chain's third joint (a fin whose axis zig-zagged), or,
        short = bool(tt.get("short")) or "body lengths" in str(tt.get("why") or "")   # when the trace saw only a
        behind = np.asarray(b.body, bool) & (p[:, 2] < chain[min(2, len(chain) - 2)][2])   # stub (the elephant's
        if behind.sum() >= 8 and not short:              # tassel: 0.019 body lengths), the tip's own thickness
            r = float(np.median(tube[behind]))
        else:
            tip = chain[-1]
            near_tip = np.linalg.norm(p - tip, axis=1) < 0.08 * H
            src = "tip"
            r = float(np.median(seg_dist(p[near_tip], chain[-2], chain[-1])[0])) if near_tip.sum() >= 8 else 0.02 * H
        r = min(float(r), float(o.get("tail_max_radius_pct_H", 15.0)) / 100.0 * H)
    r = max(float(r), 0.005 * H)
    reach = float(o.get("tail_tube", 2.5)) * r + float(o.get("tail_tube_min_pct_H", 1.0)) / 100.0 * H
    root = np.linalg.norm(p - chain[0], axis=1) < float(o.get("tail_root_blend_pct_H", 6.0)) / 100.0 * H
    mask = (tube <= reach) | root
    # the geodesic tail (mt/tail_trace.py's own rings, kept here as a mask): from the extremity behind the hips, rings
    # of equal surface distance grow until a ring widens into the body; a fin, a tassel or a thin tube alike
    geo_n, geo_why = 0, "off"
    try:
        if str(o.get("tail_geodesic", "off")) == "off":
            raise StopIteration
        g = b.g
        body = np.asarray(b.body, bool)
        hips = sk.head[sk.index["Hips"]] if "Hips" in sk.index else chain[0]
        zlen = float(np.ptp(p[body, 2])) if body.any() else H
        step = float(o.get("tail_ring_pct_body", 2.5)) / 100.0 * zlen
        cand = body & (p[:, 2] < hips[2] - 0.05 * zlen)
        if getattr(b, "gait_leg_mask", None) is not None:
            cand &= ~b.gait_leg_mask
        topo = getattr(b, "leg_topo", None) or {}
        if topo.get("on_leg") is not None:               # the legs' own skin is never the tail
            cand &= ~np.asarray(topo["on_leg"], bool)
        seeds = [int(i) for i in np.argsort(np.linalg.norm(p - hips, axis=1))[:200] if body[i]][:20]
        if cand.sum() >= 20 and seeds:
            dh, _ = _dijkstra(g, np.array(seeds), np.zeros(len(seeds), np.int64))
            dc = np.where(cand & np.isfinite(dh), dh, -1.0)
            tip = int(np.argmax(dc))
            if dc[tip] > 0.05 * zlen:
                dt, _ = _dijkstra(g, np.array([tip]), np.zeros(1, np.int64), limit=float(dc[tip]) * 1.2)
                radii, s_ = [], 0.0
                stop = None
                centres, stalled = [], 0
                while s_ < dc[tip]:
                    ring = body & (np.abs(dt - s_) < step / 2)
                    if ring.sum() >= 3:
                        c = p[ring].mean(0)
                        rr = float(np.median(np.linalg.norm(p[ring] - c, axis=1)))
                        if centres and np.linalg.norm(c - centres[-1]) < float(o.get("tail_ring_advance", 0.4)) * step:
                            stalled += 1                 # the rings spread over the rump instead of advancing
                            if stalled >= 2:             # along a tail (the elephant: 16.7k rump vertices)
                                stop = s_; geo_why = "stalled"; break
                        else:
                            stalled = 0
                        centres.append(c)
                        if len(radii) >= 3 and rr > float(o.get("tail_ring_widen", 2.0)) * float(np.max(radii[-2:])):
                            # against the last rings, not the median: a fin tapers (its rings grow steadily), the
                            # body is a jump (the axolotl's fin ended at ring 5 of 20 against the median)
                            stop = s_; geo_why = "widened"; break
                        if c[2] > hips[2] - 0.02 * zlen:
                            stop = s_; geo_why = "reached the hips"; break
                        radii.append(rr)
                    s_ += step
                if stop is None:
                    stop, geo_why = s_, "ran out"
                geo = body & np.isfinite(dt) & (dt < stop)
                if len(radii) >= 2 and geo.sum() >= 6:
                    mask |= geo
                    geo_n = int(geo.sum())
                    if src == "tip":                     # the rings know the thickness better than the chain's tip
                        r = float(np.median(radii))
    except StopIteration:
        pass
    except Exception as exc:                             # noqa: BLE001 - the tube and the root blend stand
        geo_why = f"{type(exc).__name__}: {exc}"[:80]
    # a wide thin tail (the axolotl's fin, a beaver's paddle): thin skin behind the tail root near the chain is tail
    # too, however far from the axis; the croup and the legs are thick and stay out
    thin_lim = float(o.get("tail_thin_pct_H", 6.0)) / 100.0
    far = float(o.get("tail_fin_reach_pct_H", 30.0)) / 100.0 * H
    fin_n = 0
    if thin_lim > 0:
        cand = np.nonzero((tube <= far) & ~mask & (p[:, 2] < chain[0][2]) & np.asarray(b.body, bool))[0]
        if len(cand):
            th = surface_thickness(b.g, H, query=cand, limit=thin_lim)
            fin = cand[np.isfinite(th[cand]) & (th[cand] <= thin_lim * H)]
            mask[fin] = True
            fin_n = int(len(fin))
    return mask, cols, {"radius_pct_H": round(r / H * 100, 2), "reach_pct_H": round(reach / H * 100, 2), "source": src,
                        "tube_vertices": int((tube <= reach).sum()), "root_vertices": int(root.sum()), "fin_vertices": fin_n,
                        "geodesic_vertices": geo_n, "geodesic": geo_why}


def leg_regions(b, J, pinned=()):
'''
    assert s.count(old) == 1, "leg_regions anchor"
    s = s.replace(old, new)
    # hard stage: after the leg topology rules, before the column rule
    old = '''        if plan == "quadruped" and str(o.get("leg_column_trunk", "off")) != "off" and getattr(b, "leg_column", None):
'''
    new = '''        _tail_mask, _tail_cols, _tail_info = None, [], {}
        if plan == "quadruped" and str(o.get("tail_core_own", "trace")) != "off":   # Fable 2026-10-13: the tail bones own
            try:                                         # only the tail tube (+ root blend), never the croup or the legs
                _tail_mask, _tail_cols, _tail_info = tail_tube_mask(b, sk, D, H, o)
                if _tail_mask is not None:
                    _rows = np.nonzero(~_tail_mask & ~prop)[0]
                    A[np.ix_(_rows, _tail_cols)] = False
                    _front = [sk.index[nm] for nm in ("Spine", "Spine1", "Spine2", "Neck", "Head") if nm in sk.index]
                    if _front and "Hips" in sk.index:     # what the tail loses behind the hips goes to the hind
                        _back = _rows[p[_rows, 2] < sk.head[sk.index["Hips"]][2]]   # half, never the shoulders
                        A[np.ix_(_back, _front)] = False
                    _none = ~A.any(1)
                    if _none.any():
                        A[_none] = np.asarray(sk.deform, bool)[None, :]
                    _tail_info["hard_rows_barred"] = int(len(_rows))
            except Exception as exc:                     # noqa: BLE001 - never costs the rig
                _tail_mask, _tail_info = None, {"error": f"{type(exc).__name__}: {exc}"[:120]}
            stats["tail_core_own"] = _tail_info
        if plan == "quadruped" and str(o.get("leg_column_trunk", "off")) != "off" and getattr(b, "leg_column", None):
'''
    assert s.count(old) == 1, "hard stage anchor"
    s = s.replace(old, new)
    # the smoothed weights: after isolate_legs
    old = '''    if plan == "quadruped" and getattr(b, "leg_topo", None):   # Astra TODO horse: after every blend and smoothing step
'''
    new = '''    if plan == "quadruped" and stats.get("tail_core_own") and _tail_mask is not None and _tail_cols:
        _out = ~_tail_mask & ~prop & (w[:, _tail_cols].sum(1) > 1e-6)   # Fable 2026-10-13: tail weights outside the tube
        if _out.any():                                   # go to Hips / the hind legs (the nearest of them through the air)
            _idx = np.nonzero(_out)[0]
            w[np.ix_(_idx, _tail_cols)] = 0
            _s = w[_idx].sum(1)
            _lost = _idx[_s <= 1e-6]
            if len(_lost):
                _alt = [sk.index[nm] for nm in ("Hips", "LeftHindUpLeg", "RightHindUpLeg") if nm in sk.index]
                if _alt:
                    w[_lost[:, None], np.array(_alt)[D[np.ix_(_lost, _alt)].argmin(1)][:, None]] = 1.0
            _s = w[_idx].sum(1, keepdims=True)
            w[_idx] = np.where(_s > 1e-6, w[_idx] / np.maximum(_s, 1e-12), w[_idx])
            stats["tail_core_own"]["weights_removed"] = int(len(_idx))
    if plan == "quadruped" and getattr(b, "leg_topo", None):   # Astra TODO horse: after every blend and smoothing step
'''
    assert s.count(old) == 1, "weights anchor"
    s = s.replace(old, new)
    p.write_bytes(s.replace("\r\n", "\n").encode("utf-8"))
    assert p.read_bytes().count(bytes([8])) == 0
    print("fastrig: tail_core_own")

q = d / "skin_tools.py"
t = q.read_text(encoding="utf-8")
if "tail_core_own" in t:
    print("skin_tools: already patched (tail)")
else:
    anchor = '''    "leg_column_trunk": {
'''
    i = t.rindex(anchor)
    t = t[:i] + '''    "tail_core_own": {
        "kind": "enum", "values": ("trace", "always", "off"), "default": "trace",
        "doc": "rebuild, quadruped: the tail bones own only the tail tube around the Tail -> Tail_End chain plus the root "
               "blend; croup / hind-leg skin they held goes to Hips / the hind legs. trace (default): only when "
               "mt/tail_trace.py's trace was used (its ring radius is the tube: cat 881 -> 294 tail vertices, horse "
               "rig_check_4x 40 -> 0). always: a tail the trace rejected gets an estimated radius (a stub: the tip's "
               "thickness; else the thickness behind the chain) - measured 2026-10-13: baby elephant 0e32ccf1 Tail3 "
               "27k -> 5.7k vertices, rig_check_4x 4029 -> 2036, but the ferret 59 -> 326 and the boar's welded legs "
               "tear (288 -> 580), so not the default until the trace is right (Rig classes)."},
    "tail_fat_pct_H": {
        "kind": "float", "min": 2.0, "max": 40.0, "default": 10.0,
        "doc": "rebuild, quadruped: a traced tail thicker than this (% of H) is a fat tail whose rings are the tail itself: "
               "tail_core_own strips nothing (the cartoon monster 48db2b25, 16.7 % H)."},
    "tail_tube": {
        "kind": "float", "min": 1.0, "max": 6.0, "default": 2.5,
        "doc": "rebuild, quadruped: the tail tube's reach in tail radii (tail_core_own; 2.5 keeps the axolotl's fin)."},
    "tail_tube_min_pct_H": {
        "kind": "float", "min": 0.0, "max": 10.0, "default": 1.0,
        "doc": "rebuild, quadruped: added to the tail tube's reach, in % of H (a thin tail still keeps its skin)."},
    "tail_root_blend_pct_H": {
        "kind": "float", "min": 0.0, "max": 30.0, "default": 6.0,
        "doc": "rebuild, quadruped: around the Tail head the tail bones may still blend into the croup, in % of H."},
    "tail_thin_pct_H": {
        "kind": "float", "min": 0.0, "max": 15.0, "default": 0.0,
        "doc": "rebuild, quadruped: skin thinner than this (% of H, surface_thickness) behind the tail root and within "
               "tail_fin_reach of the chain is tail too (the axolotl's fin); 0 turns the fin rule off."},
    "tail_ring_pct_body": {
        "kind": "float", "min": 1.0, "max": 8.0, "default": 2.5,
        "doc": "rebuild, quadruped: the geodesic tail rings' step from the extremity, in % of the body length (tail_core_own)."},
    "tail_geodesic": {
        "kind": "enum", "values": ("on", "off"), "default": "off",
        "doc": "rebuild, quadruped: on: the tail mask also takes the geodesic rings grown from the extremity behind the hips "
               "(tail_trace's rule); measured 2026-10-13: it kept the axolotl's fin but climbed the boar's and the ferret's "
               "legs and the cat's rump, so off."},
    "tail_max_radius_pct_H": {
        "kind": "float", "min": 1.0, "max": 40.0, "default": 15.0,
        "doc": "rebuild, quadruped: the estimated tail radius (trace not used) is capped here, in % of H."},
    "tail_ring_advance": {
        "kind": "float", "min": 0.0, "max": 1.0, "default": 0.4,
        "doc": "rebuild, quadruped: a tail ring whose centre advances less than this share of the ring step (twice in a "
               "row) has spread over the rump: the tail ends there."},
    "tail_ring_widen": {
        "kind": "float", "min": 1.3, "max": 4.0, "default": 2.0,
        "doc": "rebuild, quadruped: a ring this many times wider than the median before it is the body: the tail ends there."},
    "tail_fin_reach_pct_H": {
        "kind": "float", "min": 0.0, "max": 100.0, "default": 30.0,
        "doc": "rebuild, quadruped: how far from the tail chain the fin rule looks, in % of H."},
''' + t[i:]
    q.write_bytes(t.replace("\r\n", "\n").encode("utf-8"))
    print("skin_tools: tail options")
