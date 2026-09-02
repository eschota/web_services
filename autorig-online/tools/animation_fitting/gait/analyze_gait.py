# -*- coding: utf-8 -*-
"""Quantitative gait analysis of color-coded horse legs in a generated walk video. v3

v3 = v2 report functionality (segmentation -> hoof tracks -> contacts -> duty /
footfall / drift / closure, gait_chart.png, hoof_trajectories.png,
hooves_overlay_frame12.png, hoof_tracks.npz, metrics.json)
     + period detection on continuous (non-looped) walk footage
     + steady-state (post-acceleration) start detection
     + best N-frame window selection (closure after retiming, completeness,
       4-beat order)
     + retimed cycle export (<basename>_cycle<N>.mp4, h264 yuv420p 25 fps 768x448)
       and seam_check.png (start | end | difference)
     + accept / rework verdict.
v3.1 = amount-of-motion metrics + static-render guard:
     motion_metrics() (frame difference globally / on the dilated horse silhouette /
     per leg), ffprobe_motion() (P-frame vs I-frame packet size cross-check),
     hard verdict rule "motion_present", motion_profile.png, metrics.json["motion"].

CLI (argv-compatible with v2: positional <video> <out_dir>):
    python analyze_gait.py <video> <out_dir> [--target-frames 49] [--period-min 12]
        [--period-max 60] [--steady-thr 4.0] [--closure-thr 6.0] [--cycles M]
        [--resample blend|nearest] [--no-export] [--fps 25] [--size 768x448]

Loop convention (as in the 19b reference clip): the exported clip has N frames and
its LAST frame repeats the pose of its FIRST frame, i.e. exactly one period (or an
integer number of periods) is mapped onto N-1 frame intervals.  closure_px is the
sum of the distances of the 4 hooves + body centroid between output frame 0 and
output frame N-1 (measured on the retimed, sub-frame interpolated trajectories).

Palette: linear RGB from horse_2.v1.json, converted to sRGB here.
  fore_near = cyan (0,237,255)   fore_far = blue (97,129,255)
  hind_near = yellow (255,221,39) hind_far = pink (255,80,196)

Contact logic (v3 generalises v2 to treadmill stance):
  * velocities via np.gradient on the LINEAR timeline (no circular wrap);
  * "ground-level run" = |vy| < VY_THR px/frame for >= 3 frames (horizontal drift
    of the hoof during stance is allowed - an in-place walk slides the planted
    hoof backwards);
  * ground is a per-leg LINE y(x) least-squares fitted through the lowest run,
    iteratively merging further runs whose residual is <= GROUND_MERGE_TOL
    (recovers v2's two-anchor line for static-stance clips);
  * contact = vertically stationary AND within Y_TOL of the ground line at the
    hoof's x.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys

import cv2
import numpy as np

# ---------------------------------------------------------------- palette
PALETTE_LINEAR = {
    "body":      (0.46, 0.50, 0.56),
    "fore_near": (0.00, 0.85, 1.00),
    "fore_far":  (0.12, 0.22, 1.00),
    "hind_near": (1.00, 0.72, 0.02),
    "hind_far":  (1.00, 0.08, 0.55),
}
LEGS = ["fore_near", "fore_far", "hind_near", "hind_far"]
LATERAL = ["hind_near", "fore_near", "hind_far", "fore_far"]
DIAGONAL = ["hind_near", "fore_far", "hind_far", "fore_near"]
LEG_LABEL = {"fore_near": "fore_near (cyan)", "fore_far": "fore_far (blue)",
             "hind_near": "hind_near (yellow)", "hind_far": "hind_far (pink)"}

DEFAULT_VIDEO = r"R:\ComfyUI-data\autorig-fitting\candidates\horse_walk_19b.mp4"
DEFAULT_OUT = r"R:\ComfyUI-data\autorig-fitting\gait_analysis"

# ---------------------------------------------------------------- parameters
PARAMS = {
    "leg_dist_max": 110.0,        # max RGB distance to a leg colour
    "body_dist_max": 45.0,
    "min_comp_area": 25,
    "min_total_area_frac": 0.35,  # frame is "invalid" for a leg below this * median area
    "vy_thr": 1.2,                # px/frame: |vy| below = vertically stationary hoof
    "plateau_min_len": 3,
    "y_tol": 5.0,                 # px distance to ground line for contact
    "ground_merge_tol": 8.0,      # px residual to merge another run into the ground line
    "ground_slope_max": 1.0,      # |slope| guard for the fitted ground line
    "min_signal_std": 1.0,        # px: signals with smaller std are frozen -> excluded from ACF
    "acf_min_overlap": 8,         # frames of overlap required for the largest lag
    "subharmonic_tol": 0.85,      # prefer the smallest ACF peak >= tol * global peak
    "steady_spike_factor": 3.0,   # steady window: p80(err) <= thr and max(err) <= factor*thr
    "duty_range": (0.45, 0.75),
    "drift_cv_max": 0.5,
    "drift_min_abs": 0.05,        # px/frame: |mean dx| below this = drift undefined
    "vel_mismatch_weight": 1.0,   # cost weight of hoof velocity mismatch at the seam
    "missing_leg_penalty": 40.0,  # cost per leg that does not swing inside the window
    "order_penalty": 25.0,        # cost if the 4-beat lateral order is broken
    # --- amount of motion / static-render guard (v3.1)
    "sil_dilate_px": 2,           # dilation radius of the body+legs silhouette for the fg difference
    "fg_pix_thr": 8.0,            # grey levels: silhouette pixel with |diff| above = "moving" pixel
    "static_fg_frac_thr": 0.02,   # frame pair is static when its silhouette moving-pixel fraction is below
    "leg_move_px": 0.5,           # px/frame: leg mask centroid or hoof shift above = leg moves
    "static_frame_ratio_max": 0.2,  # hard rule: at most this fraction of frame pairs may be static
    "leg_moving_ratio_min": 0.3,    # hard rule: every leg must move in >= this fraction of frame pairs
    "ffprobe_static_ratio": 0.05,   # cross-check: mean P-frame bytes / I-frame bytes below = static hint
}


def lin_to_srgb(c):
    c = np.asarray(c, dtype=np.float64)
    lo = c * 12.92
    hi = 1.055 * np.power(np.clip(c, 1e-9, None), 1.0 / 2.4) - 0.055
    return np.where(c <= 0.0031308, lo, hi)


PALETTE_SRGB = {k: np.round(lin_to_srgb(v) * 255).astype(np.float32)
                for k, v in PALETTE_LINEAR.items()}  # RGB order
LEG_BGR = {leg: tuple(int(c) for c in PALETTE_SRGB[leg][::-1]) for leg in LEGS}
KERNEL = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))


# ======================================================================
#  1. segmentation -> hoof trajectories -> contacts (reusable functions)
# ======================================================================
def load_frames(video):
    cap = cv2.VideoCapture(video)
    frames = []
    while True:
        ok, f = cap.read()
        if not ok:
            break
        frames.append(cv2.cvtColor(f, cv2.COLOR_BGR2RGB))
    cap.release()
    if not frames:
        raise RuntimeError("no frames decoded from %s" % video)
    return np.stack(frames)  # (T, H, W, 3) uint8 RGB


def estimate_background(frame0):
    border = np.concatenate([
        frame0[:8].reshape(-1, 3), frame0[-8:].reshape(-1, 3),
        frame0[:, :8].reshape(-1, 3), frame0[:, -8:].reshape(-1, 3),
    ]).astype(np.float32)
    return np.percentile(border, 85, axis=0), np.percentile(border, 10, axis=0)


def segment_video(frames, params=PARAMS, return_masks=False):
    """Per-frame colour classification -> raw hoof positions, leg areas, body centroid.

    Returns hoof (4,T,2) float (NaN where a leg is not found), areas (4,T), body (T,2).
    Hoof = bottom-most pixels (ymax-4 .. ymax) of the leg mask: x = median, y = mean.
    With return_masks=True a 4th value is returned: cleaned label map (T,H,W) uint8,
    0..3 = legs in LEGS order, 4 = body (components >= min_comp_area), 255 = background.
    """
    T, H, W = frames.shape[:3]
    bg_light, bg_dark = estimate_background(frames[0])
    class_colors = np.stack([PALETTE_SRGB[k] for k in LEGS]
                            + [PALETTE_SRGB["body"], bg_light, bg_dark]).astype(np.float32)
    hoof = np.full((len(LEGS), T, 2), np.nan)
    areas = np.zeros((len(LEGS), T))
    body = np.full((T, 2), np.nan)
    masks = np.full((T, H, W), 255, np.uint8) if return_masks else None
    for t in range(T):
        img = frames[t].astype(np.float32)
        d = np.linalg.norm(img[None] - class_colors[:, None, None, :], axis=-1)
        lbl = np.argmin(d, axis=0)
        dmin = np.min(d, axis=0)
        for li in range(len(LEGS)):
            m = ((lbl == li) & (dmin < params["leg_dist_max"])).astype(np.uint8)
            m = cv2.morphologyEx(m, cv2.MORPH_OPEN, KERNEL)
            n, cc, stats, _ = cv2.connectedComponentsWithStats(m, 8)
            keep = np.zeros_like(m)
            for ci in range(1, n):
                if stats[ci, cv2.CC_STAT_AREA] >= params["min_comp_area"]:
                    keep[cc == ci] = 1
            a = int(keep.sum())
            areas[li, t] = a
            if a > 0:
                ys, xs = np.nonzero(keep)
                ymax = ys.max()
                sel = ys >= ymax - 4
                hoof[li, t] = (float(np.median(xs[sel])), float(np.mean(ys[sel])))
                if return_masks:
                    masks[t][keep == 1] = li
        mb = ((lbl == len(LEGS)) & (dmin < params["body_dist_max"])).astype(np.uint8)
        mb = cv2.morphologyEx(mb, cv2.MORPH_OPEN, KERNEL)
        n, cc, stats, cents = cv2.connectedComponentsWithStats(mb, 8)
        if n > 1:
            ci = 1 + np.argmax(stats[1:, cv2.CC_STAT_AREA])
            body[t] = cents[ci]
            if return_masks:
                big = np.zeros_like(mb)
                for bi in range(1, n):
                    if stats[bi, cv2.CC_STAT_AREA] >= params["min_comp_area"]:
                        big[cc == bi] = 1
                masks[t][(big == 1) & (masks[t] == 255)] = len(LEGS)
    if return_masks:
        return hoof, areas, body, masks
    return hoof, areas, body


def fill_invalid(hoof, areas, params=PARAMS):
    """Mark frames where a leg is (partly) occluded and fill them by linear interpolation
    on the LINEAR timeline (edge values held).  Returns filled copy, valid mask, dict of
    interpolated frame indices."""
    T = hoof.shape[1]
    hoof = hoof.copy()
    med_area = np.median(areas, axis=1)
    valid = np.zeros((len(LEGS), T), dtype=bool)
    interp_frames = {leg: [] for leg in LEGS}
    idx = np.arange(T)
    for li, leg in enumerate(LEGS):
        valid[li] = (areas[li] >= params["min_total_area_frac"] * med_area[li]) & ~np.isnan(hoof[li, :, 0])
        v = valid[li]
        if v.all():
            continue
        interp_frames[leg] = idx[~v].tolist()
        if not v.any():
            hoof[li] = 0.0
            continue
        for dim in range(2):
            hoof[li, ~v, dim] = np.interp(idx[~v], idx[v], hoof[li, v, dim])
    return hoof, valid, med_area, interp_frames


def fill_nan_series(a):
    a = np.asarray(a, dtype=float).copy()
    ok = ~np.isnan(a)
    if ok.all():
        return a
    if not ok.any():
        return np.zeros_like(a)
    idx = np.arange(len(a))
    a[~ok] = np.interp(idx[~ok], idx[ok], a[ok])
    return a


def smooth_tracks(hoof):
    """Light 3-tap smoothing on the LINEAR timeline (ends handled by shrink)."""
    hoof_s = hoof.copy()
    for li in range(hoof.shape[0]):
        for dim in range(2):
            a = hoof[li, :, dim]
            s = a.copy()
            if len(a) >= 3:
                s[1:-1] = (a[:-2] + a[1:-1] + a[2:]) / 3.0
                s[0] = (a[0] + a[1]) / 2.0
                s[-1] = (a[-2] + a[-1]) / 2.0
            hoof_s[li, :, dim] = s
    return hoof_s


def runs(mask):
    out, s = [], None
    for i, m in enumerate(mask):
        if m and s is None:
            s = i
        elif not m and s is not None:
            out.append((s, i - s)); s = None
    if s is not None:
        out.append((s, len(mask) - s))
    return out


def linear_intervals(c):
    return [(int(s), int(L)) for s, L in runs(np.asarray(c, bool))]


def fit_ground_line(point_runs, params=PARAMS):
    """point_runs: list of (xs, ys) arrays ordered lowest (largest median y) first.
    Least-squares line through the lowest run; other runs are merged in when their
    median residual is within ground_merge_tol.  Returns slope, intercept, used idx."""
    def fit(xs, ys):
        if xs.max() - xs.min() >= 8.0:
            a, b = np.polyfit(xs, ys, 1)
            if abs(a) <= params["ground_slope_max"]:
                return float(a), float(b)
        return 0.0, float(np.median(ys))

    used = [0]
    a, b = fit(*point_runs[0])
    for i in range(1, len(point_runs)):
        x2, y2 = point_runs[i]
        if abs(float(np.median(y2 - (a * x2 + b)))) <= params["ground_merge_tol"]:
            used.append(i)
            xs = np.concatenate([point_runs[j][0] for j in used])
            ys = np.concatenate([point_runs[j][1] for j in used])
            a, b = fit(xs, ys)
    return a, b, used


def compute_contacts(hoof_s, params=PARAMS):
    """Velocities, ground-level runs, per-leg ground line, contact mask."""
    nL, T = hoof_s.shape[0], hoof_s.shape[1]
    vx = np.zeros((nL, T)); vy = np.zeros((nL, T))
    for li in range(nL):
        vx[li] = np.gradient(hoof_s[li, :, 0]) if T > 1 else 0.0
        vy[li] = np.gradient(hoof_s[li, :, 1]) if T > 1 else 0.0
    speed = np.hypot(vx, vy)
    ground_model, plateaus_all = {}, {}
    contact = np.zeros((nL, T), dtype=bool)
    for li, leg in enumerate(LEGS):
        stat = np.abs(vy[li]) < params["vy_thr"]
        plats = []
        for s, L in runs(stat):
            if L >= params["plateau_min_len"]:
                xs = hoof_s[li, s:s + L, 0]; ys = hoof_s[li, s:s + L, 1]
                plats.append({"start": int(s), "len": int(L),
                              "x": float(np.median(xs)), "y": float(np.median(ys)),
                              "x_span": float(xs.max() - xs.min())})
        plateaus_all[leg] = plats
        if not plats:
            ground_model[leg] = None
            continue
        order = sorted(range(len(plats)), key=lambda i: -plats[i]["y"])
        point_runs = [(hoof_s[li, plats[i]["start"]:plats[i]["start"] + plats[i]["len"], 0],
                       hoof_s[li, plats[i]["start"]:plats[i]["start"] + plats[i]["len"], 1]) for i in order]
        a, b, used = fit_ground_line(point_runs, params)
        anchors = [plats[order[i]] for i in used]
        ground_model[leg] = {"slope": round(float(a), 4), "intercept": round(float(b), 2),
                             "anchors": [{k: round(v, 1) if isinstance(v, float) else v
                                          for k, v in p.items()} for p in anchors]}
        gline = a * hoof_s[li, :, 0] + b
        c = stat & (hoof_s[li, :, 1] >= gline - params["y_tol"])
        # remove 1-frame blips (edge-replicated padding, linear timeline)
        cp = np.pad(c.astype(np.uint8), 3, mode="edge")
        cp = cv2.morphologyEx(cp[None], cv2.MORPH_CLOSE, np.ones((1, 3), np.uint8))[0]
        cp = cv2.morphologyEx(cp[None], cv2.MORPH_OPEN, np.ones((1, 3), np.uint8))[0]
        contact[li] = cp[3:3 + T].astype(bool)
    return {"vx": vx, "vy": vy, "speed": speed, "contact": contact,
            "ground_model": ground_model, "plateaus": plateaus_all}


def analyze_tracks(frames, params=PARAMS):
    """Full track pipeline: segmentation -> filled/smoothed hoof tracks -> contacts."""
    hoof_raw, areas, body, masks = segment_video(frames, params, return_masks=True)
    hoof, valid, med_area, interp_frames = fill_invalid(hoof_raw, areas, params)
    hoof_s = smooth_tracks(hoof)
    body_f = np.stack([fill_nan_series(body[:, 0]), fill_nan_series(body[:, 1])], axis=1)
    kin = compute_contacts(hoof_s, params)
    return {"hoof_raw": hoof_raw, "hoof": hoof, "hoof_s": hoof_s, "areas": areas,
            "valid": valid, "med_area": med_area, "interp_frames": interp_frames,
            "body": body, "body_f": body_f, "masks": masks, **kin}


def cyc_eq(a, b):
    return len(a) == len(b) and any(list(a) == list(b[i:]) + list(b[:i]) for i in range(len(b)))


def drift_stats(vx_leg, c_leg, params=PARAMS):
    if not c_leg.any():
        return None
    dx = vx_leg[c_leg]
    m = float(np.mean(dx))
    return {
        "mean_dx_px_per_frame": round(m, 3),
        "mean_abs_dx_px_per_frame": round(float(np.mean(np.abs(dx))), 3),
        "std_dx_px_per_frame": round(float(np.std(dx)), 3),
        "uniformity_cv": (round(float(np.std(dx) / abs(m)), 2)
                          if abs(m) > params["drift_min_abs"] else None),
        "n_frames": int(c_leg.sum()),
    }


def clip_level_metrics(tr, T):
    """v2 report block over the whole clip (legacy, loop-intent semantics kept where
    meaningful; intervals are on the linear timeline)."""
    contact, vx, hoof, hoof_s, body = tr["contact"], tr["vx"], tr["hoof"], tr["hoof_s"], tr["body"]
    intervals = {leg: linear_intervals(contact[li]) for li, leg in enumerate(LEGS)}
    swing_intervals = {leg: linear_intervals(~contact[li]) for li, leg in enumerate(LEGS)}
    duty = {leg: float(contact[li].mean()) for li, leg in enumerate(LEGS)}
    no_swing = {leg: duty[leg] > 0.90 for leg in LEGS}
    onset_phase = {}
    for li, leg in enumerate(LEGS):
        ivs = intervals[leg]
        if not ivs or no_swing[leg]:
            onset_phase[leg] = None
            continue
        s, L = max(ivs, key=lambda p: p[1])
        onset_phase[leg] = s / T
    ref = "hind_near"
    movers = [l for l in LEGS if onset_phase[l] is not None]
    if onset_phase.get(ref) is not None:
        rel_phase = {l: (round(((onset_phase[l] - onset_phase[ref]) % 1.0), 4)
                         if onset_phase[l] is not None else None) for l in LEGS}
        order = sorted(movers, key=lambda l: (onset_phase[l] - onset_phase[ref]) % 1.0)
    else:
        rel_phase = {l: None for l in LEGS}
        order = movers
    if len(movers) < 4:
        sequence_type = "degenerate_only_%d_legs_step" % len(movers)
    else:
        sequence_type = ("lateral" if cyc_eq(order, LATERAL)
                         else "diagonal" if cyc_eq(order, DIAGONAL) else "irregular")
    sw_fn = ~contact[LEGS.index("fore_near")]
    sw_hn = ~contact[LEGS.index("hind_near")]
    near_swing_overlap = int((sw_fn & sw_hn).sum())
    drift = {leg: drift_stats(vx[li], contact[li]) for li, leg in enumerate(LEGS)}
    total_motion = {}
    for li, leg in enumerate(LEGS):
        p = hoof_s[li]
        path_len = float(np.sum(np.hypot(np.diff(p[:, 0]), np.diff(p[:, 1]))))
        total_motion[leg] = {
            "x_range_px": round(float(p[:, 0].max() - p[:, 0].min()), 1),
            "y_range_px": round(float(p[:, 1].max() - p[:, 1].min()), 1),
            "path_length_px": round(path_len, 1),
        }
    closure = {leg: round(float(np.linalg.norm(hoof[li, 0] - hoof[li, T - 1])), 2)
               for li, leg in enumerate(LEGS)}
    cb = float(np.linalg.norm(body[0] - body[T - 1]))
    by, bx = body[:, 1], body[:, 0]
    body_osc = {
        "y_mean": float(np.nanmean(by)),
        "y_amplitude_px": float(np.nanmax(by) - np.nanmin(by)),
        "y_std_px": float(np.nanstd(by)),
        "x_amplitude_px": float(np.nanmax(bx) - np.nanmin(bx)),
    }
    return {
        "contact_intervals_start_len": intervals,
        "swing_intervals_start_len": swing_intervals,
        "duty_factor": {k: round(v, 4) for k, v in duty.items()},
        "no_swing_flags": no_swing,
        "footfall": {
            "onset_phase": {k: (round(v, 4) if v is not None else None) for k, v in onset_phase.items()},
            "relative_phase_vs_hind_near": rel_phase,
            "observed_order": order,
            "expected_lateral_order": LATERAL,
            "sequence_type": sequence_type,
            "near_swing_overlap_frames": near_swing_overlap,
        },
        "contact_drift": drift,
        "hoof_total_motion": total_motion,
        "loop_closure_px": {"per_leg": closure,
                            "body_centroid": (round(cb, 2) if np.isfinite(cb) else None)},
        "body_oscillation": {k: (round(v, 2) if np.isfinite(v) else None) for k, v in body_osc.items()},
    }


# ======================================================================
#  1b. amount of motion / static-render guard (v3.1)
# ======================================================================
def _r(v, nd=4):
    return None if v is None or not np.isfinite(v) else round(float(v), nd)


def motion_metrics(frames, masks, hoof_raw=None, params=PARAMS):
    """Amount-of-motion metrics of the clip (guards against static / frozen renders).

    frames (T,H,W,3) uint8 RGB; masks (T,H,W) uint8 label map from
    segment_video(..., return_masks=True): 0..3 legs (LEGS order), 4 body, 255 bg.
    All per-frame series have T-1 entries (frame pair t -> t+1).

    (a) global: mean |gray[t+1]-gray[t]| over the whole frame;
    (b) silhouette: union of body+legs of frames t and t+1, dilated by sil_dilate_px;
        fraction of silhouette pixels with |diff| > fg_pix_thr ("moving pixels") and
        mean |diff| on the silhouette (= sum |diff| / silhouette area, grey levels);
    (c) per leg: the leg "moves" between t and t+1 when its mask centroid or its raw
        hoof point shifts by more than leg_move_px;
    (d) motion_score = median over frame pairs of (b) mean |diff|;
        static_frame_ratio = fraction of frame pairs whose silhouette moving-pixel
        fraction is below static_fg_frac_thr;
        per_leg_moving_ratio = fraction of frame pairs in which the leg moves.
    """
    T = frames.shape[0]
    nP = max(T - 1, 0)
    out = {
        "params": {k: params[k] for k in ("sil_dilate_px", "fg_pix_thr", "static_fg_frac_thr", "leg_move_px")},
        "frame_pairs": int(nP),
        "definitions": {
            "motion_score": "median over frame pairs of mean |gray diff| on the dilated body+legs silhouette (grey levels 0..255)",
            "static_frame_ratio": "fraction of frame pairs with silhouette moving-pixel fraction < static_fg_frac_thr",
            "per_leg_moving_ratio": "fraction of frame pairs where the leg mask centroid or hoof shifts > leg_move_px",
        },
    }
    if nP == 0:
        out.update({"motion_score": None, "static_frame_ratio": None,
                    "per_leg_moving_ratio": {l: None for l in LEGS}, "note": "single frame"})
        return out
    gray = np.stack([cv2.cvtColor(f, cv2.COLOR_RGB2GRAY) for f in frames]).astype(np.float32)
    r = int(params["sil_dilate_px"])
    kern = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * r + 1, 2 * r + 1))
    sil = np.stack([cv2.dilate((m != 255).astype(np.uint8), kern) for m in masks]).astype(bool)
    g_mean = np.zeros(nP); fg_frac = np.zeros(nP); fg_mean = np.zeros(nP); sil_area = np.zeros(nP)
    for t in range(nP):
        d = np.abs(gray[t + 1] - gray[t])
        g_mean[t] = float(d.mean())
        s = sil[t] | sil[t + 1]
        a = int(s.sum()); sil_area[t] = a
        if a > 0:
            ds = d[s]
            fg_frac[t] = float((ds > params["fg_pix_thr"]).mean())
            fg_mean[t] = float(ds.mean())
    # per-leg displacement: mask centroid and raw hoof point (NaN-aware max)
    cent = np.full((len(LEGS), T, 2), np.nan)
    for t in range(T):
        for li in range(len(LEGS)):
            ys, xs = np.nonzero(masks[t] == li)
            if len(xs):
                cent[li, t] = (xs.mean(), ys.mean())
    leg_disp = np.full((len(LEGS), nP), np.nan)
    leg_moving = np.zeros((len(LEGS), nP), dtype=bool)
    for li in range(len(LEGS)):
        disp = np.linalg.norm(np.diff(cent[li], axis=0), axis=1)
        if hoof_raw is not None:
            disp = np.fmax(disp, np.linalg.norm(np.diff(hoof_raw[li], axis=0), axis=1))
        leg_disp[li] = disp
        leg_moving[li] = np.nan_to_num(disp, nan=0.0) > params["leg_move_px"]
    static = fg_frac < params["static_fg_frac_thr"]
    out.update({
        "motion_score": _r(np.median(fg_mean), 3),
        "static_frame_ratio": _r(static.mean(), 4),
        "static_frame_pairs": [int(i) for i in np.nonzero(static)[0]],
        "per_leg_moving_ratio": {leg: _r(leg_moving[li].mean(), 4) for li, leg in enumerate(LEGS)},
        "per_leg_moving_pairs": {leg: int(leg_moving[li].sum()) for li, leg in enumerate(LEGS)},
        "global": {
            "mean_abs_diff_mean": _r(g_mean.mean(), 3), "mean_abs_diff_median": _r(np.median(g_mean), 3),
            "mean_abs_diff_min": _r(g_mean.min(), 3),
            "per_frame_mean_abs_diff": [round(float(v), 3) for v in g_mean],
        },
        "silhouette": {
            "area_mean_px": _r(sil_area.mean(), 1), "area_min_px": _r(sil_area.min(), 1),
            "moving_frac_mean": _r(fg_frac.mean()), "moving_frac_median": _r(np.median(fg_frac)),
            "moving_frac_min": _r(fg_frac.min()),
            "mean_abs_diff_mean": _r(fg_mean.mean(), 3), "mean_abs_diff_median": _r(np.median(fg_mean), 3),
            "mean_abs_diff_min": _r(fg_mean.min(), 3),
            "per_frame_moving_frac": [round(float(v), 4) for v in fg_frac],
            "per_frame_mean_abs_diff": [round(float(v), 3) for v in fg_mean],
        },
        "per_leg_displacement_px": {leg: [_r(v, 2) for v in leg_disp[li]] for li, leg in enumerate(LEGS)},
        "per_leg_moving": {leg: [bool(v) for v in leg_moving[li]] for li, leg in enumerate(LEGS)},
    })
    return out


def ffprobe_motion(path, params=PARAMS):
    """Cheap encoder-side cross-check: packet size of inter frames vs the I frame(s).
    Repeated / frozen content is coded as skip blocks -> tiny P/B packets."""
    fp = shutil.which("ffprobe")
    if fp is None:
        return {"error": "ffprobe not found in PATH"}
    try:
        r = subprocess.run([fp, "-v", "error", "-select_streams", "v:0",
                            "-show_entries", "frame=pkt_size,pict_type", "-of", "json", path],
                           capture_output=True, text=True, timeout=120)
        fr = json.loads(r.stdout or "{}").get("frames", [])
    except Exception as ex:  # noqa
        return {"error": str(ex)}
    sizes = {}
    for f in fr:
        try:
            sizes.setdefault(str(f.get("pict_type", "?")), []).append(int(f.get("pkt_size")))
        except (TypeError, ValueError):
            continue
    i_fr, p_fr, b_fr = sizes.get("I", []), sizes.get("P", []), sizes.get("B", [])
    inter = p_fr + b_fr
    i_bytes = float(np.mean(i_fr)) if i_fr else None
    mean_p = float(np.mean(p_fr)) if p_fr else None
    mean_inter = float(np.mean(inter)) if inter else None
    ratio = (mean_p / i_bytes) if (mean_p is not None and i_bytes) else None
    note = None
    if not p_fr:
        note = "no P frames (intra-only or single-frame stream): ratio undefined"
    return {"n_frames": len(fr), "n_i": len(i_fr), "n_p": len(p_fr), "n_b": len(b_fr),
            "i_frame_bytes": _r(i_bytes, 1), "mean_p_frame_bytes": _r(mean_p, 1),
            "mean_b_frame_bytes": _r(float(np.mean(b_fr)) if b_fr else None, 1),
            "mean_inter_frame_bytes": _r(mean_inter, 1),
            "ratio": _r(ratio), "ratio_definition": "mean_p_frame_bytes / i_frame_bytes",
            "static_ratio_thr": params["ffprobe_static_ratio"],
            "static_hint": (bool(ratio < params["ffprobe_static_ratio"]) if ratio is not None else None),
            "note": note}


def motion_rule(motion, params=PARAMS):
    """Hard rule motion_present: static_frame_ratio <= static_frame_ratio_max AND every leg
    moves in >= leg_moving_ratio_min of the frame pairs.  Returns (ok, reasons)."""
    reasons = []
    sfr = motion.get("static_frame_ratio")
    ms = motion.get("motion_score")
    if sfr is None:
        return False, ["static render: motion undefined (%s)" % motion.get("note")]
    ok_static = sfr <= params["static_frame_ratio_max"]
    if not ok_static:
        reasons.append("static render: %.0f%% of frame pairs without silhouette motion "
                       "(static_frame_ratio %.2f > %.2f, motion_score %.2f)"
                       % (100.0 * sfr, sfr, params["static_frame_ratio_max"], ms if ms is not None else float("nan")))
    plr = motion.get("per_leg_moving_ratio") or {}
    frozen = [l for l in LEGS if (plr.get(l) is None) or plr[l] < params["leg_moving_ratio_min"]]
    if frozen:
        reasons.append("legs %s never move (moving ratio %s < %.2f)"
                       % (", ".join(frozen), ", ".join("%.2f" % (plr.get(l) or 0.0) for l in frozen),
                          params["leg_moving_ratio_min"]))
    return bool(ok_static and not frozen), reasons


# ======================================================================
#  2. period detection + steady state
# ======================================================================
def detect_period(hoof_s, min_lag=12, max_lag=60, params=PARAMS):
    """Summed normalised autocorrelation of the 8 hoof signals (x,y of 4 legs).

    Each signal is linearly detrended; frozen signals (std < min_signal_std) are
    dropped.  ACF(k) = mean over signals of Pearson r between a[:-k] and a[k:].
    The period is the smallest local maximum in [min_lag, max_lag] whose height is
    >= subharmonic_tol * global maximum (protects against picking 2T), refined to
    sub-frame precision by parabolic interpolation.
    """
    T = hoof_s.shape[1]
    max_lag_eff = int(min(max_lag, T - params["acf_min_overlap"]))
    res = {"period_frames": None, "confidence": None, "lag_int": None,
           "search_range": [int(min_lag), max_lag_eff], "n_signals": 0,
           "signals_used": [], "acf": None, "note": None}
    if max_lag_eff < min_lag:
        res["note"] = "video too short for the requested lag range"
        return res
    t = np.arange(T, dtype=float)
    sigs, used = [], []
    for li, leg in enumerate(LEGS):
        for dim, dn in enumerate("xy"):
            a = hoof_s[li, :, dim].astype(float)
            p = np.polyfit(t, a, 1)
            a = a - np.polyval(p, t)
            if a.std() < params["min_signal_std"]:
                continue
            sigs.append(a)
            used.append("%s.%s" % (leg, dn))
    res["n_signals"] = len(sigs); res["signals_used"] = used
    if not sigs:
        res["note"] = "all hoof signals frozen (std below min_signal_std)"
        return res
    acf = np.ones(max_lag_eff + 1)
    for k in range(1, max_lag_eff + 1):
        rs = []
        for a in sigs:
            x, y = a[:-k], a[k:]
            sx, sy = x.std(), y.std()
            if sx < 1e-9 or sy < 1e-9:
                rs.append(0.0)
            else:
                rs.append(float(np.mean((x - x.mean()) * (y - y.mean())) / (sx * sy)))
        acf[k] = float(np.mean(rs))
    res["acf"] = [round(float(v), 4) for v in acf]
    lo, hi = int(min_lag), max_lag_eff
    seg = acf[lo:hi + 1]
    peaks = []
    for k in range(lo, hi + 1):
        left = acf[k - 1] if k - 1 >= 0 else -np.inf
        right = acf[k + 1] if k + 1 <= max_lag_eff else -np.inf
        if acf[k] >= left and acf[k] >= right:
            peaks.append(k)
    gmax = float(seg.max())
    if not peaks:
        k0 = lo + int(np.argmax(seg))
        res["note"] = "no local maximum in range; using argmax"
    else:
        cands = [k for k in peaks if acf[k] >= params["subharmonic_tol"] * gmax]
        k0 = min(cands) if cands else peaks[int(np.argmax([acf[k] for k in peaks]))]
    # parabolic refinement
    if 1 <= k0 <= max_lag_eff - 1:
        a, b, c = acf[k0 - 1], acf[k0], acf[k0 + 1]
        den = a - 2 * b + c
        delta = 0.5 * (a - c) / den if den < -1e-12 else 0.0
        delta = float(np.clip(delta, -0.5, 0.5))
        peak_val = float(b - 0.25 * (a - c) * delta)
    else:
        delta, peak_val = 0.0, float(acf[k0])
    period = float(k0 + delta)
    # prominence: peak minus the minimum of the ACF between lag 0 and the peak
    dip = float(acf[1:k0].min()) if k0 > 1 else float(acf[k0])
    res.update({"period_frames": round(period, 3), "confidence": round(peak_val, 4),
                "lag_int": int(k0), "prominence": round(peak_val - dip, 4),
                "global_max_in_range": round(gmax, 4),
                "local_maxima_lags": [int(k) for k in peaks]})
    return res


def interp_tracks(tracks, times):
    """tracks (n,T,2) sampled at integer frames -> values at fractional times (n,len(times),2)."""
    tracks = np.asarray(tracks, float)
    T = tracks.shape[1]
    times = np.clip(np.asarray(times, float), 0, T - 1)
    idx = np.arange(T)
    out = np.zeros((tracks.shape[0], len(times), 2))
    for i in range(tracks.shape[0]):
        for dim in range(2):
            out[i, :, dim] = np.interp(times, idx, tracks[i, :, dim])
    return out


def periodicity_error(hoof_s, period):
    """e(t) = max over legs of |p(t) - p(t+period)| for t with t+period <= T-1."""
    T = hoof_s.shape[1]
    n = int(np.floor(T - 1 - period)) + 1
    if n <= 0:
        return np.zeros(0)
    t0 = np.arange(n, dtype=float)
    p0 = interp_tracks(hoof_s, t0)
    p1 = interp_tracks(hoof_s, t0 + period)
    return np.linalg.norm(p1 - p0, axis=2).max(axis=0)


def find_steady_state(err, period, thr, params=PARAMS):
    """First frame s such that over [s, s+round(period)) the 80th percentile of e(t)
    is <= thr and max e(t) <= steady_spike_factor*thr (short occlusion spikes of the
    hoof detector are tolerated).  Fallback (not found): s minimising the moving mean."""
    n = len(err)
    if n == 0:
        return {"start": 0, "found": False, "threshold_px": thr,
                "note": "no periodicity samples (video shorter than one period)"}
    w = int(max(1, min(n, round(period))))
    sf = params["steady_spike_factor"]
    p80 = np.array([np.percentile(err[s:s + w], 80) for s in range(n - w + 1)])
    mx = np.array([err[s:s + w].max() for s in range(n - w + 1)])
    ok = np.nonzero((p80 <= thr) & (mx <= sf * thr))[0]
    base = {"threshold_px": thr, "spike_factor": sf, "check_window_frames": w}
    if len(ok):
        s = int(ok[0])
        return {"start": s, "found": True, **base,
                "p80_err_in_window_px": round(float(p80[s]), 3),
                "max_err_in_window_px": round(float(mx[s]), 3)}
    mm = np.array([err[s:s + w].mean() for s in range(n - w + 1)])
    s = int(np.argmin(mm))
    return {"start": s, "found": False, **base,
            "p80_err_in_window_px": round(float(p80[s]), 3),
            "max_err_in_window_px": round(float(mx[s]), 3),
            "note": "periodicity error never drops below threshold; using least-error start"}


# ======================================================================
#  3. best window selection
# ======================================================================
def window_leg_stats(contact, vx, s, e):
    """Per-leg swing / touchdown statistics on integer frames in [ceil(s), floor(e)]."""
    T = contact.shape[1]
    i0, i1 = int(np.ceil(s)), int(np.floor(e))
    i0 = max(0, i0); i1 = min(T - 1, i1)
    out = {}
    for li, leg in enumerate(LEGS):
        c = contact[li, i0:i1 + 1]
        sw = ~c
        prev = contact[li, i0 - 1] if i0 > 0 else c[0]
        cc = np.concatenate([[prev], c])
        touchdowns = [i0 + k for k in range(len(c)) if c[k] and not cc[k]]
        liftoffs = [i0 + k for k in range(len(c)) if (not c[k]) and cc[k]]
        out[leg] = {
            "swing_frames": int(sw.sum()),
            "touchdowns": touchdowns, "liftoffs": liftoffs,
            "duty": float(c.mean()) if len(c) else None,
            "drift": drift_stats(vx[li, i0:i1 + 1], c),
            "stepping": bool(sw.sum() >= 2 and (len(touchdowns) + len(liftoffs)) >= 1),
        }
    return out, (i0, i1)


def footfall_order(leg_stats, s, period):
    """Touchdown phase (modulo one stride) of each leg inside the window -> observed
    order, lateral check, beat spacing."""
    phases = {}
    for leg in LEGS:
        td = leg_stats[leg]["touchdowns"]
        phases[leg] = ((td[0] - s) / period) % 1.0 if td else None
    movers = [l for l in LEGS if phases[l] is not None]
    order = sorted(movers, key=lambda l: phases[l])
    order_ok = len(movers) == 4 and cyc_eq(order, LATERAL)
    gaps = None
    if len(movers) == 4:
        ph = sorted(phases[l] for l in movers)
        gaps = [round(float(((ph[(i + 1) % 4] - ph[i]) % 1.0)), 3) for i in range(4)]
    seq = ("lateral" if order_ok else "diagonal" if len(movers) == 4 and cyc_eq(order, DIAGONAL)
           else "irregular" if len(movers) == 4 else "degenerate_only_%d_legs_step" % len(movers))
    return {"touchdown_phase": {k: (round(v, 4) if v is not None else None) for k, v in phases.items()},
            "observed_order": order, "order_ok": bool(order_ok), "sequence_type": seq,
            "beat_gaps": gaps,
            "four_beat_even": (bool(all(0.12 <= g <= 0.38 for g in gaps)) if gaps else False)}


def select_best_window(tr, period, steady_start, N, cycles=None, params=PARAMS):
    hoof_s, body, contact, vx = tr["hoof_s"], tr["body_f"], tr["contact"], tr["vx"]
    T = hoof_s.shape[1]
    res = {"target_frames": int(N), "note": None}
    if period is None or period <= 0:
        res["note"] = "no period -> no window"
        return None, res
    s_min = int(steady_start)
    m_max = int(np.floor((T - 1 - s_min) / period + 1e-9))
    if m_max < 1:
        if int(np.floor((T - 1) / period + 1e-9)) >= 1:
            res["note"] = ("steady part shorter than one period; window search extended "
                           "into the acceleration phase")
            s_min = 0
            m_max = int(np.floor((T - 1 - s_min) / period + 1e-9))
        else:
            res["note"] = "video shorter than one period -> no window"
            return None, res
    if cycles is not None:
        if cycles < 1 or cycles > m_max:
            res["note"] = "requested cycles=%d not feasible (max %d)" % (cycles, m_max)
            return None, res
        m = int(cycles)
    else:  # integer number of periods whose retiming is closest to real time
        ms = np.arange(1, m_max + 1)
        m = int(ms[np.argmin(np.abs(np.log(ms * period / (N - 1))))])
    span = m * period
    time_scale = span / (N - 1)
    vel = np.stack([vx, tr["vy"]], axis=2)  # (4,T,2)
    cands = []
    for s in range(s_min, int(np.floor(T - 1 - span + 1e-9)) + 1):
        e = s + span
        p0 = interp_tracks(hoof_s, [s])[:, 0]; p1 = interp_tracks(hoof_s, [e])[:, 0]
        b0 = interp_tracks(body[None], [s])[0, 0]; b1 = interp_tracks(body[None], [e])[0, 0]
        d_h = np.linalg.norm(p1 - p0, axis=1)
        d_b = float(np.linalg.norm(b1 - b0))
        closure = float(d_h.sum() + d_b)
        v0 = interp_tracks(vel, [s])[:, 0]; v1 = interp_tracks(vel, [e])[:, 0]
        vmis = float(np.linalg.norm(v1 - v0, axis=1).sum())
        stats, (i0, i1) = window_leg_stats(contact, vx, s, e)
        stepping = [l for l in LEGS if stats[l]["stepping"]]
        ff = footfall_order(stats, s, period)  # phases modulo ONE stride, even if m > 1
        cost = (closure + params["vel_mismatch_weight"] * vmis
                + params["missing_leg_penalty"] * (4 - len(stepping))
                + (0.0 if ff["order_ok"] else params["order_penalty"]))
        cands.append({
            "start_frame": int(s), "end_time": round(float(e), 3), "cost": round(cost, 3),
            "closure_px": round(closure, 3),
            "closure_per_point_px": {**{leg: round(float(d_h[li]), 3) for li, leg in enumerate(LEGS)},
                                     "body": round(d_b, 3)},
            "seam_velocity_mismatch_px": round(vmis, 3),
            "legs_stepping": len(stepping), "stepping_legs": stepping,
            "order_ok": ff["order_ok"], "footfall": ff,
            "leg_stats": stats, "int_frames": [i0, i1],
        })
    if not cands:
        res["note"] = "no candidate start frames"
        return None, res
    best = min(cands, key=lambda c: c["cost"])
    best_out = {
        "start_frame": best["start_frame"],
        "end_time": best["end_time"],
        "span_frames": round(float(span), 3),
        "periods": m,
        "time_scale": round(float(time_scale), 5),
        "time_scale_note": "source frames per output frame; >1 = sped up, <1 = slowed down",
        "closure_px": best["closure_px"],
        "closure_per_point_px": best["closure_per_point_px"],
        "seam_velocity_mismatch_px": best["seam_velocity_mismatch_px"],
        "legs_stepping": best["legs_stepping"],
        "stepping_legs": best["stepping_legs"],
        "order_ok": best["order_ok"],
        "footfall": best["footfall"],
        "score": round(-best["cost"], 3),
        "score_definition": ("-(closure_px + %.1f*seam_velocity_mismatch + %.0f*missing_legs + %.0f*order_broken); "
                             "higher is better" % (params["vel_mismatch_weight"],
                                                    params["missing_leg_penalty"], params["order_penalty"])),
        "duty_in_window": {l: round(best["leg_stats"][l]["duty"], 4) for l in LEGS},
        "swing_frames_in_window": {l: best["leg_stats"][l]["swing_frames"] for l in LEGS},
        "touchdowns_in_window": {l: best["leg_stats"][l]["touchdowns"] for l in LEGS},
        "contact_drift_in_window": {l: best["leg_stats"][l]["drift"] for l in LEGS},
        "candidates_evaluated": len(cands),
        "search_start_min": s_min,
        "top_candidates": [{k: c[k] for k in ("start_frame", "cost", "closure_px",
                                               "seam_velocity_mismatch_px", "legs_stepping", "order_ok")}
                           for c in sorted(cands, key=lambda c: c["cost"])[:6]],
    }
    res["note"] = res["note"] or "ok"
    return best_out, res


# ======================================================================
#  4. export
# ======================================================================
def resample_cycle(frames, start, time_scale, N, mode="blend"):
    """Frame-accurate retiming: output frame k <- source time start + k*time_scale."""
    T = frames.shape[0]
    out, times = [], []
    for k in range(N):
        tau = float(np.clip(start + k * time_scale, 0, T - 1))
        times.append(tau)
        i0 = int(np.floor(tau)); f = tau - i0
        i1 = min(i0 + 1, T - 1)
        if mode == "nearest" or f < 0.02 or f > 0.98:
            fr = frames[int(round(tau))]
        else:
            fr = cv2.addWeighted(frames[i0], 1.0 - f, frames[i1], f, 0.0)
        out.append(fr)
    return np.stack(out), times


def write_mp4(frames_rgb, path, fps=25, size=(768, 448)):
    ff = shutil.which("ffmpeg")
    if ff is None:
        raise RuntimeError("ffmpeg not found in PATH")
    W, H = size
    cmd = [ff, "-y", "-loglevel", "error", "-f", "rawvideo", "-pix_fmt", "bgr24",
           "-s", "%dx%d" % (W, H), "-r", str(fps), "-i", "-", "-an",
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "12", "-preset", "slow",
           "-movflags", "+faststart", path]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    for fr in frames_rgb:
        bgr = cv2.cvtColor(fr, cv2.COLOR_RGB2BGR)
        if (bgr.shape[1], bgr.shape[0]) != (W, H):
            bgr = cv2.resize(bgr, (W, H), interpolation=cv2.INTER_AREA)
        p.stdin.write(np.ascontiguousarray(bgr).tobytes())
    p.stdin.close()
    err = p.stderr.read().decode("utf-8", "replace")
    if p.wait() != 0:
        raise RuntimeError("ffmpeg failed: " + err)


def probe_frames(path):
    fp = shutil.which("ffprobe")
    if fp is None:
        return None
    try:
        r = subprocess.run([fp, "-v", "error", "-select_streams", "v:0", "-count_frames",
                            "-show_entries", "stream=nb_read_frames,width,height,pix_fmt,codec_name,r_frame_rate",
                            "-of", "json", path], capture_output=True, text=True, timeout=60)
        st = json.loads(r.stdout)["streams"][0]
        return {"frames": int(st.get("nb_read_frames", 0)), "codec": st.get("codec_name"),
                "pix_fmt": st.get("pix_fmt"), "size": [int(st["width"]), int(st["height"])],
                "rate": st.get("r_frame_rate")}
    except Exception as ex:  # noqa
        return {"error": str(ex)}


def write_seam_check(frames, tr, best, cycle_frames, path):
    """start | end | amplified difference, with hoof markers and closure numbers."""
    s, e = best["start_frame"], best["end_time"]
    f0 = cv2.cvtColor(cycle_frames[0], cv2.COLOR_RGB2BGR).copy()
    f1 = cv2.cvtColor(cycle_frames[-1], cv2.COLOR_RGB2BGR).copy()
    diff = cv2.absdiff(f0, f1)
    diff = np.clip(diff.astype(np.int32) * 4, 0, 255).astype(np.uint8)
    p0 = interp_tracks(tr["hoof_s"], [s])[:, 0]; p1 = interp_tracks(tr["hoof_s"], [e])[:, 0]
    b0 = interp_tracks(tr["body_f"][None], [s])[0, 0]; b1 = interp_tracks(tr["body_f"][None], [e])[0, 0]
    for img, pts, bc in ((f0, p0, b0), (f1, p1, b1)):
        for li, leg in enumerate(LEGS):
            c = (int(round(pts[li, 0])), int(round(pts[li, 1])))
            cv2.circle(img, c, 9, (0, 0, 0), 2, cv2.LINE_AA)
            cv2.circle(img, c, 7, LEG_BGR[leg], -1, cv2.LINE_AA)
        if np.all(np.isfinite(bc)):
            cv2.drawMarker(img, (int(bc[0]), int(bc[1])), (0, 0, 255), cv2.MARKER_CROSS, 16, 2)
    # both hoof sets on the diff panel: start = filled, end = hollow
    for li, leg in enumerate(LEGS):
        c0 = (int(round(p0[li, 0])), int(round(p0[li, 1])))
        c1 = (int(round(p1[li, 0])), int(round(p1[li, 1])))
        cv2.circle(diff, c0, 6, LEG_BGR[leg], -1, cv2.LINE_AA)
        cv2.circle(diff, c1, 9, LEG_BGR[leg], 2, cv2.LINE_AA)
    H, W = f0.shape[:2]
    hdr = 64
    canvas = np.full((H + hdr, 3 * W + 2 * 6, 3), 255, np.uint8)
    for i, img in enumerate((f0, f1, diff)):
        x = i * (W + 6)
        canvas[hdr:hdr + H, x:x + W] = img
    titles = ["output frame 0  <- source t=%.2f" % s,
              "output frame %d  <- source t=%.2f" % (len(cycle_frames) - 1, e),
              "|start - end| x4   (filled = start, ring = end)"]
    for i, tt in enumerate(titles):
        cv2.putText(canvas, tt, (i * (W + 6) + 10, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.62,
                    (20, 20, 20), 2, cv2.LINE_AA)
    cp = best["closure_per_point_px"]
    line = "closure sum %.2f px  |  " % best["closure_px"] + "  ".join(
        "%s %.2f" % (k.replace("_", "-"), v) for k, v in cp.items())
    cv2.putText(canvas, line, (10, 52), cv2.FONT_HERSHEY_SIMPLEX, 0.52, (60, 60, 60), 1, cv2.LINE_AA)
    cv2.imwrite(path, canvas)


def write_cycle_sheet(cycle_frames, times, path, cols=7, thumb_w=192):
    """All N output frames as thumbnails (frame index <- source time) for visual QA."""
    N = len(cycle_frames)
    H, W = cycle_frames[0].shape[:2]
    th = int(round(thumb_w * H / W))
    rows = int(np.ceil(N / cols))
    canvas = np.full((rows * (th + 22) + 8, cols * (thumb_w + 6) + 6, 3), 255, np.uint8)
    for k in range(N):
        r, c = divmod(k, cols)
        x, y = 6 + c * (thumb_w + 6), 8 + r * (th + 22)
        thumb = cv2.resize(cv2.cvtColor(cycle_frames[k], cv2.COLOR_RGB2BGR), (thumb_w, th),
                           interpolation=cv2.INTER_AREA)
        canvas[y:y + th, x:x + thumb_w] = thumb
        cv2.putText(canvas, "%d <- %.2f" % (k, times[k]), (x, y + th + 16),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.45, (40, 40, 40), 1, cv2.LINE_AA)
    cv2.imwrite(path, canvas)


# ======================================================================
#  5. verdict
# ======================================================================
def make_verdict(best, period_info, steady, closure_thr, params=PARAMS, motion=None):
    """motion = metrics["motion"] block (motion_metrics output); when given, the hard
    rule motion_present (static-render guard) is evaluated first."""
    rules, reasons = {}, []
    if motion is not None:
        rules["motion_present"], motion_reasons = motion_rule(motion, params)
        reasons.extend(motion_reasons)
    if best is None:
        rules["window_found"] = False
        reasons.append("no valid cycle window (period=%s)" % period_info.get("period_frames"))
        return {"verdict": "rework", "rules": rules, "reasons": reasons}
    rules["window_found"] = True
    rules["four_legs_stepping"] = best["legs_stepping"] == 4
    if not rules["four_legs_stepping"]:
        reasons.append("only %d/4 legs swing inside the window (%s)"
                       % (best["legs_stepping"], ", ".join(best["stepping_legs"]) or "none"))
    lo, hi = params["duty_range"]
    dw = best["duty_in_window"]
    bad = [l for l in LEGS if not (lo <= dw[l] <= hi)]
    rules["duty_in_range"] = not bad
    if bad:
        reasons.append("duty outside %.2f..%.2f: %s" % (lo, hi, ", ".join("%s=%.2f" % (l, dw[l]) for l in bad)))
    rules["lateral_order"] = bool(best["order_ok"])
    if not best["order_ok"]:
        reasons.append("footfall order %s is %s, expected lateral %s (cyclic)"
                       % (best["footfall"]["observed_order"], best["footfall"]["sequence_type"], LATERAL))
    rules["closure_le_thr"] = best["closure_px"] <= closure_thr
    if not rules["closure_le_thr"]:
        reasons.append("closure after retiming %.2f px > %.1f px" % (best["closure_px"], closure_thr))
    dr = best["contact_drift_in_window"]
    signs = []
    for l in LEGS:
        d = dr[l]
        if d is None or abs(d["mean_dx_px_per_frame"]) <= params["drift_min_abs"]:
            signs.append(0)
        else:
            signs.append(int(np.sign(d["mean_dx_px_per_frame"])))
    rules["stance_drift_unidirectional"] = bool(all(sg != 0 for sg in signs) and len(set(signs)) == 1)
    if not rules["stance_drift_unidirectional"]:
        reasons.append("stance drift not unidirectional: mean dx = %s" % {
            l: (dr[l]["mean_dx_px_per_frame"] if dr[l] else None) for l in LEGS})
    cvs = {l: (dr[l]["uniformity_cv"] if dr[l] else None) for l in LEGS}
    rules["stance_drift_uniform"] = bool(all(c is not None and c <= params["drift_cv_max"] for c in cvs.values()))
    if not rules["stance_drift_uniform"]:
        reasons.append("stance drift not uniform (cv > %.2f or undefined): %s" % (params["drift_cv_max"], cvs))
    if not steady.get("found", False):
        reasons.append("warning: steady state not confirmed (%s)" % steady.get("note"))
    # Hard rules decide the kinematics; soft rules flag defects the fitter
    # repairs itself (root detrend for body drift, hoof-contact pinning for
    # stance slide). A clip failing only soft rules is usable for fitting.
    per_point = best.get("closure_per_point_px") or {}
    hoof_closure = [per_point[l] for l in LEGS if l in per_point]
    rules["hoof_closure_le_thr"] = bool(hoof_closure) and max(hoof_closure) <= closure_thr
    if hoof_closure and not rules["hoof_closure_le_thr"]:
        reasons.append("per-hoof closure %.2f px > %.1f px" % (max(hoof_closure), closure_thr))
    hard_keys = ("window_found", "four_legs_stepping", "duty_in_range", "lateral_order", "hoof_closure_le_thr")
    if motion is not None:
        hard_keys = ("motion_present",) + hard_keys
    soft_keys = ("closure_le_thr", "stance_drift_unidirectional", "stance_drift_uniform")
    hard_ok = all(rules.get(k, False) for k in hard_keys)
    soft_ok = all(rules.get(k, False) for k in soft_keys)
    verdict = "accept" if (hard_ok and soft_ok) else ("accept_with_fit_fixes" if hard_ok else "rework")
    return {"verdict": verdict, "rules": rules, "reasons": reasons,
            "hard_rules_ok": hard_ok, "soft_rules_ok": soft_ok}


# ======================================================================
#  6. plots (cv2 only)
# ======================================================================
def draw_gait_chart(tr, T, title, path, window=None):
    contact, interp_frames = tr["contact"], tr["interp_frames"]
    cw, lh, x0, y0 = max(6, min(18, 1100 // max(T, 1))), 56, 210, 60
    gc = np.full((y0 + lh * 4 + 70, x0 + cw * T + 30, 3), 255, np.uint8)
    cv2.putText(gc, title, (14, 32), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (30, 30, 30), 2, cv2.LINE_AA)
    if window is not None:
        s, e = window["start_frame"], window["end_time"]
        xa, xb = int(x0 + s * cw), int(x0 + e * cw + cw)
        cv2.rectangle(gc, (xa, y0 - 6), (xb, y0 + 4 * lh - 4), (235, 245, 235), -1)
        cv2.line(gc, (xa, y0 - 6), (xa, y0 + 4 * lh - 4), (0, 160, 0), 2)
        cv2.line(gc, (xb, y0 - 6), (xb, y0 + 4 * lh - 4), (0, 160, 0), 2)
    for li, leg in enumerate(LEGS):
        yy = y0 + li * lh
        cv2.putText(gc, LEG_LABEL[leg], (10, yy + 34), cv2.FONT_HERSHEY_SIMPLEX,
                    0.52, (30, 30, 30), 1, cv2.LINE_AA)
        for t in range(T):
            xx = x0 + t * cw
            if contact[li, t]:
                cv2.rectangle(gc, (xx, yy + 8), (xx + cw - 2, yy + lh - 12), LEG_BGR[leg], -1)
            else:
                cv2.rectangle(gc, (xx, yy + lh // 2 - 2), (xx + cw - 2, yy + lh // 2 + 2),
                              (225, 225, 225), -1)
            if t in interp_frames[leg]:
                cv2.line(gc, (xx + cw // 2 - 1, yy + lh - 9), (xx + cw // 2 - 1, yy + lh - 4),
                         (0, 0, 255), 2)
    step = 5 if cw >= 12 else 10
    for t in range(0, T, step):
        xx = x0 + t * cw
        cv2.line(gc, (xx, y0 + 4 * lh), (xx, y0 + 4 * lh + 6), (120, 120, 120), 1)
        cv2.putText(gc, str(t), (xx - 6, y0 + 4 * lh + 24), cv2.FONT_HERSHEY_SIMPLEX,
                    0.45, (60, 60, 60), 1, cv2.LINE_AA)
    cv2.putText(gc, "frame (red tick = interpolated hoof; green = selected cycle window)",
                (x0, y0 + 4 * lh + 48), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (60, 60, 60), 1, cv2.LINE_AA)
    cv2.imwrite(path, gc)


def draw_panel(canvas, rect, data, title, T, ground_levels=None):
    px, py, pw, ph = rect
    cv2.rectangle(canvas, (px, py), (px + pw, py + ph), (180, 180, 180), 1)
    cv2.putText(canvas, title, (px, py - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (30, 30, 30), 1, cv2.LINE_AA)
    all_v = np.concatenate([d for d in data.values()])
    vmin, vmax = float(all_v.min()), float(all_v.max())
    pad = 0.06 * (vmax - vmin + 1e-6)
    vmin -= pad; vmax += pad

    def sy(v):
        return int(py + (v - vmin) / (vmax - vmin) * ph)

    def sx(t):
        return int(px + t / max(T - 1, 1) * pw)

    for gv_leg, vals in (ground_levels or {}).items():
        for gv in vals:
            ygl = sy(gv)
            for xseg in range(px, px + pw, 12):
                cv2.line(canvas, (xseg, ygl), (min(xseg + 6, px + pw), ygl), LEG_BGR[gv_leg], 1)
    for leg, dd in data.items():
        pts = np.array([[sx(t), sy(dd[t])] for t in range(T)], np.int32)
        cv2.polylines(canvas, [pts], False, LEG_BGR[leg], 2, cv2.LINE_AA)
    for v in np.linspace(vmin + pad, vmax - pad, 4):
        cv2.putText(canvas, f"{v:.0f}", (px - 48, sy(v) + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
    for t in range(0, T, 10):
        cv2.putText(canvas, str(t), (sx(t) - 6, py + ph + 18), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)


def draw_trajectories(tr, T, title, path):
    hoof_s, ground_model = tr["hoof_s"], tr["ground_model"]
    gl_levels = {leg: ([p["y"] for p in ground_model[leg]["anchors"]] if ground_model[leg] else [])
                 for leg in LEGS}
    traj = np.full((760, 980, 3), 255, np.uint8)
    cv2.putText(traj, title, (16, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (30, 30, 30), 2, cv2.LINE_AA)
    draw_panel(traj, (70, 80, 860, 260), {leg: hoof_s[li, :, 0] for li, leg in enumerate(LEGS)}, "x(t), px", T)
    draw_panel(traj, (70, 430, 860, 260), {leg: hoof_s[li, :, 1] for li, leg in enumerate(LEGS)},
               "y(t), px  (dashed = planting levels)", T, ground_levels=gl_levels)
    lx = 70
    for leg in LEGS:
        cv2.rectangle(traj, (lx, 745), (lx + 16, 755), LEG_BGR[leg], -1)
        cv2.putText(traj, LEG_LABEL[leg], (lx + 22, 755), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (40, 40, 40), 1, cv2.LINE_AA)
        lx += 240
    cv2.imwrite(path, traj)


def draw_overlay(frames, tr, F, path):
    F = int(min(F, frames.shape[0] - 1))
    ov = cv2.cvtColor(frames[F], cv2.COLOR_RGB2BGR).copy()
    for li, leg in enumerate(LEGS):
        x, y = tr["hoof"][li, F]
        p = (int(round(x)), int(round(y)))
        cv2.circle(ov, p, 9, (0, 0, 0), 2, cv2.LINE_AA)
        cv2.circle(ov, p, 7, LEG_BGR[leg], -1, cv2.LINE_AA)
        tag = leg + (" [C]" if tr["contact"][li, F] else " [air]")
        ty = p[1] + 26 if li % 2 == 0 else p[1] - 16
        cv2.putText(ov, tag, (p[0] - 40, ty), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0, 0, 0), 2, cv2.LINE_AA)
        cv2.putText(ov, tag, (p[0] - 40, ty), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1, cv2.LINE_AA)
    bc = tr["body"][F]
    if np.all(np.isfinite(bc)):
        cv2.drawMarker(ov, (int(bc[0]), int(bc[1])), (0, 0, 255), cv2.MARKER_CROSS, 16, 2)
    cv2.putText(ov, f"frame {F}", (10, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 0), 2, cv2.LINE_AA)
    cv2.imwrite(path, ov)


def draw_period_plot(period_info, err, steady, best, thr, path):
    W, H = 980, 640
    img = np.full((H, W, 3), 255, np.uint8)
    # --- ACF panel
    px, py, pw, ph = 70, 60, 860, 220
    cv2.putText(img, "summed hoof autocorrelation vs lag (frames)", (px, py - 14),
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (30, 30, 30), 1, cv2.LINE_AA)
    cv2.rectangle(img, (px, py), (px + pw, py + ph), (180, 180, 180), 1)
    acf = period_info.get("acf")
    if acf:
        n = len(acf)
        lo, hi = period_info["search_range"]

        def sx(k):
            return int(px + k / max(n - 1, 1) * pw)

        def sy(v):
            return int(py + (1 - (v + 1) / 2) * ph)

        cv2.rectangle(img, (sx(lo), py + 1), (sx(hi), py + ph - 1), (240, 240, 240), -1)
        cv2.line(img, (px, sy(0)), (px + pw, sy(0)), (200, 200, 200), 1)
        pts = np.array([[sx(k), sy(acf[k])] for k in range(n)], np.int32)
        cv2.polylines(img, [pts], False, (60, 60, 200), 2, cv2.LINE_AA)
        if period_info.get("period_frames") is not None:
            kx = sx(period_info["period_frames"])
            cv2.line(img, (kx, py), (kx, py + ph), (0, 160, 0), 2)
            cv2.putText(img, "T=%.2f  conf=%.2f" % (period_info["period_frames"], period_info["confidence"]),
                        (kx + 6, py + 20), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 120, 0), 1, cv2.LINE_AA)
        for k in range(0, n, 5):
            cv2.putText(img, str(k), (sx(k) - 6, py + ph + 18), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
        for v in (-1, 0, 1):
            cv2.putText(img, str(v), (px - 30, sy(v) + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
    else:
        cv2.putText(img, "no ACF (%s)" % period_info.get("note"), (px + 10, py + 40),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 200), 1, cv2.LINE_AA)
    # --- periodicity error panel
    px, py, pw, ph = 70, 380, 860, 220
    cv2.putText(img, "periodicity error |p(t)-p(t+T)| max over legs, px  (green = steady start, "
                "shaded = selected window)", (px, py - 14), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (30, 30, 30), 1, cv2.LINE_AA)
    cv2.rectangle(img, (px, py), (px + pw, py + ph), (180, 180, 180), 1)
    if len(err):
        n = len(err)
        vmax = max(float(err.max()), thr * 1.5, 1.0)

        def sx2(t):
            return int(px + t / max(n - 1, 1) * pw)

        def sy2(v):
            return int(py + ph - v / vmax * ph)

        if best is not None:
            xa, xb = sx2(best["start_frame"]), sx2(min(best["end_time"], n - 1))
            cv2.rectangle(img, (xa, py + 1), (xb, py + ph - 1), (235, 245, 235), -1)
        cv2.line(img, (px, sy2(thr)), (px + pw, sy2(thr)), (0, 0, 220), 1)
        pts = np.array([[sx2(t), sy2(err[t])] for t in range(n)], np.int32)
        cv2.polylines(img, [pts], False, (60, 60, 200), 2, cv2.LINE_AA)
        ss = steady["start"]
        cv2.line(img, (sx2(ss), py), (sx2(ss), py + ph), (0, 160, 0), 2)
        cv2.putText(img, "steady %d%s" % (ss, "" if steady["found"] else " (fallback)"),
                    (sx2(ss) + 6, py + 20), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 120, 0), 1, cv2.LINE_AA)
        for t in range(0, n, 10):
            cv2.putText(img, str(t), (sx2(t) - 6, py + ph + 18), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
        for v in np.linspace(0, vmax, 4):
            cv2.putText(img, "%.0f" % v, (px - 40, sy2(v) + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
    cv2.imwrite(path, img)


def draw_motion_profile(motion, title, path, params=PARAMS):
    """Per-frame-pair motion: silhouette / global mean |diff|, silhouette moving-pixel
    fraction (static pairs shaded red) and per-leg moving ticks."""
    W, H = 980, 800
    img = np.full((H, W, 3), 255, np.uint8)
    cv2.putText(img, title, (16, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (30, 30, 30), 2, cv2.LINE_AA)
    sil, glob = motion.get("silhouette") or {}, motion.get("global") or {}
    fg_mean = np.asarray(sil.get("per_frame_mean_abs_diff") or [], float)
    fg_frac = np.asarray(sil.get("per_frame_moving_frac") or [], float)
    g_mean = np.asarray(glob.get("per_frame_mean_abs_diff") or [], float)
    n = len(fg_mean)
    if n == 0:
        cv2.putText(img, "no frame pairs (%s)" % motion.get("note"), (80, 120),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 200), 1, cv2.LINE_AA)
        cv2.imwrite(path, img)
        return
    px, pw = 70, 860
    static = fg_frac < params["static_fg_frac_thr"]

    def sx(t):
        return int(px + t / max(n - 1, 1) * pw)

    def shade_static(py, ph):
        for t in np.nonzero(static)[0]:
            xa = sx(max(t - 0.5, 0)); xb = sx(min(t + 0.5, n - 1))
            cv2.rectangle(img, (xa, py + 1), (max(xb, xa + 2), py + ph - 1), (225, 225, 250), -1)

    def axis_x(py, ph):
        for t in range(0, n, 5):
            cv2.putText(img, str(t), (sx(t) - 6, py + ph + 18), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)

    # --- panel 1: mean |diff| (silhouette = blue, global = grey)
    py, ph = 70, 210
    cv2.putText(img, "mean |gray diff| per frame pair, grey levels  (blue = horse silhouette, grey = whole frame; "
                "red shade = static pair)", (px, py - 12), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (30, 30, 30), 1, cv2.LINE_AA)
    cv2.rectangle(img, (px, py), (px + pw, py + ph), (180, 180, 180), 1)
    vmax = max(float(fg_mean.max()), float(g_mean.max()) if len(g_mean) else 0.0, params["fg_pix_thr"] * 1.5, 1.0)

    def sy1(v):
        return int(py + ph - v / vmax * ph)

    shade_static(py, ph)
    cv2.line(img, (px, sy1(params["fg_pix_thr"])), (px + pw, sy1(params["fg_pix_thr"])), (0, 0, 220), 1)
    if len(g_mean) == n:
        cv2.polylines(img, [np.array([[sx(t), sy1(g_mean[t])] for t in range(n)], np.int32)], False,
                      (150, 150, 150), 2, cv2.LINE_AA)
    cv2.polylines(img, [np.array([[sx(t), sy1(fg_mean[t])] for t in range(n)], np.int32)], False,
                  (200, 80, 40), 2, cv2.LINE_AA)
    for v in np.linspace(0, vmax, 4):
        cv2.putText(img, "%.0f" % v, (px - 40, sy1(v) + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
    axis_x(py, ph)

    # --- panel 2: moving-pixel fraction of the silhouette
    py, ph = 340, 170
    cv2.putText(img, "silhouette moving-pixel fraction (|diff| > %.0f)  (red line = static threshold %.3f)"
                % (params["fg_pix_thr"], params["static_fg_frac_thr"]), (px, py - 12),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (30, 30, 30), 1, cv2.LINE_AA)
    cv2.rectangle(img, (px, py), (px + pw, py + ph), (180, 180, 180), 1)
    vmax2 = max(float(fg_frac.max()), params["static_fg_frac_thr"] * 3, 0.05)

    def sy2(v):
        return int(py + ph - v / vmax2 * ph)

    shade_static(py, ph)
    cv2.line(img, (px, sy2(params["static_fg_frac_thr"])), (px + pw, sy2(params["static_fg_frac_thr"])), (0, 0, 220), 1)
    cv2.polylines(img, [np.array([[sx(t), sy2(fg_frac[t])] for t in range(n)], np.int32)], False,
                  (200, 80, 40), 2, cv2.LINE_AA)
    for v in np.linspace(0, vmax2, 4):
        cv2.putText(img, "%.2f" % v, (px - 48, sy2(v) + 4), cv2.FONT_HERSHEY_SIMPLEX, 0.42, (90, 90, 90), 1, cv2.LINE_AA)
    axis_x(py, ph)

    # --- panel 3: per-leg moving ticks
    py, lh = 560, 34
    cv2.putText(img, "leg moves between t and t+1 (centroid or hoof shift > %.1f px)" % params["leg_move_px"],
                (px, py - 12), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (30, 30, 30), 1, cv2.LINE_AA)
    plm = motion.get("per_leg_moving") or {}
    plr = motion.get("per_leg_moving_ratio") or {}
    cw = max(2, int(pw / max(n, 1)) - 1)
    for li, leg in enumerate(LEGS):
        yy = py + li * lh
        cv2.putText(img, "%s %.2f" % (leg, plr.get(leg) or 0.0), (px - 66, yy + 22),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.4, (30, 30, 30), 1, cv2.LINE_AA)
        mv = plm.get(leg) or []
        for t in range(n):
            xx = sx(t) - cw // 2
            if t < len(mv) and mv[t]:
                cv2.rectangle(img, (xx, yy + 6), (xx + cw, yy + lh - 8), LEG_BGR[leg], -1)
            else:
                cv2.rectangle(img, (xx, yy + lh // 2 - 2), (xx + cw, yy + lh // 2 + 2), (225, 225, 225), -1)
    fp = motion.get("ffprobe") or {}
    foot = ("motion_score %.2f   static_frame_ratio %.2f (max %.2f)   ffprobe P/I ratio %s"
            % (motion.get("motion_score") or 0.0, motion.get("static_frame_ratio") or 0.0,
               params["static_frame_ratio_max"],
               ("%.3f" % fp["ratio"]) if fp.get("ratio") is not None else "n/a"))
    cv2.putText(img, foot, (px, H - 40), cv2.FONT_HERSHEY_SIMPLEX, 0.52, (60, 60, 60), 1, cv2.LINE_AA)
    cv2.putText(img, "hard rule motion_present: static_frame_ratio <= %.2f and every leg moving ratio >= %.2f"
                % (params["static_frame_ratio_max"], params["leg_moving_ratio_min"]),
                (px, H - 16), cv2.FONT_HERSHEY_SIMPLEX, 0.48, (60, 60, 60), 1, cv2.LINE_AA)
    cv2.imwrite(path, img)


# ======================================================================
#  7. main
# ======================================================================
def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("video", nargs="?", default=DEFAULT_VIDEO)
    ap.add_argument("out_dir", nargs="?", default=DEFAULT_OUT)
    ap.add_argument("--target-frames", type=int, default=49, help="N frames of the exported cycle (default 49)")
    ap.add_argument("--period-min", type=int, default=12)
    ap.add_argument("--period-max", type=int, default=60)
    ap.add_argument("--steady-thr", type=float, default=4.0, help="px: periodicity error threshold for steady state")
    ap.add_argument("--closure-thr", type=float, default=6.0, help="px: accept threshold for closure after retiming")
    ap.add_argument("--cycles", type=int, default=None, help="force integer number of periods in the window")
    ap.add_argument("--resample", choices=["blend", "nearest"], default="blend")
    ap.add_argument("--no-export", action="store_true")
    ap.add_argument("--fps", type=int, default=25)
    ap.add_argument("--size", default="768x448")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    video, out_dir = args.video, args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(video))[0]
    out_w, out_h = (int(v) for v in args.size.lower().split("x"))

    frames = load_frames(video)
    T, H, W, _ = frames.shape
    print("loaded", frames.shape)

    tr = analyze_tracks(frames)
    hoof, hoof_s, contact = tr["hoof"], tr["hoof_s"], tr["contact"]

    # ---- legacy report
    metrics = {
        "video": video,
        "frames": int(T), "fps": args.fps, "size": [int(W), int(H)],
        "palette_srgb_rgb255": {k: [int(x) for x in v] for k, v in PALETTE_SRGB.items()},
        "segmentation": {
            "leg_dist_max": PARAMS["leg_dist_max"],
            "median_leg_area_px": {LEGS[i]: float(tr["med_area"][i]) for i in range(len(LEGS))},
            "interpolated_frames": tr["interp_frames"],
        },
        "contact_params": {"vy_thr_px_per_frame": PARAMS["vy_thr"], "y_tol_px": PARAMS["y_tol"],
                           "plateau_min_len": PARAMS["plateau_min_len"],
                           "ground_merge_tol_px": PARAMS["ground_merge_tol"],
                           "stance_definition": "|vy| < vy_thr and y within y_tol of the per-leg ground line "
                                                "(horizontal treadmill drift allowed)"},
        "ground_line_per_leg": tr["ground_model"],
        "stationary_plateaus": tr["plateaus"],
    }
    metrics.update(clip_level_metrics(tr, T))

    # ---- amount of motion / static-render guard
    motion = motion_metrics(frames, tr["masks"], tr["hoof_raw"])
    motion["ffprobe"] = ffprobe_motion(video)
    metrics["motion"] = motion

    # ---- period / steady state
    period_info = detect_period(hoof_s, args.period_min, args.period_max)
    period = period_info["period_frames"]
    if period is not None:
        err = periodicity_error(hoof_s, period)
        steady = find_steady_state(err, period, args.steady_thr)
    else:
        err = np.zeros(0)
        steady = {"start": 0, "found": False, "threshold_px": args.steady_thr, "note": "no period"}
    metrics["period"] = period_info
    metrics["periodicity_error_px"] = [round(float(v), 3) for v in err]
    metrics["steady_state_start"] = steady

    # ---- best window
    best, win_note = select_best_window(tr, period, steady["start"], args.target_frames, args.cycles)
    metrics["best_window"] = best
    metrics["best_window_search"] = win_note

    # ---- export
    export = {"done": False, "reason": None}
    if best is not None and not args.no_export:
        N = args.target_frames
        cyc, times = resample_cycle(frames, best["start_frame"], best["time_scale"], N, args.resample)
        clip_path = os.path.join(out_dir, "%s_cycle%d.mp4" % (base, N))
        try:
            write_mp4(cyc, clip_path, fps=args.fps, size=(out_w, out_h))
            export.update({"done": True, "path": clip_path, "frames": N, "fps": args.fps,
                           "size": [out_w, out_h], "codec": "h264 yuv420p", "resample": args.resample,
                           "source_times": [round(t, 3) for t in times],
                           "probe": probe_frames(clip_path)})
        except Exception as ex:  # noqa
            export.update({"done": False, "reason": str(ex)})
        seam_path = os.path.join(out_dir, "seam_check.png")
        write_seam_check(frames, tr, best, cyc, seam_path)
        export["seam_check"] = seam_path
        sheet_path = os.path.join(out_dir, "cycle_contact_sheet.png")
        write_cycle_sheet(cyc, times, sheet_path)
        export["cycle_sheet"] = sheet_path
    elif best is None:
        export["reason"] = "no window: " + str(win_note.get("note"))
    else:
        export["reason"] = "--no-export"
    metrics["cycle_export"] = export

    # ---- verdict
    metrics["verdict"] = make_verdict(best, period_info, steady, args.closure_thr, motion=motion)

    # ---- plots & dumps
    draw_gait_chart(tr, T, "Footfall diagram %s (%d frames, %d fps)" % (base, T, args.fps),
                    os.path.join(out_dir, "gait_chart.png"), window=best)
    draw_trajectories(tr, T, "Hoof trajectories %s (y axis down = image coords)" % base,
                      os.path.join(out_dir, "hoof_trajectories.png"))
    draw_overlay(frames, tr, 12, os.path.join(out_dir, "hooves_overlay_frame12.png"))
    draw_period_plot(period_info, err, steady, best, args.steady_thr, os.path.join(out_dir, "period_analysis.png"))
    draw_motion_profile(motion, "Motion profile %s (%d frame pairs)" % (base, motion["frame_pairs"]),
                        os.path.join(out_dir, "motion_profile.png"))
    np.savez(os.path.join(out_dir, "hoof_tracks.npz"), hoof=hoof, hoof_smooth=hoof_s,
             contact=contact, vx=tr["vx"], vy=tr["vy"], speed=tr["speed"], areas=tr["areas"],
             body_centroid=tr["body"])
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2, ensure_ascii=False)

    summary = {
        "motion": {"motion_score": motion.get("motion_score"),
                   "static_frame_ratio": motion.get("static_frame_ratio"),
                   "per_leg_moving_ratio": motion.get("per_leg_moving_ratio"),
                   "silhouette_moving_frac_median": (motion.get("silhouette") or {}).get("moving_frac_median"),
                   "ffprobe": {k: motion["ffprobe"].get(k) for k in
                               ("i_frame_bytes", "mean_p_frame_bytes", "ratio", "static_hint", "error")}},
        "period": {k: period_info.get(k) for k in ("period_frames", "confidence", "prominence", "n_signals", "note")},
        "steady_state_start": steady,
        "best_window": ({k: best[k] for k in ("start_frame", "end_time", "periods", "time_scale", "closure_px",
                                              "legs_stepping", "order_ok", "score", "duty_in_window")}
                        if best else None),
        "cycle_export": {k: export.get(k) for k in ("done", "path", "seam_check", "cycle_sheet", "reason", "probe")},
        "verdict": metrics["verdict"],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    main()
