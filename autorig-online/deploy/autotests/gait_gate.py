"""Gait gate · V3 (Fable · equines, owner 2026-10-13 «конвейерное производство»): a generated reference clip is
judged against real gait timing before it enters the motion library.  For a quadruped the footfall order is read from
the upper-leg angle curves of a library entry (the maximum protraction of a leg is its touchdown): the lag of the
near front leg behind the near hind leg, in cycle fractions, and the cycle period.

  walk    four-beat lateral sequence LH, LF, RH, RF: LF lags LH by ~0.25;  period 0.8-1.7 s (in place)
  trot    two-beat diagonal pairs LH+RF, RH+LF:       LF lags LH by ~0.5;   period 0.5-1.0 s
  canter  three-beat (left lead) LH, RH+LF, RF:       LF lags LH by ~0.33;  period 0.45-0.85 s
  gallop  four-beat transverse LH, RH, LF, RF:        LF lags LH by 0.3-0.6; period 0.35-0.65 s
  idle    no leg swing (<= 6 deg);  rear  a one-shot of 2.5-9 s with the front legs lifting >= 35 deg;
  look    a one-shot of 2-9 s where the neck / head turn >= 10 deg and the legs stay (<= 8 deg).
The far legs of a side-view entry are mirrored (half a period), so a three-beat canter is judged on the near pair
only.  `judge(entry)` -> {"ok", "why": [...], "metrics": {...}}; the rules are data (RULES) so other categories
(felines, canines, birds) add their own.  Options: GAIT_GATE_<RULE> env overrides are not needed: edit RULES.
"""
from __future__ import annotations

import numpy as np

RULES = {
    "walk": {"period": (0.8, 1.7), "lf_lag": (0.12, 0.38), "min_amp": 10.0},
    "trot": {"period": (0.5, 1.0), "lf_lag": (0.38, 0.62), "min_amp": 10.0},
    "canter": {"period": (0.45, 0.85), "lf_lag": (0.2, 0.5), "min_amp": 10.0},
    "gallop": {"period": (0.35, 0.65), "lf_lag": (0.2, 0.6), "min_amp": 10.0},
    "run": {"period": (0.35, 1.0), "min_amp": 10.0},
    "idle": {"max_amp": 6.0},
    "rear": {"period": (2.5, 9.0), "front_amp": (35.0, 130.0), "hind_amp": (8.0, 90.0), "one_shot": True},
    "look": {"period": (2.0, 9.0), "neck_amp": (10.0, 70.0), "limb_max": 8.0, "one_shot": True},
}
LEGS = {"LH": "LeftHindUpLeg", "LF": "LeftFrontUpLeg", "RH": "RightHindUpLeg", "RF": "RightFrontUpLeg"}


def _phase_of_max(c, smooth=3):
    c = np.asarray(c, float)
    if len(c) < 4:
        return 0.0
    k = np.ones(smooth) / smooth
    cs = np.convolve(np.concatenate([c[-smooth:], c, c[:smooth]]), k, "same")[smooth:-smooth]
    return float(np.argmax(cs)) / len(c)


def metrics(entry: dict) -> dict:
    """What the gate reads from an entry: the period, the legs' amplitudes and phases, the front-leg lag."""
    curves = {k: np.asarray(v, float) for k, v in (entry.get("curves") or {}).items()}
    m = {"period_s": float(entry.get("period_s") or 0.0), "loop": bool(entry.get("loop", True))}
    amp = {k: float(np.ptp(curves[b])) for k, b in LEGS.items() if b in curves}
    m["leg_amp_deg"] = {k: round(v, 1) for k, v in amp.items()}
    ph = {k: _phase_of_max(curves[b]) for k, b in LEGS.items() if b in curves}
    if "LH" in ph and "LF" in ph:
        m["lf_lag"] = round((ph["LF"] - ph["LH"]) % 1.0, 3)
    m["trunk_amp_deg"] = round(max([float(np.ptp(curves[b])) for b in ("Hips", "Spine", "Spine1", "Spine2") if b in curves] or [0.0]), 1)
    m["neck_amp_deg"] = round(max([float(np.ptp(curves[b])) for b in ("Neck", "Head") if b in curves] or [0.0]), 1)
    m["front_amp_deg"] = round(max([amp.get("LF", 0.0), amp.get("RF", 0.0)]), 1)
    m["hind_amp_deg"] = round(max([amp.get("LH", 0.0), amp.get("RH", 0.0)]), 1)
    m["limb_amp_deg"] = round(max(list(amp.values()) or [0.0]), 1)
    return m


def judge(entry: dict, rules: dict | None = None) -> dict:
    clip = str(entry.get("clip") or "")
    r = (rules or RULES).get(clip)
    m = metrics(entry)
    why = []
    if not r:
        return {"ok": True, "why": ["no rule for this clip"], "metrics": m, "rule": None}
    if "period" in r and not (r["period"][0] <= m["period_s"] <= r["period"][1]):
        why.append(f"period {m['period_s']:.2f} s outside {r['period']}")
    if "min_amp" in r and m["limb_amp_deg"] < r["min_amp"]:
        why.append(f"legs swing {m['limb_amp_deg']} deg < {r['min_amp']}")
    if "max_amp" in r and m["limb_amp_deg"] > r["max_amp"]:
        why.append(f"legs swing {m['limb_amp_deg']} deg > {r['max_amp']} (not idle)")
    if "lf_lag" in r:
        if "lf_lag" not in m:
            why.append("no LH / LF curves to read the footfall order")
        elif not (r["lf_lag"][0] <= m["lf_lag"] <= r["lf_lag"][1]):
            why.append(f"front leg lags hind by {m['lf_lag']:.2f} of the cycle, wanted {r['lf_lag']} ({clip})")
    if "front_amp" in r and not (r["front_amp"][0] <= m["front_amp_deg"] <= r["front_amp"][1]):
        why.append(f"front legs lift {m['front_amp_deg']} deg, wanted {r['front_amp']}")
    if "hind_amp" in r and not (r["hind_amp"][0] <= m["hind_amp_deg"] <= r["hind_amp"][1]):
        why.append(f"hind legs {m['hind_amp_deg']} deg, wanted {r['hind_amp']}")
    if "neck_amp" in r and not (r["neck_amp"][0] <= m["neck_amp_deg"] <= r["neck_amp"][1]):
        why.append(f"neck / head turn {m['neck_amp_deg']} deg, wanted {r['neck_amp']}")
    if "limb_max" in r and m["limb_amp_deg"] > r["limb_max"]:
        why.append(f"legs move {m['limb_amp_deg']} deg > {r['limb_max']} (the legs should stay)")
    if r.get("one_shot") and m["loop"]:
        why.append("a one-shot clip must not loop")
    return {"ok": not why, "why": why, "metrics": m, "rule": r}


def audit(lib_root) -> list:
    """Every entry of the library judged. -> [(species, clip, ok, why)]."""
    import json
    import pathlib
    out = []
    for p in sorted(pathlib.Path(lib_root).glob("*/*.json")):
        try:
            e = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        j = judge(e)
        out.append((p.parent.name, p.stem, j["ok"], j["why"], j["metrics"]))
    return out


if __name__ == "__main__":
    import sys
    for sp, clip, ok, why, m in audit(sys.argv[1] if len(sys.argv) > 1 else "animal_motion_lib"):
        print(f"{'PASS' if ok else 'FAIL':4s} {sp:18s} {clip:7s} period {m['period_s']:.2f}s lf_lag {m.get('lf_lag')} "
              f"legs {m['limb_amp_deg']} neck {m['neck_amp_deg']}  {'; '.join(why)}")
